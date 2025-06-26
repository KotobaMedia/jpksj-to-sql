use std::collections::HashMap;
use std::ops::RangeInclusive;

use anyhow::{anyhow, Context, Result};
use bytesize::ByteSize;
use once_cell::sync::Lazy;
use regex::Regex;
use scraper::{selectable::Selectable, Html, Selector};
use serde::Serialize;
use url::Url;

use super::table_read::{parse_table, parsed_to_string_array};

// Compile the regex once for efficiency.
// This regex looks for one or more digits at the very start of the string, immediately followed by '年'.
static YEAR_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(\d+)年").unwrap());

// Regex for extracting year ranges and URLs from data selection lines
static YEAR_RANGE_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(データ基準年：|最新のデータは)(\d{4})(～?(\d{4}))?年").unwrap());

#[derive(Debug, Serialize)]
pub struct DataPage {
    pub url: Url,
    #[serde(skip)]
    pub unfiltered_items: Vec<DataItem>,
    pub metadata: DataPageMetadata,
    pub data_selections: Vec<DataSelection>,
}

impl DataPage {
    /// Scrapes a data page from the given URL and returns a DataPage instance
    pub async fn scrape(url: &Url) -> Result<Self> {
        let response = reqwest::get(url.clone()).await?;
        let body = response.text().await?;
        let document = Html::parse_document(&body);

        let metadata = Self::extract_metadata(&document, url)
            .await
            .with_context(|| format!("when accessing {}", url.to_string()))?;

        let unfiltered_items = Self::extract_data_items(&document, url)?;
        let data_selections = Self::extract_data_selections(&document, url)?;

        Ok(DataPage {
            url: url.clone(),
            unfiltered_items,
            metadata,
            data_selections,
        })
    }

    /// Returns filtered data items based on the filtering logic
    pub fn items(&self) -> Vec<DataItem> {
        Self::filter_data_items(&self.unfiltered_items)
    }

    /// Extracts data items from the HTML document
    fn extract_data_items(document: &Html, base_url: &Url) -> Result<Vec<DataItem>> {
        let td_sel = Selector::parse("td").unwrap();
        let data_tr_sel = Selector::parse("table.dataTables tr, table.dataTables-mesh tr").unwrap();
        let data_path_re =
            Regex::new(r"javascript:DownLd(?:_new)?\('[^']*',\s*'[^']*',\s*'([^']+)'").unwrap();

        let mut items: Vec<DataItem> = Vec::new();
        let mut use_nendo = false;

        for row in document.select(&data_tr_sel) {
            let tds = row.select(&td_sel).collect::<Vec<_>>();
            if tds.is_empty() {
                continue;
            }

            let area = tds[0].text().collect::<String>().trim().to_string();
            if area == "地域" {
                // Check if this is a header row with 年度 column
                if tds[2].text().collect::<String>().contains("年度") {
                    use_nendo = true;
                }
                continue;
            }

            let crs = tds[1].text().collect::<String>().trim().to_string();
            let year_str = tds[2].text().collect::<String>().trim().to_string();

            let (year, nendo) = if use_nendo {
                (None, Some(year_str))
            } else {
                (Some(year_str.clone()), None)
            };

            let bytes_str = tds[3]
                .text()
                .next()
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow!("bytes string not found"))?;

            // Use 10MB as default if parsing fails (just for progress indication)
            let bytes: ByteSize = bytes_str.parse().unwrap_or_else(|_| ByteSize::mb(10));

            let file_url = Self::extract_file_url(&tds[5], base_url, &data_path_re)?;

            let item = DataItem {
                area,
                bytes: bytes.0,
                crs,
                year,
                nendo,
                file_url,
            };
            items.push(item);
        }

        Ok(items)
    }

    /// Extracts file URL from a table cell containing a download link
    fn extract_file_url(
        cell: &scraper::ElementRef,
        base_url: &Url,
        data_path_re: &Regex,
    ) -> Result<Url> {
        let a_sel = Selector::parse("a").unwrap();

        let file_js_onclick = cell
            .select(&a_sel)
            .next()
            .and_then(|a| a.value().attr("onclick"))
            .ok_or_else(|| anyhow!("download link not found"))?;

        let file_path = data_path_re
            .captures(file_js_onclick)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str())
            .ok_or_else(|| anyhow!("file path not found in onclick: {}", file_js_onclick))?;

        let file_url = base_url
            .join(file_path)
            .map_err(|e| anyhow!("failed to join URL: {}", e))?;

        Ok(file_url)
    }

    /// Extracts metadata from the HTML document
    async fn extract_metadata<'a, S: Selectable<'a>>(
        html: S,
        base_url: &Url,
    ) -> Result<DataPageMetadata> {
        let mut metadata = DataPageMetadata::default();
        let table_sel = Selector::parse("table").unwrap();
        let t_cell_sel = Selector::parse("th, td").unwrap();
        let tables: Vec<scraper::ElementRef<'a>> = html.select(&table_sel).collect();

        let strip_space_re = Regex::new(r"\s+").unwrap();

        // Extract fundamental information
        metadata.fundamental =
            Self::extract_fundamental_info(&tables, &t_cell_sel, &strip_space_re)?;

        // Extract attribute information
        metadata.attribute = Self::extract_attribute_info(
            &tables,
            &table_sel,
            &t_cell_sel,
            &strip_space_re,
            base_url,
        )?;

        // Parse reference data for attributes
        Self::parse_attribute_references(&mut metadata.attribute).await?;

        Ok(metadata)
    }

    /// Extracts fundamental information from tables
    fn extract_fundamental_info(
        tables: &[scraper::ElementRef],
        t_cell_sel: &Selector,
        strip_space_re: &Regex,
    ) -> Result<HashMap<String, String>> {
        // Find table containing "更新履歴" or "内容"
        let fundamental_table = tables
            .iter()
            .find(|table| {
                let headers: Vec<String> = table
                    .select(t_cell_sel)
                    .map(|th| th.text().collect::<String>().trim().to_string())
                    .collect();
                headers
                    .iter()
                    .any(|h| h.contains("更新履歴") || h.contains("内容"))
            })
            .ok_or_else(|| anyhow!("基本情報の table が見つかりませんでした"))?;

        let fundamental_parsed = parse_table(fundamental_table.clone());
        let fundamental_parsed_str = parsed_to_string_array(fundamental_parsed);

        let mut fundamental = HashMap::new();
        for row in fundamental_parsed_str.outer_iter() {
            if row.len() < 2 {
                continue;
            }
            let key = row[0].as_ref().unwrap().trim().to_string();
            let mut value = row[1].as_ref().unwrap().trim().to_string();
            value = strip_space_re.replace_all(&value, " ").to_string();
            fundamental.insert(key, value);
        }

        if fundamental.is_empty() {
            return Err(anyhow!("基本情報が見つかりませんでした"));
        }

        Ok(fundamental)
    }

    /// Extracts attribute information from tables
    fn extract_attribute_info(
        tables: &[scraper::ElementRef],
        table_sel: &Selector,
        _t_cell_sel: &Selector,
        strip_space_re: &Regex,
        base_url: &Url,
    ) -> Result<HashMap<String, AttributeMetadata>> {
        let attr_key_regex = Regex::new(r"^(.*?)\s*[（(]([a-zA-Z0-9-_]+)▲?[）)]$").unwrap();

        // Find table containing attribute information
        let attribute_table = tables
            .iter()
            .find_map(|table| {
                // Skip tables that contain nested tables
                if table.select(table_sel).count() > 1 {
                    return None;
                }

                let parsed = parse_table(table.clone());
                let mut attr_indices: Option<(usize, usize, usize)> = None;
                let mut attr_map: HashMap<String, AttributeMetadata> = HashMap::new();

                for row in parsed.outer_iter() {
                    if row.len() < 3 {
                        continue;
                    }

                    if let Some((attr_name_idx, desc_idx, type_idx)) = attr_indices {
                        // Process attribute data row
                        let attr_name_str = row[attr_name_idx]
                            .as_ref()?
                            .text()
                            .collect::<String>()
                            .trim()
                            .to_string();

                        let Some(name_match) = attr_key_regex.captures(&attr_name_str) else {
                            continue;
                        };

                        let name_jp = name_match.get(1).unwrap();
                        let name_id = name_match.get(2).unwrap();

                        let mut description = row[desc_idx]
                            .as_ref()?
                            .text()
                            .collect::<String>()
                            .trim()
                            .to_string();
                        description = strip_space_re.replace_all(&description, " ").to_string();

                        let attr_type_ele = row[type_idx].as_ref().unwrap();
                        let mut attr_type_str =
                            attr_type_ele.text().collect::<String>().trim().to_string();
                        attr_type_str = strip_space_re.replace_all(&attr_type_str, " ").to_string();

                        let ref_url = Self::extract_ref_url(attr_type_ele, base_url);

                        attr_map.insert(
                            name_id.as_str().to_string(),
                            AttributeMetadata {
                                name: name_jp.as_str().to_string(),
                                description,
                                attr_type: attr_type_str,
                                ref_url,
                                r#ref: None,
                            },
                        );
                    } else {
                        // Find column indices for attribute table
                        attr_indices = Self::find_attribute_indices(row);
                    }
                }

                if attr_map.is_empty() {
                    None
                } else {
                    Some(attr_map)
                }
            })
            .ok_or_else(|| anyhow!("属性情報の table が見つかりませんでした"))?;

        Ok(attribute_table)
    }

    /// Finds the column indices for attribute name, description, and type
    fn find_attribute_indices(
        row: ndarray::ArrayView1<Option<scraper::ElementRef>>,
    ) -> Option<(usize, usize, usize)> {
        let mut attr_name: Option<usize> = None;
        let mut attr_desc: Option<usize> = None;
        let mut attr_type: Option<usize> = None;

        for (i, cell) in row.iter().enumerate() {
            let Some(cell) = cell.as_ref() else {
                continue;
            };
            let cell_str = cell.text().collect::<String>().trim().to_string();

            if cell_str.contains("属性名") {
                attr_name = Some(i);
            } else if cell_str.contains("説明") {
                attr_desc = Some(i);
            } else if cell_str.contains("属性の型") || cell_str.contains("属性型") {
                attr_type = Some(i);
            }

            if attr_name.is_some() && attr_desc.is_some() && attr_type.is_some() {
                return Some((attr_name.unwrap(), attr_desc.unwrap(), attr_type.unwrap()));
            }
        }

        None
    }

    /// Extracts reference URL from an attribute type element
    fn extract_ref_url(attr_type_ele: &scraper::ElementRef, base_url: &Url) -> Option<Url> {
        let a_sel = Selector::parse("a").unwrap();
        attr_type_ele
            .select(&a_sel)
            .next()
            .and_then(|a| a.value().attr("href"))
            .and_then(|href| base_url.join(href).ok())
    }

    /// Parses reference data for attributes that have reference URLs
    async fn parse_attribute_references(
        attributes: &mut HashMap<String, AttributeMetadata>,
    ) -> Result<()> {
        for attr in attributes.values_mut() {
            if let Some(ref_url) = &attr.ref_url {
                if ref_url.to_string().contains(".xlsx") {
                    // AdminiBoundary_CD.xlsx is handled in admini_boundary.rs
                    continue;
                }
                attr.r#ref = parse_ref_from_url(ref_url)
                    .await
                    .with_context(|| format!("when accessing ref url: {}", ref_url))?;
            }
        }
        Ok(())
    }

    /// Filters data items to select the most recent and relevant ones
    fn filter_data_items(items: &[DataItem]) -> Vec<DataItem> {
        // Step 1: Filter items by CRS (世界測地系)
        let crs_filtered: Vec<&DataItem> = items
            .iter()
            .filter(|item| item.crs == "世界測地系")
            .collect();

        // Step 2: Group items by area
        let mut area_groups: HashMap<String, Vec<&DataItem>> = HashMap::new();
        for item in crs_filtered {
            // If 全国 is already in the map, and we aren't in the 全国 group, skip this item
            if area_groups.contains_key("全国") && item.area != "全国" {
                continue;
            }
            area_groups.entry(item.area.clone()).or_default().push(item);
        }

        // Step 3: For each area, select items with the maximum recency
        let mut result = Vec::new();
        for (_area, group) in area_groups {
            let max_recency = group.iter().filter_map(|item| parse_recency(item)).max();
            if let Some(max_year) = max_recency {
                result.extend(
                    group
                        .into_iter()
                        .filter(|item| parse_recency(item) == Some(max_year))
                        .cloned(),
                );
            } else {
                result.extend(group.into_iter().cloned());
            }
        }
        result
    }

    /// Extracts data selections from the page that contain year ranges and URLs
    fn extract_data_selections(document: &Html, base_url: &Url) -> Result<Vec<DataSelection>> {
        let mut selections = Vec::new();
        let p_selector = Selector::parse("p").unwrap();

        for element in document.select(&p_selector) {
            let text = element.text().collect::<String>();
            if !text.contains("選択したデータ項目は") {
                continue;
            }
            if !(text.contains("最新のデータは") || text.contains("データ基準年：")) {
                continue;
            }
            // Split by <br> tags and process each line
            let html_content = element.inner_html();
            let lines: Vec<&str> = html_content.split("<br>").collect();

            for line in lines {
                let line_document = Html::parse_fragment(line);
                let line_text = line_document
                    .root_element()
                    .text()
                    .collect::<String>()
                    .trim()
                    .to_string();
                let Some(captures) = YEAR_RANGE_REGEX.captures(&line_text) else {
                    continue;
                };
                let start_year = captures
                    .get(2)
                    .and_then(|m| m.as_str().parse::<u32>().ok())
                    .ok_or_else(|| anyhow!("failed to parse start year from: {}", line_text))?;
                let end_year = captures
                    .get(4)
                    .and_then(|m| m.as_str().parse::<u32>().ok())
                    .unwrap_or(start_year);
                let a_selector = Selector::parse("a").unwrap();
                let url = line_document
                    .select(&a_selector)
                    .next()
                    .and_then(|a| a.value().attr("href"))
                    .and_then(|href| base_url.join(href).ok());
                let url = if url.is_none() && line_text.contains("最新のデータは") {
                    base_url.clone()
                } else {
                    url.ok_or_else(|| anyhow!("no URL found in line: {}", line_text))?
                };
                selections.push(DataSelection {
                    year: start_year..=end_year,
                    url,
                });
            }
        }
        Ok(selections)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DataItem {
    pub area: String,
    pub crs: String,
    pub bytes: u64,
    pub year: Option<String>,  // 年
    pub nendo: Option<String>, // 年度
    pub file_url: Url,
}

#[derive(Debug, Clone, Serialize)]
pub struct DataSelection {
    pub year: RangeInclusive<u32>,
    pub url: Url,
}

#[derive(Debug, Clone, Serialize)]
pub enum RefType {
    Enum(Vec<String>),
    Code(HashMap<String, String>),
}

async fn parse_ref_from_url(url: &Url) -> Result<Option<RefType>> {
    if url.to_string().contains("PubFacAdminCd.html") {
        return Ok(None);
    }

    let response = reqwest::get(url.clone()).await?;
    let body = response.text().await?;
    let document = Html::parse_document(&body);

    // Selector for cells (<td> or <th>)
    let td_sel = Selector::parse("td, th").unwrap();
    // Selector for table rows
    let tr_sel = Selector::parse("table tr").unwrap();

    let mut headers = Vec::new();
    // Extract first row
    let first_row = document
        .select(&tr_sel)
        .next()
        .ok_or_else(|| anyhow!("no first row found"))?;

    for element in first_row.select(&td_sel) {
        headers.push(
            element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string(),
        );
    }

    if headers.is_empty() {
        return Err(anyhow!("no headers found"));
    }

    let code_idx_opt = headers.iter().position(|h| h == "コード");
    if let Some(code_idx) = code_idx_opt {
        let name_idx = headers
            .iter()
            .position(|h| {
                h == "対応する内容"
                    || h == "内容"
                    || h.contains("定義")
                    || h.contains("分類")
                    || h.contains("種別")
                    || h.contains("対象")
                    || h.contains("区分")
            })
            .ok_or_else(|| anyhow!("name index not found in headers: {:?}", headers))?;
        // code list
        let mut code_map = HashMap::new();
        for row in document.select(&tr_sel) {
            let tds = row.select(&td_sel).collect::<Vec<_>>();
            if tds.len() < 2 {
                continue;
            }
            // code_idx is the index of the code column
            let code = tds
                .get(code_idx)
                .ok_or(anyhow!("code not found"))?
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string();
            let name = tds
                .get(name_idx)
                .ok_or(anyhow!("name not found"))?
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string();
            if !code.is_empty() && code != "コード" && !name.is_empty() {
                code_map.insert(code, name);
            }
        }
        if code_map.is_empty() {
            return Err(anyhow!("no code found"));
        }
        return Ok(Some(RefType::Code(code_map)));
    } else if headers[0].contains("定数") {
        // enum list
        let mut enum_list = Vec::new();
        for cell in document.select(&td_sel) {
            let cell_text = cell.text().collect::<Vec<_>>().join(" ").trim().to_string();
            if !cell_text.is_empty() && cell_text != "定数" {
                enum_list.push(cell_text);
            }
        }
        if enum_list.is_empty() {
            return Err(anyhow!("no enum found"));
        }
        return Ok(Some(RefType::Enum(enum_list)));
    }

    Err(anyhow!("ref table not found"))
}

#[derive(Debug, Clone, Serialize)]
pub struct AttributeMetadata {
    pub name: String,
    pub description: String,
    pub attr_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_url: Option<Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#ref: Option<RefType>,
}

#[derive(Default, Debug, Serialize)]
pub struct DataPageMetadata {
    pub fundamental: HashMap<String, String>,
    pub attribute: HashMap<String, AttributeMetadata>,
}

/// Extracts the numeric year from a field formatted like "2006年（平成18年）".
/// If the field does not match, returns None.
fn extract_year_from_field(field: &str) -> Option<u32> {
    YEAR_REGEX
        .captures(field)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

/// Determines the recency value for an item, preferring the `year` field.
/// Falls back to `nendo` if necessary.
fn parse_recency(item: &DataItem) -> Option<u32> {
    if let Some(ref y) = item.year {
        if let Some(year) = extract_year_from_field(y) {
            return Some(year);
        }
    }
    if let Some(ref n) = item.nendo {
        if let Some(year) = extract_year_from_field(n) {
            return Some(year);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scrape_c23() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-C23.html").unwrap();
        let page = DataPage::scrape(&url).await.unwrap();
        assert_eq!(page.items().len(), 39);

        // Test data selections functionality
        assert!(!page.data_selections.is_empty());
        println!("C23 data selections:");
        for (i, selection) in page.data_selections.iter().enumerate() {
            println!(
                "  {}: {}-{} -> {}",
                i + 1,
                selection.year.start(),
                selection.year.end(),
                selection.url
            );
        }

        // Verify that each selection has valid data
        for selection in &page.data_selections {
            assert!(*selection.year.start() > 1900 && *selection.year.start() < 2100);
            assert!(selection.url.to_string().starts_with("http"));
        }

        // Check that we have at least one selection with the current document URL (最新データ)
        let current_url_selections: Vec<_> = page
            .data_selections
            .iter()
            .filter(|s| s.url == url)
            .collect();
        assert!(
            !current_url_selections.is_empty(),
            "Should have at least one selection with current document URL for 最新データ"
        );
    }

    #[tokio::test]
    async fn test_scrape_n03() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N03-2024.html").unwrap();
        let page = DataPage::scrape(&url).await.unwrap();
        // 全国パターン
        assert_eq!(page.items().len(), 1);

        let naiyo = page.metadata.fundamental.get("内容").unwrap();
        assert!(naiyo.contains("全国の行政界について、都道府県名、"));

        let zahyoukei = page.metadata.fundamental.get("座標系").unwrap();
        assert!(zahyoukei.contains("世界測地系"));

        let todoufukenmei = page.metadata.attribute.get("N03_001").unwrap();
        assert!(todoufukenmei.name.contains("都道府県名"));
        assert!(todoufukenmei.description.contains("都道府県の名称"));
        assert!(todoufukenmei.attr_type.contains("文字列"));

        let lg_code = page.metadata.attribute.get("N03_007").unwrap();
        assert!(lg_code.name.contains("全国地方公共団体コード"));
        assert!(lg_code.description.contains("JIS X 0401"));
        assert!(lg_code.attr_type.contains("コードリスト"));
        assert!(lg_code
            .ref_url
            .as_ref()
            .is_some_and(|u| u.as_str().contains("AdminiBoundary_CD.xlsx")));
    }

    #[tokio::test]
    async fn test_scrape_a27() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-A27-2023.html").unwrap();
        let page = DataPage::scrape(&url).await.unwrap();
        // 全国パターン
        assert_eq!(page.items().len(), 1);

        let a27_001 = page.metadata.attribute.get("A27_001").unwrap();
        assert_eq!(a27_001.name, "行政区域コード");
        let a27_002 = page.metadata.attribute.get("A27_002").unwrap();
        assert_eq!(a27_002.name, "設置主体");
        let a27_003 = page.metadata.attribute.get("A27_003").unwrap();
        assert_eq!(a27_003.name, "学校コード");
        let a27_004 = page.metadata.attribute.get("A27_004").unwrap();
        assert_eq!(a27_004.name, "名称");
        let a27_005 = page.metadata.attribute.get("A27_005").unwrap();
        assert_eq!(a27_005.name, "所在地");
    }

    #[tokio::test]
    async fn test_scrape_a38() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-A38-2020.html").unwrap();
        let page = DataPage::scrape(&url).await.unwrap();
        // 全国パターン
        assert_eq!(page.items().len(), 1);

        let a38a_001 = page.metadata.attribute.get("A38a_001").unwrap();
        assert_eq!(a38a_001.name, "行政区域コード");
        let a38b_001 = page.metadata.attribute.get("A38b_001").unwrap();
        assert_eq!(a38b_001.name, "行政区域コード");
        let a38c_001 = page.metadata.attribute.get("A38c_001").unwrap();
        assert_eq!(a38c_001.name, "都道府県名");
    }

    #[tokio::test]
    async fn test_parse_ref_enum() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/codelist/L01_v3_2_RoadEnumType.html")
                .unwrap();
        let ref_enum = parse_ref_from_url(&url).await.unwrap().unwrap();
        if let RefType::Enum(ref enum_list) = ref_enum {
            assert_eq!(enum_list.len(), 14);
            assert_eq!(enum_list[0], "国道");
            assert_eq!(enum_list[1], "都道");
        } else {
            panic!("Expected RefType::Enum, but got something else.");
        }
    }

    struct TestCase<'a> {
        url: &'a str,
        expected_len: usize,
        expected: HashMap<&'a str, &'a str>,
    }

    async fn run_parse_ref_code_test(test_case: TestCase<'_>) {
        let url = Url::parse(test_case.url).unwrap();
        let ref_enum = parse_ref_from_url(&url).await.unwrap().unwrap();

        match ref_enum {
            RefType::Code(ref code_map) => {
                assert_eq!(code_map.len(), test_case.expected_len);
                for (key, value) in test_case.expected.iter() {
                    assert_eq!(code_map.get(*key).unwrap(), value);
                }
            }
            _ => panic!("Expected RefType::Code, but got something else."),
        }
    }

    #[tokio::test]
    async fn test_parse_ref_code() {
        let test_cases = [
            TestCase {
                url: "https://nlftp.mlit.go.jp/ksj/gml/codelist/reasonForDesignationCode.html",
                expected_len: 7,
                expected: HashMap::from([
                    ("1", "水害（河川）"),
                    ("2", "水害（海）"),
                    ("3", "水害（河川・海）"),
                    ("7", "その他"),
                ]),
            },
            TestCase {
                url: "https://nlftp.mlit.go.jp/ksj/gml/codelist/CodeOfPhenomenon.html",
                expected_len: 3,
                expected: HashMap::from([
                    ("1", "急傾斜地の崩壊"),
                    ("2", "土石流"),
                    ("3", "地滑り"),
                ]),
            },
            TestCase {
                url: "https://nlftp.mlit.go.jp/ksj/gml/codelist/MedClassCd.html",
                expected_len: 3,
                expected: HashMap::from([("1", "病院"), ("2", "診療所"), ("3", "歯科診療所")]),
            },
            TestCase {
                url: "https://nlftp.mlit.go.jp/ksj/gml/codelist/ReferenceDataCd.html",
                expected_len: 6,
                expected: HashMap::from([
                    ("1", "10mDEM"),
                    ("2", "5m空中写真DEM"),
                    ("3", "5mレーザDEM"),
                    ("4", "2mDEM"),
                ]),
            },
            TestCase {
                url: "https://nlftp.mlit.go.jp/ksj/gml/codelist/LandUseCd-09.html",
                expected_len: 17,
                expected: HashMap::from([("0100", "田"), ("1100", "河川地及び湖沼")]),
            },
            TestCase {
                url: "https://nlftp.mlit.go.jp/ksj/gml/codelist/welfareInstitution_welfareFacilityMiddleClassificationCode.html",
                expected_len: 62,
                expected: HashMap::from([
                    ("0101", "救護施設"),
                    ("0399", "その他"),
                ]),
            },
            TestCase {
                url: "https://nlftp.mlit.go.jp/ksj/gml/codelist/water_depth_code.html",
                expected_len: 6,
                expected: HashMap::from([
                    ("1", "0m 以上 0.5m 未満"),
                    ("6", "20.0m 以上"),
                ]),
            }
        ];

        for test_case in test_cases {
            run_parse_ref_code_test(test_case).await;
        }
    }

    #[tokio::test]
    async fn test_extract_data_selections() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N03-2024.html").unwrap();
        let page = DataPage::scrape(&url).await.unwrap();
        assert!(!page.data_selections.is_empty());
        println!("Found {} data selections:", page.data_selections.len());
        for (i, selection) in page.data_selections.iter().enumerate() {
            println!(
                "  {}: {}-{} -> {}",
                i + 1,
                selection.year.start(),
                selection.year.end(),
                selection.url
            );
        }
        for selection in &page.data_selections {
            assert!(*selection.year.start() > 1900 && *selection.year.start() < 2100);
            assert!(selection.url.to_string().starts_with("http"));
        }
    }
}
