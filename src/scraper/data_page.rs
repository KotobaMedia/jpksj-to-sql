use std::collections::HashMap;

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

#[derive(Debug, Serialize)]
pub struct DataPage {
    pub url: Url,
    pub items: Vec<DataItem>,
    pub metadata: DataPageMetadata,
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

pub async fn scrape(url: &Url) -> Result<DataPage> {
    let response = reqwest::get(url.clone()).await?;
    let body = response.text().await?;
    let document = Html::parse_document(&body);

    let metadata = extract_metadata(&document, &url)
        .with_context(|| format!("when accessing {}", url.to_string()))?;

    let td_sel = Selector::parse("td").unwrap();
    let data_tr_sel = Selector::parse("table.dataTables tr, table.dataTables-mesh tr").unwrap();
    let data_path_re = Regex::new(r"javascript:DownLd\('[^']*',\s*'[^']*',\s*'([^']+)'").unwrap();

    let mut items: Vec<DataItem> = Vec::new();
    let mut use_nendo = false;
    for row in document.select(&data_tr_sel) {
        let tds = row.select(&td_sel).collect::<Vec<_>>();
        if tds.len() == 0 {
            continue;
        }
        let area = tds[0].text().collect::<String>().trim().to_string();
        if area == "地域" {
            // header
            if tds[2].text().collect::<String>().contains("年度") {
                use_nendo = true;
            }
            continue;
        }

        let crs = tds[1].text().collect::<String>().trim().to_string();

        let year_str = tds[2].text().collect::<String>().trim().to_string();
        let year = if use_nendo == true {
            None
        } else {
            Some(year_str.clone())
        };
        let nendo = if use_nendo == false {
            None
        } else {
            Some(year_str)
        };
        let Some(bytes_str) = tds[3].text().next().map(|s| s.to_string()) else {
            continue;
        };
        // if we couldn't parse the bytes, we'll just use 10MB as a default. It's just used for progress.
        let bytes: ByteSize = bytes_str.parse().unwrap_or_else(|_| ByteSize::mb(10));
        let Some(file_js_onclick) = tds[5]
            .select(&Selector::parse("a").unwrap())
            .next()
            .and_then(|a| a.value().attr("onclick"))
        else {
            // panic!("file_js_onclick not found: {:?}", tds[5].html());
            continue;
        };
        let Some(file_url) = data_path_re
            .captures(file_js_onclick)
            .map(|c| c.get(1).unwrap().as_str())
            .map(|s| url.join(s).unwrap())
        else {
            // panic!("file_url not found: {:?}", file_js_onclick);
            continue;
        };

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

    items = filter_data_items(items);

    Ok(DataPage {
        url: url.clone(),
        items,
        metadata,
    })
}

#[derive(Debug, Serialize)]
pub struct AttributeMetadata {
    name: String,
    description: String,
    attr_type: String,
    ref_url: Option<Url>,
}

#[derive(Default, Debug, Serialize)]
pub struct DataPageMetadata {
    fundamental: HashMap<String, String>,
    attribute: HashMap<String, AttributeMetadata>,
}

fn extract_metadata<'a, S: Selectable<'a>>(html: S, base_url: &Url) -> Result<DataPageMetadata> {
    let mut metadata = DataPageMetadata::default();
    let table_sel = Selector::parse("table").unwrap();
    let t_cell_sel = Selector::parse("th, td").unwrap();
    let tables: Vec<scraper::ElementRef<'a>> = html.select(&table_sel).collect();

    let strip_space_re = Regex::new(r"\s+").unwrap();

    // 「更新履歴」や「内容」が入っているtableを探す
    let fundamental_table = tables
        .iter()
        .find(|table| {
            let headers: Vec<String> = table
                .select(&t_cell_sel)
                .map(|th| th.text().collect::<String>().trim().to_string())
                .collect();
            headers
                .iter()
                .any(|h| h.contains("更新履歴") || h.contains("内容"))
        })
        .ok_or_else(|| anyhow!("基本情報の table が見つかりませんでした"))?
        .clone();
    let fundamental_parsed = parse_table(fundamental_table);
    let fundamental_parsed_str = parsed_to_string_array(fundamental_parsed);
    // println!("{:?}", fundamental_parsed);
    for row in fundamental_parsed_str.outer_iter() {
        if row.len() < 2 {
            continue;
        }
        let key = row[0].as_ref().unwrap().trim().to_string();
        let mut value = row[1].as_ref().unwrap().trim().to_string();
        value = strip_space_re.replace_all(&value, " ").to_string();
        metadata.fundamental.insert(key, value);
    }

    if metadata.fundamental.is_empty() {
        return Err(anyhow!("基本情報が見つかりませんでした"));
    }

    // ※シェープファイルの属性名の後ろに「▲」を付与している項目は、属性値無しのときは、空欄でなく半角アンダーライン（ _ ）を記述している。
    // TODO: この処理をハンドリングする?
    let attr_key_regex = Regex::new(r"^(.*?)\s*[（(]([a-zA-Z0-9-_]+)▲?[）)]$").unwrap();

    // 「属性情報」や「属性名」が入っているtableを探す
    metadata.attribute = tables
        .iter()
        .find_map(|table| {
            let parsed = parse_table(table.clone());
            // 属性名、説明、属性型
            let mut attr_indices: Option<(usize, usize, usize)> = None;

            let mut attr_map: HashMap<String, AttributeMetadata> = HashMap::new();

            for row in parsed.outer_iter() {
                if row.len() < 3 {
                    continue;
                }
                if let Some((attr_name_idx, desc_idx, type_idx)) = attr_indices {
                    let attr_name_str = row[attr_name_idx]
                        .as_ref()
                        .unwrap()
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
                        .as_ref()
                        .unwrap()
                        .text()
                        .collect::<String>()
                        .trim()
                        .to_string();
                    description = strip_space_re.replace_all(&description, " ").to_string();
                    let attr_type_ele = row[type_idx].as_ref().unwrap();
                    let mut attr_type_str =
                        attr_type_ele.text().collect::<String>().trim().to_string();
                    attr_type_str = strip_space_re.replace_all(&attr_type_str, " ").to_string();

                    let mut ref_url = None;
                    if let Some(a) = attr_type_ele.select(&Selector::parse("a").unwrap()).next() {
                        let href = a.value().attr("href").unwrap();
                        ref_url = Some(base_url.join(href).unwrap());
                    }

                    attr_map.insert(
                        name_id.as_str().to_string(),
                        AttributeMetadata {
                            name: name_jp.as_str().to_string(),
                            description,
                            attr_type: attr_type_str,
                            ref_url,
                        },
                    );
                } else {
                    let mut tmp: (usize, usize, usize) = (0, 0, 0);
                    for (i, cell) in row.iter().enumerate() {
                        let Some(cell) = cell.as_ref() else {
                            continue;
                        };
                        let cell_str = cell.text().collect::<String>().trim().to_string();
                        if cell_str.contains("属性名") {
                            tmp.0 = i;
                        } else if cell_str.contains("説明") {
                            tmp.1 = i;
                        } else if cell_str.contains("属性の型") || cell_str.contains("属性型")
                        {
                            tmp.2 = i;
                        }
                        if (tmp.0 != 0) && (tmp.1 != 0) && (tmp.2 != 0) {
                            attr_indices = Some(tmp);
                            // println!("Found cell indices: {:?}", attr_indices);
                            break;
                        }
                    }
                }
            }

            if attr_map.is_empty() {
                return None;
            }
            Some(attr_map)
        })
        .ok_or_else(|| anyhow!("属性情報の table が見つかりませんでした"))?;

    Ok(metadata)
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

/**
 * データのリストから、CRSが世界測地系のものを抽出する
 * 全国データある場合はそれだけを返す
 * ない場合はそのまま帰す（殆どの場合は都道府県別）
 */
fn filter_data_items(items: Vec<DataItem>) -> Vec<DataItem> {
    // Step 1: Filter items by CRS.
    let crs_filtered: Vec<DataItem> = items
        .into_iter()
        .filter(|item| item.crs == "世界測地系")
        .collect();

    // Step 2: If any item has area == "全国", narrow to those items only.
    let area_filtered: Vec<DataItem> = if crs_filtered.iter().any(|item| item.area == "全国") {
        crs_filtered
            .into_iter()
            .filter(|item| item.area == "全国")
            .collect()
    } else {
        crs_filtered
    };

    // Step 3: Determine the most recent year among items that have one.
    let max_recency = area_filtered
        .iter()
        .filter_map(|item| parse_recency(item))
        .max();

    if let Some(max_year) = max_recency {
        area_filtered
            .into_iter()
            .filter(|item| parse_recency(item) == Some(max_year))
            .collect()
    } else {
        // No valid recency info; return all area_filtered items.
        area_filtered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scrape_c23() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-C23.html").unwrap();
        let page = scrape(&url).await.unwrap();
        assert_eq!(page.items.len(), 39);
    }

    #[tokio::test]
    async fn test_scrape_n03() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N03-2024.html").unwrap();
        let page = scrape(&url).await.unwrap();
        // 全国パターン
        assert_eq!(page.items.len(), 1);

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
        let page = scrape(&url).await.unwrap();
        // 全国パターン
        assert_eq!(page.items.len(), 1);

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
}
