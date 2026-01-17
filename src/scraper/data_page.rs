use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use scraper::{Html, Selector};
use serde::Serialize;
use url::Url;

use super::api;

// Compile the regex once for efficiency.
// This regex looks for one or more digits at the very start of the string.
static YEAR_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(\d+)(?:年|年度)?").unwrap());

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

pub async fn scrape(identifier: &str, year: Option<u32>) -> Result<DataPage> {
    let dataset = api::fetch_dataset_detail(identifier)
        .await
        .with_context(|| format!("when requesting dataset detail for {}", identifier))?;

    let Some(version) = select_version(&dataset.versions, year) else {
        return Err(anyhow!("No versions found for {}", identifier));
    };

    let version_detail = api::fetch_dataset_version(identifier, &version.id)
        .await
        .with_context(|| {
            format!(
                "when requesting dataset version detail for {} {}",
                identifier, version.id
            )
        })?;

    let metadata = build_metadata_from_api(&dataset, &version_detail).await?;

    let mut items: Vec<DataItem> = version_detail
        .files
        .into_iter()
        .map(|file| {
            let year_str = file
                .year
                .filter(|y| *y > 0)
                .map(|y| format!("{}年", y));
            DataItem {
                area: file.area,
                crs: String::new(),
                bytes: file.bytes,
                year: year_str,
                nendo: None,
                file_url: file.file_url,
            }
        })
        .collect();

    items = filter_data_items(items, year);

    Ok(DataPage {
        url: version.source_url.clone(),
        items,
        metadata,
    })
}

fn select_version<'a>(
    versions: &'a [api::DatasetDetailVersion],
    year: Option<u32>,
) -> Option<&'a api::DatasetDetailVersion> {
    if let Some(target_year) = year {
        if let Some(version) = versions
            .iter()
            .find(|v| target_year >= v.start_year && target_year <= v.end_year)
        {
            return Some(version);
        }
    }

    versions
        .iter()
        .find(|v| v.most_recent)
        .or_else(|| versions.first())
}

async fn build_metadata_from_api(
    dataset: &api::DatasetDetail,
    version_detail: &api::DatasetVersionDetail,
) -> Result<DataPageMetadata> {
    let mut metadata = DataPageMetadata::default();

    let mut content_parts: Vec<String> = Vec::new();
    for variant in &version_detail.variants {
        if let Some(desc) = variant.geometry_description.as_ref() {
            let trimmed = desc.trim();
            if !trimmed.is_empty() {
                content_parts.push(trimmed.to_string());
            }
        }
    }

    let content = if !content_parts.is_empty() {
        content_parts.join(" / ")
    } else if !version_detail.description.trim().is_empty() {
        version_detail.description.trim().to_string()
    } else if !dataset.description.trim().is_empty() {
        dataset.description.trim().to_string()
    } else {
        dataset.name.trim().to_string()
    };

    metadata
        .fundamental
        .insert("内容".to_string(), content);

    let mut attr_map: HashMap<String, AttributeMetadata> = HashMap::new();
    for variant in &version_detail.variants {
        for attr in &variant.attributes {
            attr_map.insert(
                attr.attribute_name.clone(),
                AttributeMetadata {
                    name: attr.readable_name.clone(),
                    description: attr.description.clone(),
                    attr_type: attr.attr_type.clone(),
                    ref_url: attr.type_ref_url.clone(),
                    r#ref: None,
                },
            );
        }
    }
    metadata.attribute = attr_map;

    for attr in metadata.attribute.values_mut() {
        if let Some(ref_url) = &attr.ref_url {
            if ref_url.to_string().contains(".xlsx") {
                // AdminiBoundary_CD.xlsx is handled separately in admini_boundary.rs
                continue;
            }
            attr.r#ref = parse_ref_from_url(ref_url)
                .await
                .with_context(|| format!("when accessing ref url: {}", ref_url))?;
        }
    }

    Ok(metadata)
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
    let trimmed = field.trim();
    YEAR_REGEX
        .captures(trimmed)
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
fn filter_data_items(items: Vec<DataItem>, year: Option<u32>) -> Vec<DataItem> {
    // Step 1: Filter items by CRS if it is known.
    let crs_filtered: Vec<DataItem> = items
        .into_iter()
        .filter(|item| item.crs.is_empty() || item.crs == "世界測地系")
        .collect();

    // Step 2: Group items by area.
    let mut area_groups: HashMap<String, Vec<DataItem>> = HashMap::new();
    for item in crs_filtered {
        // If 全国 is already in the map, and we aren't in the 全国 group, skip this item.
        if area_groups.contains_key("全国") && item.area != "全国" {
            continue;
        }
        area_groups.entry(item.area.clone()).or_default().push(item);
    }

    // Step 3: For each area evaluate the max recency and filter items accordingly.
    let mut result = Vec::new();
    for (_area, group) in area_groups {
        let max_recency = match year {
            Some(y) => Some(y),
            None => group.iter().filter_map(|item| parse_recency(item)).max(),
        };
        if let Some(max_year) = max_recency {
            result.extend(
                group
                    .into_iter()
                    .filter(|item| parse_recency(item) == Some(max_year)),
            );
        } else {
            result.extend(group);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scrape_c23() {
        let page = scrape("C23", None).await.unwrap();
        assert_eq!(page.items.len(), 39);
    }

    #[tokio::test]
    async fn test_scrape_n03() {
        let page = scrape("N03", Some(2024)).await.unwrap();
        // 全国パターン
        assert_eq!(page.items.len(), 1);

        let naiyo = page.metadata.fundamental.get("内容").unwrap();
        assert!(naiyo.contains("行政界"));

        let todoufukenmei = page.metadata.attribute.get("N03_001").unwrap();
        assert!(todoufukenmei.name.contains("都道府県名"));
        assert!(todoufukenmei.description.contains("都道府県"));
        assert!(todoufukenmei.attr_type.contains("CharacterString"));

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
        let page = scrape("A27", Some(2023)).await.unwrap();
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

    #[tokio::test]
    async fn test_scrape_a38() {
        let page = scrape("A38", Some(2020)).await.unwrap();
        // 全国パターン
        assert_eq!(page.items.len(), 1);

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
            },
        ];

        for test_case in test_cases {
            run_parse_ref_code_test(test_case).await;
        }
    }

    #[tokio::test]
    async fn test_scrape_specific_year() {
        let page = scrape("N03", Some(2011)).await.unwrap();
        assert!(!page.items.is_empty());
        for item in page.items {
            let year = parse_recency(&item).unwrap();
            assert_eq!(year, 2011);
        }
    }
}
