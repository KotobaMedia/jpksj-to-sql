use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;
use url::Url;

use super::api;

// Compile the regex once for efficiency.
// This regex looks for one or more digits at the very start of the string.
static YEAR_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(\d+)(?:年|年度)?").unwrap());

#[derive(Debug, Clone, Serialize)]
pub struct VariantAttribute {
    pub readable_name: String,
    pub attribute_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VariantMetadata {
    pub variant_name: String,
    pub variant_identifier: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shapefile_hint: Option<String>,
    pub attributes: Vec<VariantAttribute>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DataPageVersion {
    pub id: String,
    pub start_year: u32,
    pub end_year: u32,
}

#[derive(Debug, Serialize)]
pub struct DataPage {
    pub url: Url,
    pub items: Vec<DataItem>,
    pub metadata: DataPageMetadata,
    pub variants: Vec<VariantMetadata>,
    pub version: DataPageVersion,
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

    let variants = version_detail
        .variants
        .iter()
        .map(|variant| VariantMetadata {
            variant_name: variant.variant_name.clone(),
            variant_identifier: variant.variant_identifier.clone(),
            shapefile_hint: variant.shapefile_hint.clone(),
            attributes: variant
                .attributes
                .iter()
                .map(|attr| VariantAttribute {
                    readable_name: attr.readable_name.clone(),
                    attribute_name: attr.attribute_name.clone(),
                })
                .collect(),
        })
        .collect::<Vec<_>>();

    let version_info = DataPageVersion {
        id: version.id.clone(),
        start_year: version.start_year,
        end_year: version.end_year,
    };

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
        variants,
        version: version_info,
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
            let ref_url = attr.type_ref_url.clone();
            let r#ref = if ref_url
                .as_ref()
                .is_some_and(|url| url.as_str().contains(".xlsx"))
            {
                // AdminiBoundary_CD.xlsx is handled separately in admini_boundary.rs
                None
            } else {
                parse_ref_from_attribute(attr).with_context(|| {
                    format!("when parsing ref list for {}", attr.attribute_name)
                })?
            };
            attr_map.insert(
                attr.attribute_name.clone(),
                AttributeMetadata {
                    name: attr.readable_name.clone(),
                    description: attr.description.clone(),
                    attr_type: attr.attr_type.clone(),
                    ref_url,
                    r#ref,
                },
            );
        }
    }
    metadata.attribute = attr_map;

    Ok(metadata)
}

#[derive(Debug, Clone, Serialize)]
pub enum RefType {
    Enum(Vec<String>),
    Code(HashMap<String, String>),
}

fn parse_ref_from_attribute(attr: &api::DatasetAttribute) -> Result<Option<RefType>> {
    if let Some(ref_code) = attr.type_ref_code.as_ref() {
        return parse_ref_code_list(ref_code).map(Some);
    }
    if let Some(ref_enum) = attr.type_ref_enum.as_ref() {
        return parse_ref_enum_list(ref_enum).map(Some);
    }
    Ok(None)
}

fn parse_ref_code_list(entries: &[String]) -> Result<RefType> {
    let mut code_map = HashMap::new();
    for entry in entries {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.splitn(2, ':');
        let code = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("invalid code entry: {}", entry))?;
        let name = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("invalid code entry: {}", entry))?;
        code_map.insert(code.to_string(), name.to_string());
    }
    if code_map.is_empty() {
        return Err(anyhow!("no code entries found"));
    }
    Ok(RefType::Code(code_map))
}

fn parse_ref_enum_list(entries: &[String]) -> Result<RefType> {
    let enum_list = entries
        .iter()
        .map(|entry| entry.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    if enum_list.is_empty() {
        return Err(anyhow!("no enum entries found"));
    }
    Ok(RefType::Enum(enum_list))
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

        let c23_002 = page.metadata.attribute.get("C23_002").unwrap();
        match c23_002.r#ref.as_ref().unwrap() {
            RefType::Code(code_map) => {
                assert_eq!(code_map.get("1").unwrap(), "国土交通省河川局");
                assert_eq!(code_map.get("0").unwrap(), "その他");
            }
            _ => panic!("Expected RefType::Code, but got something else."),
        }
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
        let page = scrape("L01", Some(2025)).await.unwrap();
        let l01_028 = page.metadata.attribute.get("L01_028").unwrap();
        match l01_028.r#ref.as_ref().unwrap() {
            RefType::Enum(enum_list) => {
                assert!(!enum_list.is_empty());
                assert!(enum_list.iter().any(|value| value == "住宅"));
                assert!(enum_list.iter().any(|value| value == "その他"));
            }
            _ => panic!("Expected RefType::Enum, but got something else."),
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
