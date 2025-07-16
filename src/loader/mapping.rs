use anyhow::Result;
use calamine::{Reader, Xlsx};
use derive_builder::Builder;
use regex::Regex;
use std::vec;
use tokio::sync::OnceCell;
use url::Url;

use crate::downloader;

use super::xlsx_helpers::data_to_string;

#[derive(Builder, Clone, Debug)]
#[builder(derive(Debug))]
pub struct ShapefileMetadata {
    #[allow(dead_code)]
    pub cat1: String, // 4. 交通
    #[allow(dead_code)]
    pub cat2: String, // 交通
    pub name: String, // 鉄道時系列（ライン）
    #[allow(dead_code)]
    pub version: String, // 2023年度版
    #[allow(dead_code)]
    pub data_year: String, // 令和5年度

    /// シェープファイル名
    /// （表記中のYYは年次、MMは月、PPは都道府県コード、CCCCCは市区町村コード、AAは支庁コード、mmmmはメッシュコードを示します。）
    #[allow(dead_code)]
    #[builder(default = "vec![]")]
    pub shapefile_matcher: Vec<String>,
    // // parsed version of shapefile_matcher; computed from shapefile_matcher
    #[builder(setter(skip), default = "self.create_shapefile_name_regex()?")]
    pub shapefile_name_regex: Vec<Regex>,

    pub field_mappings: Vec<(String, String)>,

    /// 元データの識別子
    /// インポート識別子はインポート後のテーブル名になります。これは、単一データセットに対して複数テーブルとして扱う場合に必要です。
    pub original_identifier: String,
    /// インポート識別子
    pub identifier: String,
}

// fn create_shapefile_name_regex(_template_string: String) -> Result<Regex, String> {
//     Regex::new(r"(?i:(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx))$").map_err(|e| e.to_string())
// }

fn format_name(name: &str) -> String {
    let mut formatted_name = name.to_string();
    // Remove any parentheses and their contents
    let remove_re = Regex::new(r"（[^）]+）").unwrap();
    formatted_name = remove_re.replace_all(&formatted_name, "").to_string();
    // Remove leading and trailing whitespace
    formatted_name = formatted_name.trim().to_string();
    formatted_name
}

fn create_shapefile_name_regex(template_string: String) -> Result<Regex, String> {
    let remove_re = Regex::new(r"（[^）]+）").unwrap();
    let template = remove_re.replace_all(template_string.as_str(), "");
    let template = template.trim();

    // If the template ends with ".shp" (case‑insensitive), remove it.
    let base_template = if template.to_lowercase().ends_with(".shp") {
        // Remove the last 4 characters (".shp")
        &template[..template.len() - 4]
    } else {
        &template
    };

    // This regex matches only the allowed placeholders.
    // Note: We deliberately list the tokens so that only these are replaced.
    let token_pattern = r"(YY|MM|PP|CCCCC|AA|mmmm)";
    let re = Regex::new(token_pattern).unwrap();

    let mut result = String::from("(?:^|/)");
    let mut last_index = 0;

    // Iterate over each found token in the template.
    for mat in re.find_iter(base_template) {
        // Escape and append the literal text before the token.
        let escaped = regex::escape(&base_template[last_index..mat.start()]);
        result.push_str(&escaped);

        // Use the length of the token to determine the number of digits.
        let token = mat.as_str();
        let replacement = format!("\\d{{{}}}", token.len());
        result.push_str(&replacement);

        last_index = mat.end();
    }

    // Append and escape any trailing literal text.
    result.push_str(&regex::escape(&base_template[last_index..]));
    result.push_str(r"(?i:(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx))$");

    Regex::new(&result).map_err(|e| e.to_string())
}

impl ShapefileMetadataBuilder {
    fn create_shapefile_name_regex(&self) -> Result<Vec<Regex>, String> {
        match &self.shapefile_matcher {
            None => {
                return Ok(vec![Regex::new(
                    r"(?i:(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx))$",
                )
                .unwrap()]);
            }
            Some(ref template_strings) => template_strings
                .iter()
                .map(|s| create_shapefile_name_regex(s.clone()))
                .collect(),
        }
    }
}

/// Splits and normalizes a shapefile matcher string into a vector of strings.
fn split_shapefile_matcher(s: &str) -> Vec<String> {
    s.replace("\r\n", "\n")
        .replace("A38-YY_PP_", "A38-YY_") // 医療圏のshapefile名が間違っている
        .split('\n')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn should_start_new_metadata_record(
    builder: &ShapefileMetadataBuilder,
    row: &[calamine::Data],
) -> bool {
    let cat1 = data_to_string(&row[0]);
    let cat2 = data_to_string(&row[1]);
    let shapefile_names = data_to_string(&row[5])
        .map(|s| split_shapefile_matcher(&s))
        .and_then(|v| if v.is_empty() { None } else { Some(v) });
    let original_identifier = data_to_string(&row[8]);
    let mapping_id = data_to_string(&row[7]);

    if let (Some(cat1), Some(cat2)) = (cat1, cat2) {
        if builder.cat1.clone().is_some_and(|s| s != cat1)
            || builder.cat2.clone().is_some_and(|s| s != cat2)
        {
            return true;
        }
    }

    if let Some(shapefile_names) = shapefile_names {
        if builder
            .shapefile_matcher
            .clone()
            .is_some_and(|s| s != shapefile_names)
        {
            return true;
        }
    }

    if let (Some(identifier), Some(mapping_id)) = (original_identifier, mapping_id) {
        // 例外: 医療圏。１，２，３次医療圏はそれぞれ別テーブルとして扱う。
        // -> 識別子はそれぞれA38だが、属性コードの頭4文字が異なる（A38a, A38b, A38c）
        if identifier == "A38"
            && builder.field_mappings.as_ref().is_some_and(|m| {
                m.last().is_some_and(|(_, prev_mapping_id)| {
                    !mapping_id.starts_with(&prev_mapping_id.chars().take(4).collect::<String>())
                })
            })
        {
            return true;
        }
    }
    false
}

fn extract_identifier_from_row(row: &[calamine::Data]) -> Option<String> {
    let identifier = data_to_string(&row[8]);
    if let Some(ref id) = identifier {
        if id == "A38" {
            if let Some(mapping_id) = data_to_string(&row[7]) {
                // Take the first 4 characters of mapping_id, or the whole string if shorter
                return Some(mapping_id.chars().take(4).collect());
            }
        }
    }
    identifier
}

async fn download_mapping_definition_file() -> Result<downloader::DownloadedFile> {
    let url = Url::parse("https://nlftp.mlit.go.jp/ksj/gml/codelist/shape_property_table2.xlsx")?;
    downloader::download_to_tmp(&url).await
}

async fn parse_mapping_file() -> Result<Vec<ShapefileMetadata>> {
    let file = download_mapping_definition_file().await?;
    let path = file.path;
    let mut workbook: Xlsx<_> = calamine::open_workbook(&path)?;
    let mut out: Vec<ShapefileMetadata> = Vec::new();
    let sheet = workbook.worksheet_range("全データ")?;
    let mut data_started = false;

    let mut builder = ShapefileMetadataBuilder::default();
    for row in sheet.rows() {
        let cat1 = data_to_string(&row[0]);

        if !data_started {
            if cat1.is_some_and(|s| s == "大分類") {
                data_started = true;
            }
            continue;
        }

        if should_start_new_metadata_record(&builder, row) {
            match builder.build() {
                Ok(metadata) => out.push(metadata),
                Err(e) => panic!("Error: {}, {:?}", e, builder),
            }
            builder = ShapefileMetadataBuilder::default();
        }

        if let Some(original_identifier) = data_to_string(&row[8]) {
            if builder.original_identifier.is_none() {
                builder.original_identifier(original_identifier);
            }
        }
        if let Some(identifier) = extract_identifier_from_row(row) {
            if builder.identifier.is_none() {
                builder.identifier(identifier.clone());
            }

            let name_override = match identifier.as_str() {
                "A38a" => Some("一次医療圏"),
                "A38b" => Some("二次医療圏"),
                "A38c" => Some("三次医療圏"),
                _ => None,
            };
            if let Some(name) = name_override {
                builder.name(name.to_string());
            }
        }

        if let Some(cat1) = cat1 {
            builder.cat1(cat1);
        }
        if let Some(cat2) = data_to_string(&row[1]) {
            builder.cat2(cat2);
        }
        if let Some(name) = data_to_string(&row[2]) {
            if builder.name.is_none() {
                builder.name(format_name(&name));
            }
        }
        if let Some(version) = data_to_string(&row[3]) {
            builder.version(version);
        }
        if let Some(data_year) = data_to_string(&row[4]) {
            builder.data_year(data_year);
        }
        if let Some(shapefile_matcher) = data_to_string(&row[5]) {
            let mut matchers = builder.shapefile_matcher.clone().unwrap_or(vec![]);
            matchers.extend(split_shapefile_matcher(&shapefile_matcher));
            builder.shapefile_matcher(matchers);
        }

        if let Some((field_name, shape_name)) = data_to_string(&row[6]).zip(data_to_string(&row[7]))
        {
            let mut mappings = builder.field_mappings.clone().unwrap_or(vec![]);
            mappings.push((field_name, shape_name));
            builder.field_mappings(mappings);
        }
    }

    // last row
    if let Ok(metadata) = builder.build() {
        out.push(metadata);
    }

    Ok(out)
}

static MAPPING_DEFS: OnceCell<Vec<ShapefileMetadata>> = OnceCell::const_new();
pub async fn mapping_defs() -> Result<&'static Vec<ShapefileMetadata>> {
    MAPPING_DEFS
        .get_or_try_init(|| async { parse_mapping_file().await })
        .await
}

pub async fn find_mapping_def_for_entry(identifier: &str) -> Result<Vec<ShapefileMetadata>> {
    let defs = mapping_defs().await?;
    Ok(defs
        .iter()
        .filter(|def| def.original_identifier == identifier)
        .cloned()
        .collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_mapping_file() {
        let result = parse_mapping_file().await;
        assert!(result.is_ok());
        let data = &result.unwrap();

        let metadata = &data[0];
        assert_eq!(metadata.cat1, "2. 政策区域");
        assert_eq!(metadata.cat2, "大都市圏・条件不利地域");
        assert_eq!(metadata.name, "三大都市圏計画区域");
        assert_eq!(metadata.version, "2003年度版");
        assert_eq!(metadata.data_year, "平成15年度");
        assert_eq!(
            metadata.shapefile_matcher,
            vec!["A03-YY_SYUTO-g_ThreeMajorMetroPlanArea.shp"]
        );
        assert_eq!(metadata.field_mappings.len(), 8);

        // find 医療圏
        let metadata = data
            .iter()
            .filter(|m| m.name.contains("医療圏"))
            .collect::<Vec<_>>();
        assert_eq!(metadata.len(), 3);
        assert_eq!(
            metadata.iter().map(|m| m.name.clone()).collect::<Vec<_>>(),
            vec!["一次医療圏", "二次医療圏", "三次医療圏"]
        );
    }
}
