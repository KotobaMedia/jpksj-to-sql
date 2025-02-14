use std::{path::PathBuf, vec};

// the module responsible for mapping shapefile field names to database column names.
// this module also can recognize the metadata of the shapefile
use anyhow::Result;
use calamine::{Data, DataType, Reader, Xlsx};
use derive_builder::Builder;
use regex::Regex;
use tokio::sync::OnceCell;
use url::Url;

use crate::downloader;

#[derive(Builder, Clone, Debug)]
#[builder(derive(Debug))]
pub struct ShapefileMetadata {
    pub cat1: String,      // 4. 交通
    pub cat2: String,      // 交通
    pub name: String,      // 鉄道時系列（ライン）
    pub version: String,   // 2023年度版
    pub data_year: String, // 令和5年度

    /// シェープファイル名
    /// （表記中のYYは年次、MMは月、PPは都道府県コード、CCCCCは市区町村コード、AAは支庁コード、mmmmはメッシュコードを示します。）
    #[builder(default = "vec![]")]
    pub shapefile_matcher: Vec<String>,
    // // parsed version of shapefile_matcher; computed from shapefile_matcher
    #[builder(setter(skip), default = "self.create_shapefile_name_regex()?")]
    pub shapefile_name_regex: Vec<Regex>,

    pub field_mappings: Vec<(String, String)>,
    pub identifier: String,
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
                return Ok(vec![Regex::new(r"\.shp$").unwrap()]);
            }
            Some(ref template_strings) => template_strings
                .iter()
                .map(|s| create_shapefile_name_regex(s.clone()))
                .collect(),
        }
    }
}

async fn download_mapping_definition_file(tmp: &PathBuf) -> Result<downloader::DownloadedFile> {
    let url = Url::parse("https://nlftp.mlit.go.jp/ksj/gml/codelist/shape_property_table2.xlsx")?;
    downloader::download_to_tmp(&tmp, &url).await
}

fn data_to_string(data: &Data) -> Option<String> {
    data.get_string()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

async fn parse_mapping_file(tmp: &PathBuf) -> Result<Vec<ShapefileMetadata>> {
    let file = download_mapping_definition_file(&tmp).await?;
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

        if let Some(((cat1, cat2), name)) = cat1
            .zip(data_to_string(&row[1]))
            .zip(data_to_string(&row[2]))
        {
            if builder.cat1.clone().is_some_and(|s| s != cat1)
                || builder.cat2.clone().is_some_and(|s| s != cat2)
                || builder.name.clone().is_some_and(|s| s != name)
            {
                match builder.build() {
                    Ok(metadata) => out.push(metadata),
                    Err(e) => panic!("Error: {}, {:?}, current out: {:?}", e, builder, out),
                }
                builder = ShapefileMetadataBuilder::default();
            }

            builder.cat1(cat1);
            builder.cat2(cat2);
            builder.name(name);
        }

        if let Some(version) = data_to_string(&row[3]) {
            builder.version(version);
        }
        if let Some(data_year) = data_to_string(&row[4]) {
            builder.data_year(data_year);
        }
        if let Some(identifier) = data_to_string(&row[8]) {
            builder.identifier(identifier);
        }
        if let Some(shapefile_matcher) = data_to_string(&row[5]) {
            let mut matchers = builder.shapefile_matcher.clone().unwrap_or(vec![]);
            shapefile_matcher
                .split("\n")
                .for_each(|s| matchers.push(s.to_string()));
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
pub async fn mapping_defs(tmp: &PathBuf) -> Result<&Vec<ShapefileMetadata>> {
    MAPPING_DEFS
        .get_or_try_init(|| async { parse_mapping_file(&tmp).await })
        .await
}

pub async fn find_mapping_def_for_entry(
    tmp: &PathBuf,
    identifier: &str,
) -> Result<Option<ShapefileMetadata>> {
    let defs = mapping_defs(&tmp).await?;
    Ok(defs
        .iter()
        .find(|def| def.identifier == identifier)
        .cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_mapping_file() {
        let tmp = PathBuf::from("./tmp");
        let result = parse_mapping_file(&tmp).await;
        assert!(result.is_ok());
        let data = &result.unwrap();

        let metadata = &data[0];
        assert_eq!(metadata.cat1, "2. 政策区域");
        assert_eq!(metadata.cat2, "大都市圏・条件不利地域");
        assert_eq!(metadata.name, "三大都市圏計画区域（ポリゴン）");
        assert_eq!(metadata.version, "2003年度版");
        assert_eq!(metadata.data_year, "平成15年度");
        assert_eq!(
            metadata.shapefile_matcher,
            vec!["A03-YY_SYUTO-g_ThreeMajorMetroPlanArea.shp"]
        );
        assert_eq!(metadata.field_mappings.len(), 8);
    }
}
