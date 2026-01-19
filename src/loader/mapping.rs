use anyhow::{Context, Result};
use derive_builder::Builder;
use regex::Regex;

use crate::scraper::data_page::{DataPageMetadata, VariantMetadata};
use crate::scraper::Dataset;

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

    // If the template ends with ".shp" (case-insensitive), remove it.
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

struct MultiOutputRule {
    original_identifier: &'static str,
    outputs: Vec<MultiOutputOutput>,
}

struct MultiOutputOutput {
    identifier: &'static str,
    shapefile_matcher: &'static str,
    shapefile_name_regex: Regex,
}

fn multi_output_rules() -> Vec<MultiOutputRule> {
    vec![MultiOutputRule {
        original_identifier: "N03",
        outputs: vec![
            MultiOutputOutput {
                identifier: "N03",
                shapefile_matcher: "N03-YYYYMMDD.shp",
                shapefile_name_regex: Regex::new(
                    r"(?i)(?:^|/)N03-\d{8}(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx)$",
                )
                .unwrap(),
            },
            MultiOutputOutput {
                identifier: "N03_prefecture",
                shapefile_matcher: "N03-YYYYMMDD_prefecture.shp",
                shapefile_name_regex: Regex::new(
                    r"(?i)(?:^|/)N03-\d{8}_prefecture(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx)$",
                )
                .unwrap(),
            },
        ],
    }]
}

fn apply_multi_output_rules(metadata: ShapefileMetadata) -> Vec<ShapefileMetadata> {
    for rule in multi_output_rules() {
        if rule.original_identifier != metadata.original_identifier {
            continue;
        }

        return rule
            .outputs
            .into_iter()
            .map(|output| {
                let mut metadata = metadata.clone();
                metadata.identifier = output.identifier.to_string();
                metadata.shapefile_matcher = vec![output.shapefile_matcher.to_string()];
                metadata.shapefile_name_regex = vec![output.shapefile_name_regex];
                metadata
            })
            .collect();
    }

    vec![metadata]
}

fn field_mappings_from_variant(variant: &VariantMetadata) -> Vec<(String, String)> {
    variant
        .attributes
        .iter()
        .filter_map(|attr| {
            let field_name = attr.readable_name.trim();
            let shape_name = attr.attribute_name.trim();
            if field_name.is_empty() || shape_name.is_empty() {
                return None;
            }
            Some((field_name.to_string(), shape_name.to_string()))
        })
        .collect()
}

fn field_mappings_from_metadata(metadata: &DataPageMetadata) -> Vec<(String, String)> {
    let mut mappings = metadata
        .attribute
        .iter()
        .filter_map(|(attribute_name, attr)| {
            let field_name = attr.name.trim();
            let shape_name = attribute_name.trim();
            if field_name.is_empty() || shape_name.is_empty() {
                return None;
            }
            Some((field_name.to_string(), shape_name.to_string()))
        })
        .collect::<Vec<_>>();

    mappings.sort_by(|a, b| a.1.cmp(&b.1));
    mappings
}

fn fallback_variant(dataset: &Dataset) -> VariantMetadata {
    VariantMetadata {
        variant_name: dataset.initial_item.name.clone(),
        variant_identifier: dataset.initial_item.identifier.clone(),
        shapefile_hint: None,
        attributes: vec![],
    }
}

pub async fn mapping_defs_for_dataset(dataset: &Dataset) -> Result<Vec<ShapefileMetadata>> {
    let original_identifier = dataset.initial_item.identifier.clone();
    let mut variants = dataset.page.variants.clone();
    if variants.is_empty() {
        variants.push(fallback_variant(dataset));
    }

    let mut mappings = Vec::new();
    for variant in variants {
        let mut field_mappings = field_mappings_from_variant(&variant);
        if field_mappings.is_empty() {
            field_mappings = field_mappings_from_metadata(&dataset.page.metadata);
        }

        let name = if variant.variant_name.trim().is_empty() {
            dataset.initial_item.name.clone()
        } else {
            variant.variant_name.clone()
        };

        let identifier = if variant.variant_identifier.trim().is_empty() {
            original_identifier.clone()
        } else {
            variant.variant_identifier.clone()
        };

        let mut builder = ShapefileMetadataBuilder::default();
        builder.cat1(dataset.initial_item.category1_name.clone());
        builder.cat2(dataset.initial_item.category2_name.clone());
        builder.name(format_name(&name));
        builder.version(format!(
            "{}-{}",
            dataset.page.version.start_year, dataset.page.version.end_year
        ));
        builder.data_year(dataset.page.version.end_year.to_string());
        builder.original_identifier(original_identifier.clone());
        builder.identifier(identifier);
        builder.field_mappings(field_mappings);

        if let Some(matchers) = variant
            .shapefile_hint
            .as_ref()
            .map(|s| split_shapefile_matcher(s))
            .filter(|m| !m.is_empty())
        {
            builder.shapefile_matcher(matchers);
        }

        let metadata = builder
            .build()
            .with_context(|| "when building shapefile metadata from API")?;
        mappings.push(metadata);
    }

    let mappings = if mappings.len() == 1 && mappings[0].identifier == original_identifier {
        apply_multi_output_rules(mappings.into_iter().next().unwrap())
    } else {
        mappings
    };

    Ok(mappings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scraper::{data_page, initial};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_mapping_defs_for_dataset() {
        let initial = initial::scrape().await.unwrap();
        let data_item = initial
            .data
            .into_iter()
            .find(|item| item.identifier == "N03")
            .unwrap();
        let page = data_page::scrape(&data_item.identifier, Some(2024))
            .await
            .unwrap();
        let dataset = Dataset {
            initial_item: data_item,
            page: Arc::new(page),
            zip_file_paths: vec![],
        };

        let mappings = mapping_defs_for_dataset(&dataset).await.unwrap();
        assert!(!mappings.is_empty());
        assert!(mappings.iter().all(|m| !m.field_mappings.is_empty()));
    }
}
