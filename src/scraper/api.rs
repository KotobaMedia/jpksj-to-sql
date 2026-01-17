use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use url::Url;

pub const API_BASE_URL: &str = "https://jpksj-api.kmproj.com/";

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetListItem {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub category1_name: String,
    pub category2_name: String,
    pub id: String,
    pub source_url: Url,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetDetailVersion {
    pub id: String,
    pub start_year: u32,
    pub end_year: u32,
    #[serde(default)]
    pub most_recent: bool,
    pub source_url: Url,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetDetail {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub id: String,
    pub versions: Vec<DatasetDetailVersion>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetAttribute {
    pub readable_name: String,
    pub attribute_name: String,
    pub description: String,
    #[serde(rename = "type")]
    pub attr_type: String,
    #[serde(default)]
    pub type_ref_url: Option<Url>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetVariant {
    pub variant_name: String,
    pub variant_identifier: String,
    #[serde(default)]
    pub geometry_type: Option<String>,
    #[serde(default)]
    pub geometry_description: Option<String>,
    #[serde(default)]
    pub shapefile_hint: Option<String>,
    #[serde(default)]
    pub attributes: Vec<DatasetAttribute>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetFile {
    pub area: String,
    pub bytes: u64,
    #[serde(default)]
    pub year: Option<u32>,
    pub file_url: Url,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetVersionDetail {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub id: String,
    pub id_with_version: String,
    pub start_year: u32,
    pub end_year: u32,
    #[serde(default)]
    pub variants: Vec<DatasetVariant>,
    #[serde(default)]
    pub files: Vec<DatasetFile>,
}

pub fn dataset_list_url() -> Result<Url> {
    api_url("datasets.json")
}

pub async fn fetch_dataset_list() -> Result<Vec<DatasetListItem>> {
    let url = dataset_list_url()?;
    fetch_json(url).await
}

pub async fn fetch_dataset_detail(id: &str) -> Result<DatasetDetail> {
    let url = api_url(&format!("datasets/{}.json", id))?;
    fetch_json(url).await
}

pub async fn fetch_dataset_version(id: &str, version_id: &str) -> Result<DatasetVersionDetail> {
    let url = api_url(&format!("datasets/{}/{}.json", id, version_id))?;
    fetch_json(url).await
}

fn api_url(path: &str) -> Result<Url> {
    Url::parse(API_BASE_URL)?.join(path).context("when building JPKSJ API url")
}

async fn fetch_json<T: DeserializeOwned>(url: Url) -> Result<T> {
    let response = reqwest::get(url.clone())
        .await
        .with_context(|| format!("when requesting {}", url))?
        .error_for_status()
        .with_context(|| format!("when checking response from {}", url))?;
    let parsed = response
        .json::<T>()
        .await
        .with_context(|| format!("when parsing JSON from {}", url))?;
    Ok(parsed)
}
