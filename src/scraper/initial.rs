use anyhow::{Context, Result};
use serde::Serialize;
use url::Url;

use crate::scraper::api;

#[derive(Debug, Clone, Serialize)]
pub struct DataItem {
    pub category1_name: String,
    pub category2_name: String,
    pub name: String,
    pub data_source: String,
    pub data_accuracy: String,
    pub metadata_xml: Url,
    pub usage: String,

    pub url: Url,
    pub identifier: String,
}

#[derive(Debug)]
pub struct ScrapeResult {
    #[allow(dead_code)]
    pub url: Url,
    pub data: Vec<DataItem>,
}

pub async fn scrape() -> Result<ScrapeResult> {
    let datasets = api::fetch_dataset_list()
        .await
        .context("when requesting dataset list from JPKSJ API")?;

    let data = datasets
        .into_iter()
        .map(|item| DataItem {
            category1_name: item.category1_name,
            category2_name: item.category2_name,
            name: item.name,
            data_source: String::new(),
            data_accuracy: String::new(),
            metadata_xml: item.source_url.clone(),
            usage: String::new(),
            url: item.source_url,
            identifier: item.id,
        })
        .collect::<Vec<_>>();

    Ok(ScrapeResult {
        url: api::dataset_list_url()?,
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scrape() {
        let result = scrape().await.unwrap();
        assert_eq!(result.data.len(), 126);
        let first = result.data.get(0).unwrap();
        assert_eq!(first.name, "海岸線");
        assert_eq!(first.identifier, "C23");
    }
}
