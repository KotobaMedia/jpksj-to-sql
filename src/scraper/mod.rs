// The scraper module is responsible for downloading the data from the website.
use anyhow::Result;
use derive_builder::Builder;
use std::{fmt, path::PathBuf, sync::Arc};

use crate::downloader::path_for_url;

pub mod data_page;
mod download_queue;
pub mod initial;

#[derive(Clone)]
pub struct Dataset {
    // pub item: data_page::DataItem,
    pub initial_item: initial::DataItem,
    pub page: Arc<data_page::DataPage>,
    pub zip_file_paths: Vec<PathBuf>,
}

impl fmt::Display for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Dataset identifier={} url={}",
            self.page.identifier,
            self.page.url.to_string()
        )
    }
}

#[derive(Builder)]
pub struct Scraper {
    skip_dl: bool,
    filter_identifiers: Option<Vec<String>>,
}

impl Scraper {
    pub async fn download_all(&self) -> Result<Vec<Dataset>> {
        let mut dl_queue = download_queue::DownloadQueue::new();
        let initial = initial::scrape().await?;
        let data_items = initial.data;
        let mut out: Vec<Dataset> = Vec::new();
        for initial_item in data_items {
            // TODO: 非商用を対応
            if initial_item.usage == "非商用" {
                continue;
            }

            let page = Arc::new(data_page::scrape(&initial_item.url).await?);
            if let Some(filter_identifiers) = &self.filter_identifiers {
                if !filter_identifiers.contains(&page.identifier) {
                    continue;
                }
            }

            let items = data_page::filter_data_items(page.items.clone());
            let mut zip_file_paths: Vec<PathBuf> = Vec::new();
            for item in items {
                let expected_path = path_for_url(&item.file_url);
                zip_file_paths.push(expected_path.0);
                if !self.skip_dl {
                    dl_queue.push(item).await?;
                }
            }
            out.push(Dataset {
                initial_item,
                page,
                zip_file_paths,
            });
        }
        dl_queue.close().await?;
        Ok(out)
    }
}
