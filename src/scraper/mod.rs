// The scraper module is responsible for downloading the data from the website.
use anyhow::Result;
use derive_builder::Builder;
use std::{fmt, path::PathBuf, sync::Arc};

use crate::downloader::path_for_url;

pub mod data_page;
mod download_queue;
pub mod initial;
pub mod ref_parser;
mod table_read;
pub mod year_parser;

#[cfg(test)]
mod test_helpers;

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
            self.initial_item.identifier,
            self.page.url.to_string()
        )
    }
}

impl Dataset {
    pub fn identifier(&self) -> String {
        self.initial_item.identifier.clone()
    }
}

#[derive(Builder)]
pub struct Scraper {
    skip_dl: bool,
    filter_identifiers: Option<Vec<String>>,
    year: Option<Vec<u32>>,
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
            if let Some(filter_identifiers) = &self.filter_identifiers {
                if !filter_identifiers.contains(&initial_item.identifier) {
                    continue;
                }
            }

            let datasets = self.download_one(&mut dl_queue, initial_item).await?;
            out.extend(datasets);
        }

        dl_queue.close().await?;
        Ok(out)
    }

    async fn download_one(
        &self,
        dl_queue: &mut download_queue::DownloadQueue,
        initial_item: initial::DataItem,
    ) -> Result<Vec<Dataset>> {
        let page_res =
            data_page::DataPage::scrape(&initial_item.url, &self.year.clone().unwrap_or(vec![]))
                .await;

        if let Err(err) = page_res {
            println!("[ERROR, skipping...] {:?}", err);
            return Ok(vec![]);
        }

        let page = Arc::new(page_res.unwrap());
        let mut datasets = vec![
            self.process_page(dl_queue, initial_item.clone(), page.clone())
                .await?,
        ];

        for yearly_version in &page.yearly_versions {
            let page_res = data_page::DataPage::scrape(
                &yearly_version.url,
                &self.year.clone().unwrap_or(vec![]),
            )
            .await?;
            let page = Arc::new(page_res);
            let dataset = self
                .process_page(dl_queue, initial_item.clone(), page.clone())
                .await?;
            datasets.push(dataset);
        }

        Ok(datasets)
    }

    async fn process_page(
        &self,
        dl_queue: &mut download_queue::DownloadQueue,
        initial_item: initial::DataItem,
        page: Arc<data_page::DataPage>,
    ) -> Result<Dataset> {
        // this page is the most recent page.
        // previous versions are in yearly_versions; we'll loop through them

        let mut zip_file_paths: Vec<PathBuf> = Vec::new();
        for item in &page.items() {
            let expected_path = path_for_url(&item.file_url);
            zip_file_paths.push(expected_path.0);

            // download the file -- we'll be using it later.
            if !self.skip_dl {
                dl_queue.push(item.clone()).await?;
            }
        }

        Ok(Dataset {
            initial_item,
            page,
            zip_file_paths,
        })
    }
}
