// The loader module is responsible for loading data from ZIP files and in to the database.

use crate::scraper::Dataset;
use anyhow::Result;
use derive_builder::Builder;

mod admini_boundary;
mod gdal;
mod load_queue;
mod mapping;
mod xslx_helpers;
mod zip_traversal;

#[derive(Builder)]
pub struct Loader {
    datasets: Vec<Dataset>,
    postgres_url: String,
    skip_if_exists: bool,
}

impl Loader {
    pub async fn load_all(self) -> Result<()> {
        let mut load_queue = load_queue::LoadQueue::new(&self).await?;
        for dataset in self.datasets {
            load_queue.push(&dataset).await?;
        }
        load_queue.close().await?;
        admini_boundary::load_admini_boundary(&self.postgres_url).await?;
        Ok(())
    }
}
