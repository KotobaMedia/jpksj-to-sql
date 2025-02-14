// The loader module is responsible for loading data from ZIP files and in to the database.

use crate::scraper::Dataset;
use anyhow::Result;
use std::path::PathBuf;

mod gdal;
mod load_queue;
mod mapping;
mod zip_traversal;

pub async fn load_all(
    tmp: &PathBuf,
    datasets: &Vec<Dataset>,
    postgres_url: &str,
    skip_if_exists: bool,
) -> Result<()> {
    let mut load_queue = load_queue::LoadQueue::new(tmp, postgres_url, skip_if_exists);
    for dataset in datasets {
        load_queue.push(dataset).await?;
    }
    load_queue.close().await?;
    Ok(())
}
