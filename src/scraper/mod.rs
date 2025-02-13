use std::path::PathBuf;

use anyhow::Result;

mod data_page;
mod download_queue;
mod downloader;
mod initial;

pub async fn download_all(tmp: PathBuf) -> Result<()> {
    let mut dl_queue = download_queue::DownloadQueue::new(tmp.clone());
    let initial = initial::scrape().await?;
    let data_items = initial.data;
    for item in data_items {
        let page = data_page::scrape(item.url).await?;
        let items = data_page::filter_data_items(page.items);
        for item in items {
            dl_queue.push(item).await?;
        }
    }
    dl_queue.close().await?;
    Ok(())
}
