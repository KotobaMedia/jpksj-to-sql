#![warn(unused_extern_crates)]

use anyhow::{Context, Result};

mod cli;
mod context;
mod downloader;
mod loader;
mod metadata;
mod scraper;

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::main();
    if let Some(tmp) = args.tmp_dir {
        context::set_tmp(tmp);
    }
    tokio::fs::create_dir_all(context::tmp()).await?;

    // Download all files first
    let scraper = scraper::ScraperBuilder::default()
        .skip_dl(args.skip_download)
        .filter_identifiers(args.filter_identifiers.clone())
        .year(args.year)
        .build()
        .context("while building scraper")?;
    let datasets = scraper
        .download_all()
        .await
        .with_context(|| format!("while downloading initial data"))?;

    let loader = loader::LoaderBuilder::default()
        .datasets(datasets)
        .postgres_url(args.postgres_url.clone())
        .skip_if_exists(args.skip_sql_if_exists)
        .build()
        .context("while building loader")?;
    loader
        .load_all()
        .await
        .with_context(|| "while loading datasets")?;

    Ok(())
}
