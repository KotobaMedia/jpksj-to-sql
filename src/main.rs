#![warn(unused_extern_crates)]

use std::path::PathBuf;

use anyhow::{Context, Result};

mod cli;
mod context;
mod downloader;
mod loader;
mod scraper;

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::main();
    let tmp = args.tmp_dir.unwrap_or_else(|| PathBuf::from("./tmp"));
    context::set_tmp(tmp);
    tokio::fs::create_dir_all(context::tmp()).await?;

    // Download all files first
    let datasets = scraper::download_all(args.skip_download)
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
