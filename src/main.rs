#![warn(unused_extern_crates)]

use std::path::PathBuf;

use anyhow::{Context, Ok, Result};

mod cli;
mod downloader;
mod loader;
mod scraper;

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::main();
    let tmp = args.tmp_dir.unwrap_or_else(|| PathBuf::from("./tmp"));
    tokio::fs::create_dir_all(&tmp).await?;
    // println!("Postgres URL: {}", args.postgres_url);

    // Download all files first
    let datasets = scraper::download_all(&tmp, args.skip_download)
        .await
        .with_context(|| format!("while downloading initial data"))?;

    loader::load_all(&tmp, &datasets, &args.postgres_url, args.skip_sql_if_exists)
        .await
        .with_context(|| "while loading datasets")?;

    Ok(())
}
