#![warn(unused_extern_crates)]

use anyhow::{Context, Result};
use std::path::PathBuf;

mod cli;
mod context;
mod downloader;
mod loader;
mod metadata;
mod scraper;

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::main();
    loader::check_gdal_tools()
        .await
        .context("while checking GDAL tools")?;
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

    let output = parse_output_target(&args.output_format, &args.output_destination)
        .context("while parsing output settings")?;

    let loader = loader::LoaderBuilder::default()
        .datasets(datasets)
        .output(output)
        .skip_if_exists(args.skip_if_exists)
        .build()
        .context("while building loader")?;
    loader
        .load_all()
        .await
        .with_context(|| "while loading datasets")?;

    Ok(())
}

fn parse_output_target(format: &str, destination: &str) -> Result<loader::OutputTarget> {
    let normalized = normalize_format(format);
    if is_postgres_format(&normalized) {
        return Ok(loader::OutputTarget::Postgres {
            postgres_url: destination.to_string(),
        });
    }

    let extension = file_extension_for_format(&normalized);
    Ok(loader::OutputTarget::File {
        output_dir: PathBuf::from(destination),
        gdal_driver: format.to_string(),
        file_extension: extension,
    })
}

fn normalize_format(format: &str) -> String {
    format.trim().to_ascii_lowercase()
}

fn is_postgres_format(normalized: &str) -> bool {
    matches!(
        normalized,
        "postgres" | "postgresql" | "postgis" | "pg"
    )
}

fn file_extension_for_format(normalized: &str) -> String {
    match normalized {
        "geoparquet" | "parquet" => "parquet".to_string(),
        "geojson" | "geojsonseq" => "geojson".to_string(),
        "flatgeobuf" => "fgb".to_string(),
        _ => normalize_extension(normalized),
    }
}

fn normalize_extension(input: &str) -> String {
    let mut out = String::new();
    let mut prev_underscore = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_underscore = false;
        } else if !prev_underscore {
            out.push('_');
            prev_underscore = true;
        }
    }
    while out.starts_with('_') {
        out.remove(0);
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        "gdal".to_string()
    } else {
        out
    }
}
