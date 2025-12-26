// The loader module is responsible for loading data from ZIP files and into the output destination.

use crate::scraper::Dataset;
use anyhow::Result;
use derive_builder::Builder;
use std::path::{Path, PathBuf};

mod admini_boundary;
mod gdal;
mod load_queue;
pub mod mapping;
mod xslx_helpers;
mod zip_traversal;

pub async fn check_gdal_tools() -> Result<()> {
    gdal::check_gdal_tools().await
}

#[derive(Builder)]
pub struct Loader {
    datasets: Vec<Dataset>,
    output: OutputTarget,
    skip_if_exists: bool,
}

impl Loader {
    pub async fn load_all(self) -> Result<()> {
        let mut load_queue = load_queue::LoadQueue::new(&self).await?;
        for dataset in self.datasets {
            load_queue.push(&dataset).await?;
        }
        load_queue.close().await?;
        if let OutputTarget::Postgres { postgres_url } = &self.output {
            admini_boundary::load_admini_boundary(postgres_url).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum OutputTarget {
    Postgres { postgres_url: String },
    File {
        output_dir: PathBuf,
        gdal_driver: String,
        file_extension: String,
    },
}

impl OutputTarget {
    pub fn postgres_url(&self) -> Option<&str> {
        match self {
            Self::Postgres { postgres_url } => Some(postgres_url.as_str()),
            _ => None,
        }
    }

    pub fn output_dir(&self) -> Option<&Path> {
        match self {
            Self::File { output_dir, .. } => Some(output_dir.as_path()),
            _ => None,
        }
    }

    pub fn gdal_driver(&self) -> Option<&str> {
        match self {
            Self::File { gdal_driver, .. } => Some(gdal_driver.as_str()),
            _ => None,
        }
    }

    pub fn file_extension(&self) -> Option<&str> {
        match self {
            Self::File {
                file_extension, ..
            } => Some(file_extension.as_str()),
            _ => None,
        }
    }

    pub fn output_path(&self, identifier: &str) -> Option<PathBuf> {
        let output_dir = self.output_dir()?;
        let extension = self.file_extension()?;
        Some(output_dir.join(identifier).with_extension(extension))
    }
}
