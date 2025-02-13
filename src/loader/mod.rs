// The loader module is responsible for loading data from ZIP files and in to the database.

use crate::scraper::Dataset;
use anyhow::Result;
use std::path::PathBuf;

mod gdal;
mod mapping;
mod zip_traversal;

pub async fn load(tmp: &PathBuf, dataset: &Dataset, postgres_url: &str) -> Result<()> {
    // first, let's get the entry for this dataset from the mapping file
    let mapping = mapping::find_mapping_def_for_entry(
        &tmp,
        &dataset.initial_item.category1_name,
        &dataset.initial_item.category2_name,
        &dataset.initial_item.name,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("No mapping found for dataset"))?;

    println!(
        "Loading dataset: {} - {} - {}",
        mapping.cat1, mapping.cat2, mapping.name
    );

    let shapefiles =
        zip_traversal::matching_shapefiles_in_zip(&tmp, &dataset.zip_file_path, &mapping).await?;

    for shapefile in shapefiles {
        let vrt_path = gdal::create_vrt(&shapefile, &mapping).await?;
        gdal::load_to_postgres(&vrt_path, postgres_url).await?;
    }

    Ok(())
}
