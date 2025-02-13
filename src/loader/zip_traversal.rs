// the module responsible for opening ZIP files and traversing them.
// sometimes, zip files are inside zip files, so when a zip file is encountered, we have to recursively traverse it.
// only extracts shapefiles, to a temporary directory, so ogr2ogr can load them directly to the database.

use super::mapping::ShapefileMetadata;
use anyhow::Result;
use regex::Regex;
use std::{fs::File, path::PathBuf};
use zip::ZipArchive;

fn extract_zip(tmp: &PathBuf, zip_path: &PathBuf, matchers: &Vec<Regex>) -> Result<Vec<PathBuf>> {
    let mut out = vec![];
    let file = File::open(zip_path)?;
    let zip_filename = zip_path.file_name().unwrap().to_str().unwrap();
    let tmp = tmp.join(zip_filename).with_extension("");
    std::fs::create_dir_all(&tmp)?;
    let mut zip = ZipArchive::new(file)?;
    for i in 0..zip.len() {
        let mut file = zip.by_index(i)?;
        let file_name = file.name().to_string();
        if file_name.ends_with(".zip") {
            let tmp_zip = tmp.join(file_name);
            std::io::copy(&mut file, &mut File::create(&tmp_zip)?)?;
            out.extend(extract_zip(&tmp_zip, &tmp, &matchers)?);
        } else if matchers.iter().any(|r| r.is_match(&file_name)) {
            let tmp_shp = tmp.join(file_name);
            std::io::copy(&mut file, &mut File::create(&tmp_shp)?)?;
            out.push(tmp_shp);
        }
    }
    Ok(out)
}

pub async fn matching_shapefiles_in_zip(
    tmp: &PathBuf,
    zip_path: &PathBuf,
    mapping: &ShapefileMetadata,
) -> Result<Vec<PathBuf>> {
    let shp_tmp = tmp.join("shp");
    tokio::fs::create_dir_all(&shp_tmp).await?;
    let matchers = mapping.shapefile_name_regex.clone();
    let zip_path = zip_path.clone();

    let shapefile_paths =
        tokio::task::spawn_blocking(move || extract_zip(&shp_tmp, &zip_path, &matchers)).await??;

    Ok(shapefile_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_matching_shapefiles_in_zip() {
        let tmp = PathBuf::from("./tmp");
        let zip = PathBuf::from("./test_data/zip/A30a5-11_4939-jgd_GML.zip");
        let mapping = ShapefileMetadata {
            cat1: "cat1".to_string(),
            cat2: "cat2".to_string(),
            name: "name".to_string(),
            version: "version".to_string(),
            data_year: "data_year".to_string(),
            shapefile_matcher: vec!["A30a5-YY_mmmm_SedimentDisasterAndSnowslide.shp".to_string()],
            field_mappings: vec![],
            identifier: "identifier".to_string(),
            shapefile_name_regex: vec![Regex::new(
                r"A30a5-\d{2}_\d{4}_SedimentDisasterAndSnowslide(?i:(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx))$",
            )
            .unwrap()],
        };
        let result = matching_shapefiles_in_zip(&tmp, &zip, &mapping).await;
        assert!(result.is_ok());
        let data = result.unwrap();
    }
}
