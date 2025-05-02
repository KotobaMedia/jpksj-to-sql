// the module responsible for opening ZIP files and traversing them.
// sometimes, zip files are inside zip files, so when a zip file is encountered, we have to recursively traverse it.
// only extracts shapefiles, to a temporary directory, so ogr2ogr can load them directly to the database.

use super::mapping::ShapefileMetadata;
use anyhow::{Context, Result};
use regex::Regex;
use std::{fs::File, path::PathBuf};
use zip::ZipArchive;

fn extract_zip(
    outdir: &PathBuf,
    zip_path: &PathBuf,
    matchers: &Vec<Regex>,
) -> Result<Vec<PathBuf>> {
    let mut out = vec![];
    let file = File::open(zip_path)?;
    let zip_filename = zip_path.file_name().unwrap().to_str().unwrap();
    let outdir = outdir.join(zip_filename).with_extension("");
    let mut zip = ZipArchive::new(file)?;
    // println!("Matchers: {:?}", matchers);
    for i in 0..zip.len() {
        let mut file = zip.by_index(i)?;
        // replace Windows backslashes with forward slashes
        let file_name = file.name().to_string().replace("\\", "/");
        let dest_path = outdir.join(&file_name);
        let basedir = dest_path.parent().unwrap();

        // println!("Extracting: {}", file_name);
        if file_name.ends_with(".zip") {
            std::fs::create_dir_all(&basedir)?;
            std::io::copy(&mut file, &mut File::create(&dest_path)?)?;
            out.extend(
                extract_zip(&outdir, &dest_path, &matchers)
                    .with_context(|| format!("when extracting nested {}", dest_path.display()))?,
            );
        } else if matchers.iter().any(|r| r.is_match(&file_name)) {
            if file_name.starts_with("N08-21_GML/utf8/") {
                // skip this file, it's a duplicate and contains malformed UTF8
                continue;
            }
            std::fs::create_dir_all(&basedir)?;
            std::io::copy(&mut file, &mut File::create(&dest_path)?)?;
            out.push(dest_path);
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

    let mut all_paths = {
        let shp_tmp = shp_tmp.clone();
        let zip_path = zip_path.clone();
        if mapping.identifier == "A33" {
            // A33 shapefiles don't match the regex, so we'll skip this part and
            // fall back to the expanded matchers, focusing on Polygon files only
            let expanded_matchers = vec![Regex::new(
                r"Po?lygon(?i:(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx))$",
            )?];

            tokio::task::spawn_blocking(move || {
                extract_zip(&shp_tmp, &zip_path, &expanded_matchers)
                    .with_context(|| format!("when extracting {}", zip_path.display()))
            })
            .await??
        } else {
            tokio::task::spawn_blocking(move || {
                extract_zip(&shp_tmp, &zip_path, &matchers)
                    .with_context(|| format!("when extracting {}", zip_path.display()))
            })
            .await??
        }
    };

    if all_paths.is_empty() {
        println!("No shapefiles found in zip file, expanding matchers...");
        // since we didn't get any shapefiles this time, let's expand the matchers to see if we can find any
        let expanded_matchers = vec![Regex::new(
            r"(?i:(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx))$",
        )?];

        all_paths = tokio::task::spawn_blocking(move || {
            extract_zip(&shp_tmp, &zip_path, &expanded_matchers)
                .with_context(|| format!("when extracting {}", zip_path.display()))
        })
        .await??;
    }

    // at this point, we have decompressed all shapefiles (and accompanying files)
    // however, we only need the `.shp` files for passing to ogr2ogr
    let shapefile_paths = all_paths
        .iter()
        .filter(|p| p.extension().unwrap() == "shp")
        .cloned()
        .collect::<Vec<_>>();

    // println!(
    //     "Found {} shapefiles: \n{}",
    //     shapefile_paths.len(),
    //     shapefile_paths
    //         .iter()
    //         .map(|s| format!("- {}", s.display()))
    //         .collect::<Vec<_>>()
    //         .join("\n")
    // );

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
            original_identifier: "original_identifier".to_string(),
            identifier: "identifier".to_string(),
            shapefile_name_regex: vec![Regex::new(
                r"A30a5-\d{2}_\d{4}_SedimentDisasterAndSnowslide(?i:(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx))$",
            )
            .unwrap()],
        };
        let result = matching_shapefiles_in_zip(&tmp, &zip, &mapping).await;
        assert!(result.is_ok());
        let _ = result.unwrap();
    }

    #[tokio::test]
    async fn test_matching_shapefiles_in_zip_subdir() {
        let tmp = PathBuf::from("./tmp");
        let zip = PathBuf::from("./test_data/zip/P23-12_38_GML.zip");
        let mapping = ShapefileMetadata {
            cat1: "cat1".to_string(),
            cat2: "cat2".to_string(),
            name: "name".to_string(),
            version: "version".to_string(),
            data_year: "data_year".to_string(),
            shapefile_matcher: vec!["P23a-YY_PP.shp".to_string()],
            field_mappings: vec![],
            original_identifier: "original_identifier".to_string(),
            identifier: "identifier".to_string(),
            shapefile_name_regex: vec![Regex::new(
                r"(?:^|/)P23a-\d{2}_\d{2}(?i:(?:\.shp|\.cpg|\.dbf|\.prj|\.qmd|\.shx))$",
            )
            .unwrap()],
        };
        let result = matching_shapefiles_in_zip(&tmp, &zip, &mapping).await;
        assert!(result.is_ok());
        let _ = result.unwrap();
    }
}
