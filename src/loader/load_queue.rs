use crate::context;
use crate::loader::gdal;
use crate::loader::{mapping, zip_traversal, OutputTarget};
use crate::metadata::{self, ColumnSchema, MetadataConnection};
use crate::scraper::Dataset;
use anyhow::{Context, Result};
use async_channel::unbounded;
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::max;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task;

use super::Loader;

async fn load(
    dataset: &Dataset,
    output: &OutputTarget,
    skip_if_exists: bool,
    metadata_conn: Option<&MetadataConnection>,
) -> Result<()> {
    let tmp = context::tmp();
    let vrt_tmp = tmp.join("vrt");
    tokio::fs::create_dir_all(&vrt_tmp)
        .await
        .with_context(|| format!("when creating tempdir for vrt: {}", &vrt_tmp.display()))?;

    let identifier = &dataset.initial_item.identifier;

    // first, let's get the entries for this dataset from the API metadata
    let mappings = mapping::mapping_defs_for_dataset(dataset)
        .await
        .with_context(|| {
            format!(
                "when finding mapping definitions for entry: {}",
                &identifier
            )
        })?;

    for mapping in mappings {
        // overwrite the identifier with the one from the mapping file
        let identifier = mapping.identifier.clone().to_lowercase();
        // println!(
        //     "Loading dataset: {} - {} - {} as {}",
        //     mapping.cat1, mapping.cat2, mapping.name, mapping.identifier
        // );

        let mut shapefiles: Vec<PathBuf> = Vec::new();
        for zip_file_path in &dataset.zip_file_paths {
            let shapefiles_in_zip =
                zip_traversal::matching_shapefiles_in_zip(tmp, zip_file_path, &mapping)
                    .await
                    .with_context(|| {
                        format!(
                            "when looking for matching shapefiles in zip: {}",
                            &zip_file_path.display()
                        )
                    })?;
            shapefiles.extend(shapefiles_in_zip);
        }

        println!("Found {} shapefiles.", shapefiles.len());

        let output_path = output.output_path(&identifier);
        let already_exists = if skip_if_exists {
            match output {
                OutputTarget::Postgres { postgres_url } => {
                    gdal::has_layer(postgres_url, &mapping.identifier)
                        .await
                        .with_context(|| format!("when asking gdal for layer"))?
                }
                OutputTarget::File { .. } => match output_path.as_ref() {
                    Some(path) => path.exists(),
                    None => false,
                },
            }
        } else {
            false
        };

        let needs_load = !(skip_if_exists && already_exists);
        let needs_vrt = needs_load
            || (matches!(output, OutputTarget::File { .. }) && !shapefiles.is_empty());
        let mut vrt_path = None;
        if needs_vrt {
            let path = vrt_tmp.join(&identifier).with_extension("vrt");
            gdal::create_vrt(&path, &shapefiles, &mapping)
                .await
                .context("when creating VRT")?;
            vrt_path = Some(path);
        }

        if skip_if_exists && already_exists {
            match output {
                OutputTarget::Postgres { .. } => {
                    println!("Table already exists for {}, skipping", mapping.identifier);
                }
                _ => {
                    if let Some(path) = output_path.as_ref() {
                        println!("Output already exists at {}, skipping", path.display());
                    } else {
                        println!("Output already exists for {}, skipping", mapping.identifier);
                    }
                }
            }
        } else if needs_load {
            let vrt_path = vrt_path
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing VRT path for {}", identifier))?;
            match output {
                OutputTarget::Postgres { postgres_url } => {
                    gdal::load_to_postgres(&vrt_path, postgres_url)
                        .await
                        .context("when loading to Postgres")?;
                }
                OutputTarget::File { .. } => {
                    let output_path = output_path
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("missing output path for {}", identifier))?;
                    let driver = output
                        .gdal_driver()
                        .ok_or_else(|| anyhow::anyhow!("missing GDAL driver"))?;
                    gdal::load_to_file(&vrt_path, &output_path, driver)
                        .await
                        .with_context(|| {
                            format!(
                                "when gdal loading VRT {} to {}",
                                &vrt_path.display(),
                                &output_path.display()
                            )
                        })?;
                }
            }
        }

        if let Some(metadata_conn) = metadata_conn {
            let metadata = metadata_conn
                .build_metadata_from_dataset(&identifier, &mapping, dataset)
                .await
                .context("when building metadata from dataset")?;
            // println!("Metadata: {:?}", metadata);
            metadata_conn
                .create_dataset(&identifier, &metadata)
                .await
                .context("when creating dataset metadata")?;
        } else if let OutputTarget::File { .. } = output {
            let schema_source = if let Some(vrt_path) = vrt_path.as_ref() {
                vrt_path.as_path()
            } else {
                output_path
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing output path for {}", identifier))?
                    .as_path()
            };
            let schema = gdal::layer_schema(schema_source).await.with_context(|| {
                format!("when reading schema from {}", schema_source.display())
            })?;

            let mut columns = Vec::with_capacity(schema.fields.len() + 2);
            columns.push(ColumnSchema {
                name: "ogc_fid".to_string(),
                data_type: "int4".to_string(),
            });
            for field in schema.fields {
                columns.push(ColumnSchema {
                    name: field.name,
                    data_type: gdal::ogr_type_to_postgres(&field.ogr_type),
                });
            }
            if let Some(geom_type) = schema.geometry_type {
                let geom_type = gdal::promote_geometry_type(&geom_type);
                let srid = schema.geometry_srid.unwrap_or(-1);
                columns.push(ColumnSchema {
                    name: "geom".to_string(),
                    data_type: format!("geometry({}, {})", geom_type, srid),
                });
            }

            let metadata = metadata::build_metadata_from_columns(&mapping, dataset, columns);
            let output_path = output_path
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing output path for {}", identifier))?;
            let metadata_path = output_path.with_extension("metadata.json");
            let json = serde_json::to_string_pretty(&metadata)?;
            tokio::fs::write(&metadata_path, json).await?;
        }
    }
    Ok(())
}

struct PBStatusUpdateMsg {
    added: u64,
    finished: u64,
    msg: Option<String>,
}

pub struct LoadQueue {
    pb_status_sender: Option<async_channel::Sender<PBStatusUpdateMsg>>,
    sender: Option<async_channel::Sender<Dataset>>,

    set: Option<task::JoinSet<()>>,
}

impl LoadQueue {
    pub async fn new(loader: &Loader) -> Result<Self> {
        let Loader {
            output,
            skip_if_exists,
            ..
        } = loader;

        if let Some(output_dir) = output.output_dir() {
            tokio::fs::create_dir_all(output_dir).await?;
        }

        let metadata_conn = if let Some(postgres_url) = output.postgres_url() {
            Some(MetadataConnection::new(postgres_url).await?)
        } else {
            None
        };

        let (pb_status_sender, pb_status_receiver) = unbounded::<PBStatusUpdateMsg>();
        let (sender, receiver) = unbounded::<Dataset>();
        let mut set = task::JoinSet::new();
        let size = max(num_cpus::get() - 1, 1);
        for _i in 0..size {
            let receiver = receiver.clone();
            let pb_sender = pb_status_sender.clone();
            let output = output.clone();
            let skip_if_exists = *skip_if_exists;
            let metadata_conn = metadata_conn.clone();
            set.spawn(async move {
                while let Ok(item) = receiver.recv().await {
                    // println!("processor {} loading", _i);
                    pb_sender
                        .send(PBStatusUpdateMsg {
                            added: 0,
                            finished: 0,
                            msg: Some(item.initial_item.identifier.clone()),
                        })
                        .await
                        .unwrap();
                    let result = load(&item, &output, skip_if_exists, metadata_conn.as_ref()).await;
                    if let Err(e) = result {
                        let identifier = item.initial_item.identifier.clone();
                        eprintln!(
                            "Error in loading dataset {}, skipping... {:?}",
                            identifier, e
                        );
                    }
                    pb_sender
                        .send(PBStatusUpdateMsg {
                            added: 0,
                            finished: 1,
                            msg: Some(item.initial_item.identifier.clone()),
                        })
                        .await
                        .unwrap();
                }
            });
        }

        set.spawn(async move {
            let pb = ProgressBar::new(0);
            pb.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{msg}] [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}",
                )
                .unwrap()
                .progress_chars("=>-"),
            );
            pb.enable_steady_tick(Duration::from_millis(300));
            let mut length = 0;
            let mut position = 0;
            while let Ok(msg) = pb_status_receiver.recv().await {
                length += msg.added;
                position += msg.finished;
                pb.set_length(length);
                pb.set_position(position);
                if let Some(msg) = msg.msg {
                    pb.set_message(msg);
                }
            }
            pb.finish();
            println!("取り込みが終了しました。");
        });

        Ok(Self {
            pb_status_sender: Some(pb_status_sender),
            sender: Some(sender),
            set: Some(set),
        })
    }

    pub async fn push(&self, item: &Dataset) -> Result<()> {
        let Some(sender) = &self.sender else {
            return Err(anyhow::anyhow!("LoadQueue is already closed"));
        };
        let Some(pb_status_sender) = &self.pb_status_sender else {
            return Err(anyhow::anyhow!("LoadQueue is already closed"));
        };
        pb_status_sender
            .send(PBStatusUpdateMsg {
                added: 1,
                finished: 0,
                msg: None,
            })
            .await?;
        sender.send(item.clone()).await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        let Some(_) = self.sender.take() else {
            return Err(anyhow::anyhow!("LoadQueue is already closed"));
        };
        let Some(set) = self.set.take() else {
            return Err(anyhow::anyhow!("LoadQueue is already closed"));
        };
        let Some(_) = self.pb_status_sender.take() else {
            return Err(anyhow::anyhow!("LoadQueue is already closed"));
        };
        set.join_all().await;
        Ok(())
    }
}
