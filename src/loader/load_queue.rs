use crate::context;
use crate::loader::gdal;
use crate::loader::{mapping, zip_traversal};
use crate::metadata::MetadataConnection;
use crate::scraper::Dataset;
use anyhow::Result;
use async_channel::unbounded;
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::max;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task;

use super::Loader;

async fn load(
    dataset: &Dataset,
    postgres_url: &str,
    skip_if_exists: bool,
    metadata_conn: &MetadataConnection,
) -> Result<()> {
    let tmp = context::tmp();
    let vrt_tmp = tmp.join("vrt");
    tokio::fs::create_dir_all(&vrt_tmp).await?;

    let identifier = &dataset.initial_item.identifier;

    // first, let's get the entries for this dataset from the mapping file
    let mappings = mapping::find_mapping_def_for_entry(&identifier).await?;

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
                zip_traversal::matching_shapefiles_in_zip(tmp, zip_file_path, &mapping).await?;
            shapefiles.extend(shapefiles_in_zip);
        }

        println!("Found {} shapefiles.", shapefiles.len());

        let has_layer = gdal::has_layer(postgres_url, &mapping.identifier).await?;
        if skip_if_exists && has_layer {
            println!("Table already exists for {}, skipping", mapping.identifier);
        } else {
            let vrt_path = vrt_tmp.join(&identifier).with_extension("vrt");
            gdal::create_vrt(&vrt_path, &shapefiles, &mapping).await?;
            gdal::load_to_postgres(&vrt_path, postgres_url).await?;
        }

        let metadata = metadata_conn
            .build_metadata_from_dataset(&identifier, &mapping, dataset)
            .await?;
        // println!("Metadata: {:?}", metadata);
        metadata_conn.create_dataset(&identifier, &metadata).await?;
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
            postgres_url,
            skip_if_exists,
            ..
        } = loader;

        let metadata_conn = MetadataConnection::new(postgres_url).await?;

        let (pb_status_sender, pb_status_receiver) = unbounded::<PBStatusUpdateMsg>();
        let (sender, receiver) = unbounded::<Dataset>();
        let mut set = task::JoinSet::new();
        let size = max(num_cpus::get() - 1, 1);
        for _i in 0..size {
            let receiver = receiver.clone();
            let pb_sender = pb_status_sender.clone();
            let postgres_url = postgres_url.to_string();
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
                    let result = load(&item, &postgres_url, skip_if_exists, &metadata_conn).await;
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
            println!("DB取り組みが終了しました。");
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
