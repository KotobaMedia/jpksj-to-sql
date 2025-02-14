use crate::loader::gdal;
use crate::loader::{mapping, zip_traversal};
use crate::scraper::Dataset;
use anyhow::Result;
use async_channel::unbounded;
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::max;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task;

async fn load(
    tmp: &PathBuf,
    dataset: &Dataset,
    postgres_url: &str,
    skip_if_exists: bool,
) -> Result<()> {
    let vrt_tmp = tmp.join("vrt");
    tokio::fs::create_dir_all(&vrt_tmp).await?;

    // first, let's get the entry for this dataset from the mapping file
    let mapping = mapping::find_mapping_def_for_entry(&tmp, &dataset.page.identifier)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("No mapping found for dataset: {:?}", dataset.initial_item)
        })?;

    println!(
        "Loading dataset: {} - {} - {} as {}",
        mapping.cat1, mapping.cat2, mapping.name, mapping.identifier
    );

    let mut shapefiles: Vec<PathBuf> = Vec::new();
    for zip_file_path in &dataset.zip_file_paths {
        let shapefiles_in_zip =
            zip_traversal::matching_shapefiles_in_zip(tmp, zip_file_path, &mapping).await?;
        shapefiles.extend(shapefiles_in_zip);
    }

    if skip_if_exists && gdal::has_layer(postgres_url, &mapping.identifier).await? {
        println!("Table already exists, skipping");
        return Ok(());
    }

    let vrt_path = vrt_tmp
        .join(dataset.page.identifier.clone())
        .with_extension("vrt");
    gdal::create_vrt(&vrt_path, &shapefiles, &mapping).await?;
    gdal::load_to_postgres(&vrt_path, postgres_url).await?;

    Ok(())
}

struct PBStatusUpdateMsg {
    added: u64,
    finished: u64,
}

pub struct LoadQueue {
    pb_status_sender: Option<async_channel::Sender<PBStatusUpdateMsg>>,
    sender: Option<async_channel::Sender<Dataset>>,

    set: Option<task::JoinSet<()>>,
}

impl LoadQueue {
    pub fn new(tmp: &PathBuf, postgres_url: &str, skip_if_exists: bool) -> Self {
        let (pb_status_sender, pb_status_receiver) = unbounded::<PBStatusUpdateMsg>();
        let (sender, receiver) = unbounded::<Dataset>();
        let mut set = task::JoinSet::new();
        let size = max(num_cpus::get_physical() / 2, 1);
        for _i in 0..size {
            let receiver = receiver.clone();
            let pb_sender = pb_status_sender.clone();
            let postgres_url = postgres_url.to_string();
            let tmp = tmp.clone();
            set.spawn(async move {
                while let Ok(item) = receiver.recv().await {
                    // println!("processor {} loading", _i);
                    load(&tmp, &item, &postgres_url, skip_if_exists)
                        .await
                        .unwrap();
                    pb_sender
                        .send(PBStatusUpdateMsg {
                            added: 0,
                            finished: 1,
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
                    "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}",
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
            }
            pb.finish_with_message("ダウンロードが終了しました。");
        });
        Self {
            pb_status_sender: Some(pb_status_sender),
            sender: Some(sender),
            set: Some(set),
        }
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
