use crate::scraper::downloader;
use anyhow::Result;
use async_channel::unbounded;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::fmt::Write;
use std::path::PathBuf;
use tokio::task;

use super::data_page::DataItem;

const DL_QUEUE_SIZE: usize = 10;

struct PBStatusUpdateMsg {
    added: u64,
    finished: u64,
}

pub struct DownloadQueue {
    pb_status_sender: Option<async_channel::Sender<PBStatusUpdateMsg>>,
    sender: Option<async_channel::Sender<DataItem>>,

    set: Option<task::JoinSet<()>>,
}

impl DownloadQueue {
    pub fn new(tmp: PathBuf) -> Self {
        let (pb_status_sender, pb_status_receiver) = unbounded::<PBStatusUpdateMsg>();
        let (sender, receiver) = unbounded::<DataItem>();
        let mut set = task::JoinSet::new();
        for _ in 0..DL_QUEUE_SIZE {
            let receiver = receiver.clone();
            let pb_sender = pb_status_sender.clone();
            let tmp = tmp.clone();
            set.spawn(async move {
                while let Ok(item) = receiver.recv().await {
                    // println!("Downloading: {}", url);
                    let url = item.file_url;
                    downloader::download_to_tmp(tmp.clone(), url).await.unwrap();
                    pb_sender
                        .send(PBStatusUpdateMsg {
                            added: 0,
                            finished: item.bytes,
                        })
                        .await
                        .unwrap();
                }
            });
        }

        set.spawn(async move {
            let pb = ProgressBar::new(0);
            pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
                .progress_chars("#>-"));
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

    pub async fn push(&self, item: DataItem) -> Result<()> {
        let Some(sender) = &self.sender else {
            return Err(anyhow::anyhow!("DownloadQueue is already closed"));
        };
        let Some(pb_status_sender) = &self.pb_status_sender else {
            return Err(anyhow::anyhow!("DownloadQueue is already closed"));
        };
        pb_status_sender
            .send(PBStatusUpdateMsg {
                added: item.bytes,
                finished: 0,
            })
            .await?;
        sender.send(item).await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        let Some(_) = self.sender.take() else {
            return Err(anyhow::anyhow!("DownloadQueue is already closed"));
        };
        let Some(set) = self.set.take() else {
            return Err(anyhow::anyhow!("DownloadQueue is already closed"));
        };
        let Some(_) = self.pb_status_sender.take() else {
            return Err(anyhow::anyhow!("DownloadQueue is already closed"));
        };
        // let Some(status) = self.status.take() else {
        //     return Err(anyhow::anyhow!("DownloadQueue is already closed"));
        // };
        set.join_all().await;
        Ok(())
    }
}
