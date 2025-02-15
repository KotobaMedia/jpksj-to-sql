use anyhow::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::context;

#[derive(Serialize, Deserialize)]
struct Metadata {
    last_modified: Option<String>,
    etag: Option<String>,
}

pub struct DownloadedFile {
    pub path: PathBuf,
}

pub fn path_for_url(url: &Url) -> (PathBuf, PathBuf) {
    let tmp = context::tmp();
    let filename = url
        .path_segments()
        .and_then(|segments| segments.last())
        .unwrap_or("file");
    (
        tmp.join(filename),
        tmp.join(format!("{}.meta.json", filename)),
    )
}

pub async fn download_to_tmp(url: &Url) -> Result<DownloadedFile> {
    let (file_path, meta_path) = path_for_url(&url);

    // Try to read existing metadata if it exists.
    let metadata: Option<Metadata> = if let Ok(meta_content) = fs::read_to_string(&meta_path).await
    {
        serde_json::from_str(&meta_content).ok()
    } else {
        None
    };

    let client = reqwest::Client::new();
    let mut request = client.get(url.clone());

    // Add conditional headers if metadata is available.
    if let Some(meta) = &metadata {
        if let Some(etag) = &meta.etag {
            request = request.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = &meta.last_modified {
            request = request.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
        }
    }

    let response = request.send().await?;

    // If the server indicates the file has not changed, return the existing file.
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        // What if the file is missing even though we have metadata?
        if !file_path.exists() {
            return Err(anyhow::anyhow!(
                "Server returned 304 Not Modified, but file is missing"
            ));
        }
        return Ok(DownloadedFile { path: file_path });
    }

    // Ensure the response is successful (will error on 4xx or 5xx responses).
    let response = response.error_for_status()?;

    // Create (or overwrite) the target file.
    let mut file = File::create(&file_path).await?;

    // Extract metadata from response headers.
    let last_modified = response
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let new_metadata = Metadata {
        last_modified,
        etag,
    };

    // Stream the response body and write it chunk by chunk.
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;

    // Serialize and write the metadata to a {filename}.meta.json file.
    let meta_json = serde_json::to_string_pretty(&new_metadata)?;
    fs::write(&meta_path, meta_json).await?;
    // Note that this is set after the file is completely written. That way, if the process crashed or was interrupted, we won't have a partial file.

    Ok(DownloadedFile { path: file_path })
}
