use anyhow::{anyhow, Result};
use std::fs;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;

// Import the existing scraper modules
use jpksj_to_sql::scraper::data_page::DataPage;
use jpksj_to_sql::scraper::initial::{scrape, DataItem};

async fn get_all_data_urls(item: &DataItem) -> Result<Vec<Url>> {
    let mut urls = vec![item.url.clone()];

    // Use the existing DataPage scraper to find other years
    match DataPage::scrape(&item.url).await {
        Ok(data_page) => {
            for selection in data_page.data_selections {
                urls.push(selection.url);
            }
        }
        Err(e) => {
            eprintln!(
                "Warning: Could not scrape data page for {}: {}",
                item.name, e
            );
        }
    }

    Ok(urls)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Scraping data from KSJ website...");
    let scrape_result = scrape().await?;

    // Filter out non-commercial datasets
    let commercial_data: Vec<_> = scrape_result
        .data
        .iter()
        .filter(|item| item.usage != "非商用")
        .cloned()
        .collect();

    println!(
        "Found {} data items ({} commercial, {} non-commercial)",
        scrape_result.data.len(),
        commercial_data.len(),
        scrape_result.data.len() - commercial_data.len()
    );

    // Create the fixtures directory
    let fixtures_dir = Path::new("test_data/fixtures/ksj");
    fs::create_dir_all(fixtures_dir)?;

    println!("Downloading HTML files to {:?}...", fixtures_dir);

    let mut downloaded_count = 0;
    let mut error_count = 0;
    let mut total_urls = 0;

    for (index, item) in commercial_data.iter().enumerate() {
        println!(
            "[{}/{}] Processing {}...",
            index + 1,
            commercial_data.len(),
            item.name
        );

        let all_urls = match get_all_data_urls(item).await {
            Ok(urls) => urls,
            Err(e) => {
                println!("  ✗ Error getting URLs: {}", e);
                error_count += 1;
                continue;
            }
        };

        println!("  Found {} URLs for this data", all_urls.len());

        for (url_index, url) in all_urls.iter().enumerate() {
            total_urls += 1;

            // Extract basename from URL, removing query strings
            let url_path = url.path();
            let basename = url_path.split('/').last().unwrap_or("unknown");
            let filename = if basename.ends_with(".html") {
                basename.to_string()
            } else {
                format!("{}.html", basename)
            };

            let filepath = fixtures_dir.join(&filename);

            // Skip if file already exists
            if filepath.exists() {
                println!(
                    "    [{}/{}] Skipping {} (already exists)",
                    url_index + 1,
                    all_urls.len(),
                    filename
                );
                continue;
            }

            print!(
                "    [{}/{}] Downloading {}... ",
                url_index + 1,
                all_urls.len(),
                filename
            );

            match download_html(url, &filepath).await {
                Ok(_) => {
                    println!("✓");
                    downloaded_count += 1;
                }
                Err(e) => {
                    println!("✗ Error: {}", e);
                    error_count += 1;
                }
            }

            // Add a small delay to be respectful to the server
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    println!("\nDownload complete!");
    println!("Successfully downloaded: {} files", downloaded_count);
    println!("Errors: {} files", error_count);
    println!("Total URLs processed: {} files", total_urls);
    println!(
        "Total data items processed: {} files",
        commercial_data.len()
    );

    Ok(())
}

async fn download_html(url: &url::Url, filepath: &std::path::Path) -> Result<()> {
    let response = reqwest::get(url.as_str()).await?;

    if !response.status().is_success() {
        return Err(anyhow!("HTTP error: {}", response.status()));
    }

    let html_content = response.text().await?;

    let mut file = File::create(filepath).await?;
    file.write_all(html_content.as_bytes()).await?;

    Ok(())
}
