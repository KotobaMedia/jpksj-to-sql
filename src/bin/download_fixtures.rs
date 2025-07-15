use anyhow::{anyhow, Result};
use scraper::{Html, Selector};
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
    match DataPage::scrape(&item.url, &[]).await {
        Ok(data_page) => {
            for selection in data_page.yearly_versions {
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

fn url_to_filepath(base_dir: &Path, url: &Url) -> Result<std::path::PathBuf> {
    let path = url.path();

    // Remove leading slash and create path components
    let path_components: Vec<&str> = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    if path_components.is_empty() {
        return Ok(base_dir.join("index.html"));
    }

    let mut filepath = base_dir.to_path_buf();
    for component in path_components {
        filepath.push(component);
    }

    // If the path doesn't end with an extension, assume it's HTML
    if let Some(extension) = filepath.extension() {
        if extension.to_string_lossy().is_empty() {
            filepath.set_extension("html");
        }
    } else {
        filepath.set_extension("html");
    }

    Ok(filepath)
}

fn extract_links_from_html(html_content: &str, base_url: &Url) -> Result<Vec<Url>> {
    let document = Html::parse_document(html_content);
    let selector = Selector::parse("a[href]").unwrap();

    let mut links = Vec::new();

    for element in document.select(&selector) {
        if let Some(href) = element.value().attr("href") {
            match base_url.join(href) {
                Ok(url) => {
                    // Only include links to the same host and containing "/codelist/"
                    if url.host_str() == base_url.host_str() && url.path().contains("/codelist/") {
                        links.push(url);
                    }
                }
                Err(_) => {
                    // Skip invalid URLs
                    continue;
                }
            }
        }
    }

    Ok(links)
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
    let fixtures_dir = Path::new("test_data");
    fs::create_dir_all(fixtures_dir)?;

    println!("Downloading HTML files to {:?}...", fixtures_dir);

    let mut downloaded_count = 0;
    let mut error_count = 0;
    let mut total_urls = 0;
    let mut processed_urls = std::collections::HashSet::new();

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

            let filepath = match url_to_filepath(fixtures_dir, url) {
                Ok(path) => path,
                Err(e) => {
                    println!("  ✗ Error creating filepath: {}", e);
                    error_count += 1;
                    continue;
                }
            };

            // Check if file already exists
            if filepath.exists() {
                println!(
                    "    [{}/{}] File exists {}, checking for missing links...",
                    url_index + 1,
                    all_urls.len(),
                    filepath.display()
                );

                // Read existing file and extract links
                match fs::read_to_string(&filepath) {
                    Ok(html_content) => {
                        match download_missing_links_from_html(
                            &html_content,
                            url,
                            fixtures_dir,
                            &mut processed_urls,
                        )
                        .await
                        {
                            Ok(linked_count) => {
                                if linked_count > 0 {
                                    println!(
                                        "      Downloaded {} missing linked files",
                                        linked_count
                                    );
                                } else {
                                    println!("      All linked files already exist");
                                }
                            }
                            Err(e) => {
                                println!("      ✗ Error processing links: {}", e);
                                error_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        println!("      ✗ Error reading existing file: {}", e);
                        error_count += 1;
                    }
                }
                continue;
            }

            print!(
                "    [{}/{}] Downloading {}... ",
                url_index + 1,
                all_urls.len(),
                filepath.display()
            );

            match download_html_and_links(url, &filepath, fixtures_dir, &mut processed_urls).await {
                Ok((downloaded, linked_count)) => {
                    println!("✓ ({} linked files)", linked_count);
                    downloaded_count += downloaded;
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

async fn download_html_and_links(
    url: &Url,
    filepath: &Path,
    base_dir: &Path,
    processed_urls: &mut std::collections::HashSet<String>,
) -> Result<(usize, usize)> {
    let response = reqwest::get(url.as_str()).await?;

    if !response.status().is_success() {
        return Err(anyhow!("HTTP error: {}", response.status()));
    }

    let html_content = response.text().await?;

    // if the HTML has `アクセスの増加を検知しました` in the body, we'll error here
    if html_content.contains("アクセスの増加を検知しました") {
        return Err(anyhow!("アクセスの増加を検知しました"));
    }

    // Create parent directory if it doesn't exist
    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(parent)?;
    }

    // Write the main HTML file
    let mut file = File::create(filepath).await?;
    file.write_all(html_content.as_bytes()).await?;

    // Extract and download linked files
    let links = extract_links_from_html(&html_content, url)?;
    let mut linked_count = 0;

    for link_url in links {
        let link_key = link_url.to_string();

        // Skip if we've already processed this URL
        if processed_urls.contains(&link_key) {
            continue;
        }

        processed_urls.insert(link_key);

        let link_filepath = match url_to_filepath(base_dir, &link_url) {
            Ok(path) => path,
            Err(_) => continue, // Skip invalid paths
        };

        // Skip if the linked file already exists
        if link_filepath.exists() {
            continue;
        }

        // Create parent directory for linked file
        if let Some(parent) = link_filepath.parent() {
            if let Err(_) = fs::create_dir_all(parent) {
                continue; // Skip if we can't create directory
            }
        }

        // Download the linked file
        match download_html(&link_url, &link_filepath).await {
            Ok(_) => {
                linked_count += 1;
            }
            Err(_) => {
                // Continue with other links even if one fails
                continue;
            }
        }

        // Small delay between linked file downloads
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    Ok((1, linked_count)) // 1 for main file + linked_count for linked files
}

async fn download_html(url: &url::Url, filepath: &std::path::Path) -> Result<()> {
    let response = reqwest::get(url.as_str()).await?;

    if !response.status().is_success() {
        return Err(anyhow!("HTTP error: {}", response.status()));
    }

    let html_content = response.text().await?;

    // if the HTML has `アクセスの増加を検知しました` in the body, we'll error here
    if html_content.contains("アクセスの増加を検知しました") {
        return Err(anyhow!("アクセスの増加を検知しました"));
    }

    let mut file = File::create(filepath).await?;
    file.write_all(html_content.as_bytes()).await?;

    Ok(())
}

async fn download_missing_links_from_html(
    html_content: &str,
    url: &Url,
    base_dir: &Path,
    processed_urls: &mut std::collections::HashSet<String>,
) -> Result<usize> {
    let links = extract_links_from_html(html_content, url)?;
    let mut linked_count = 0;

    for link_url in links {
        let link_key = link_url.to_string();

        // Skip if we've already processed this URL
        if processed_urls.contains(&link_key) {
            continue;
        }

        processed_urls.insert(link_key);

        let link_filepath = match url_to_filepath(base_dir, &link_url) {
            Ok(path) => path,
            Err(_) => continue, // Skip invalid paths
        };

        // Skip if the linked file already exists
        if link_filepath.exists() {
            continue;
        }

        // Create parent directory for linked file
        if let Some(parent) = link_filepath.parent() {
            if let Err(_) = fs::create_dir_all(parent) {
                continue; // Skip if we can't create directory
            }
        }

        // Download the linked file
        match download_html(&link_url, &link_filepath).await {
            Ok(_) => {
                linked_count += 1;
            }
            Err(_) => {
                // Continue with other links even if one fails
                continue;
            }
        }

        // Small delay between linked file downloads
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    Ok(linked_count)
}
