use std::fs;
use std::path::Path;
use std::sync::Once;

use mockito::{Matcher, Server};
use url::Url;

static INIT: Once = Once::new();

/// Sets up a mock server that serves HTML fixtures from the test_data/fixtures/ksj directory.
/// Returns the mock server and a function to get the base URL for the server.
///
/// The server will serve files based on their filename, so a file like `KsjTmplt-N03-2024.html`
/// will be available at `/KsjTmplt-N03-2024.html`.
pub async fn setup_mock_server() -> (mockito::ServerGuard, Box<dyn Fn() -> Url>) {
    INIT.call_once(|| {
        // Any one-time initialization can go here
    });

    let mut server = Server::new_async().await;

    // Get the fixtures directory
    let fixtures_dir = Path::new("test_data");

    // Create a mock that matches any GET request and dynamically reads files from disk
    let _mock = server
        .mock("GET", Matcher::Any)
        .with_status(200)
        .with_header("content-type", "text/html; charset=utf-8")
        .with_body_from_request(|request| {
            // Extract the path from the request
            let path = request.path();

            // Remove leading slash and construct the file path
            let filename = path.trim_start_matches('/');
            let file_path = fixtures_dir.join(filename);

            // Check if the file exists
            if file_path.is_file() {
                // Read the file content as bytes
                fs::read(&file_path).unwrap_or_else(|_| Vec::new())
            } else {
                eprintln!("Warning: file not found: {}", filename);
                // Return empty response for non-existent files
                Vec::new()
            }
        })
        .create_async()
        .await;

    // Capture the host_with_port before moving server
    let host_with_port = server.host_with_port();

    // Create a function to get the base URL
    let base_url_fn = Box::new(move || Url::parse(&format!("http://{}", host_with_port)).unwrap());

    (server, base_url_fn)
}

/// Helper function to create a URL for a specific fixture file
pub fn fixture_url(base_url: &Url, filename: &str) -> Url {
    base_url.join(&filename).unwrap()
}
