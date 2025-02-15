use std::sync::Arc;

use crate::scraper::Dataset;
use anyhow::{Context, Result};
use serde::Serialize;
use tokio_postgres::{Client, NoTls};

const INIT_SQL: &str = include_str!("../data/schema.sql");

#[derive(Clone)]
pub struct MetadataConnection {
    client: Arc<Client>,
}

impl MetadataConnection {
    pub async fn new(connection_str: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_str, NoTls)
            .await
            .with_context(|| "when connecting to PostgreSQL")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("PostgreSQL connection error: {}", e);
            }
        });
        client
            .simple_query(INIT_SQL) // we use simple_query because we are running
            .await
            .with_context(|| "when initializing PostgreSQL schema")?;
        Ok(MetadataConnection {
            client: Arc::new(client),
        })
    }

    pub async fn create_dataset(&self, dataset: &Dataset) -> Result<()> {
        let lowercase_identifier = &dataset.page.identifier.to_lowercase();
        let metadata = DatasetMetadata {
            data_item: &dataset.initial_item,
            data_page: &dataset.page,
        };
        let metadata_value = serde_json::to_value(metadata)?;
        self.client
            .execute(
                "INSERT INTO datasets (table_name, metadata) VALUES ($1, $2) ON CONFLICT (table_name) DO UPDATE SET metadata = EXCLUDED.metadata",
                &[&lowercase_identifier, &metadata_value],
            )
            .await
            .with_context(|| "when inserting dataset into PostgreSQL")?;
        Ok(())
    }
}

#[derive(Serialize)]
struct DatasetMetadata<'a> {
    data_item: &'a crate::scraper::initial::DataItem,
    data_page: &'a crate::scraper::data_page::DataPage,
}
