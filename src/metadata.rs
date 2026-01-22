use crate::{loader::mapping::ShapefileMetadata, scraper::Dataset};
use anyhow::{Context, Result};
use km_to_sql::{
    metadata::{ColumnEnumDetails, ColumnForeignKeyDetails, ColumnMetadata, TableMetadata},
    postgres::{init_schema, upsert},
};
use std::sync::Arc;
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
            .batch_execute(INIT_SQL)
            .await
            .with_context(|| "when initializing PostgreSQL schema")?;
        init_schema(&client).await?;

        Ok(MetadataConnection {
            client: Arc::new(client),
        })
    }

    pub async fn build_metadata_from_dataset(
        &self,
        table_name: &str,
        metadata: &ShapefileMetadata,
        dataset: &Dataset,
    ) -> Result<TableMetadata> {
        let columns_in_db = self
            .client
            .query(
                r#"
                SELECT
                    cols.ordinal_position AS position,
                    cols.column_name,
                    cols.udt_name AS underlying_type,
                    gc.type AS geometry_type,
                    gc.srid AS geometry_srid
                FROM information_schema.columns cols
                LEFT JOIN public.geometry_columns gc
                    ON gc.f_table_schema = cols.table_schema
                    AND gc.f_table_name = cols.table_name
                    AND gc.f_geometry_column = cols.column_name
                WHERE cols.table_schema = 'public'
                AND cols.table_name = $1
                ORDER BY cols.ordinal_position
                "#,
                &[&table_name],
            )
            .await
            .with_context(|| "when querying columns from PostgreSQL")?;

        // println!("[table: {}] Columns in DB: {:?}", table_name, columns_in_db);

        let data_item = &dataset.initial_item;
        let data_page = &dataset.page;

        let dp_col_vec: Vec<_> = data_page.metadata.attribute.clone().into_values().collect();

        let mut columns: Vec<ColumnMetadata> = vec![];
        for db_column in columns_in_db {
            let column_name: String = db_column.get(1);
            let column_type: String = db_column.get(2);
            let geometry_type: Option<String> = db_column.get(3);
            let geometry_srid: Option<i32> = db_column.get(4);
            let column_type = if let Some(geometry_type) = geometry_type {
                format!(
                    "geometry({}, {})",
                    geometry_type,
                    geometry_srid.unwrap_or(-1)
                )
            } else {
                column_type
            };

            let mut column_metadata = ColumnMetadata {
                name: column_name.clone(),
                desc: None,
                data_type: column_type,
                foreign_key: None,
                enum_values: None,
            };

            if let Some(column) = dp_col_vec.iter().find(|c| c.name == column_name) {
                column_metadata.desc = Some(column.description.clone());

                if column.attr_type.contains("行政区域コード") {
                    column_metadata.foreign_key = Some(ColumnForeignKeyDetails {
                        foreign_table: "admini_boundary_cd".to_string(),
                        foreign_column: "改正後のコード".to_string(),
                    });
                }

                use crate::scraper::data_page::RefType;
                column_metadata.enum_values = match &column.r#ref {
                    Some(RefType::Code(map)) => Some(
                        map.iter()
                            .map(|(key, value)| ColumnEnumDetails {
                                value: key.clone(),
                                desc: Some(value.clone()),
                            })
                            .collect(),
                    ),
                    Some(RefType::Enum(vec)) => Some(
                        vec.iter()
                            .map(|value| ColumnEnumDetails {
                                value: value.clone(),
                                desc: None,
                            })
                            .collect(),
                    ),
                    None => None,
                }
            }

            columns.push(column_metadata);
        }

        let desc = data_page.metadata.fundamental.get("内容").cloned().or_else(|| {
            let trimmed = data_item.name.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        let table_metadata = TableMetadata {
            name: metadata.name.clone(),
            desc,
            source: Some("国土数値情報".to_string()),
            source_url: Some(data_page.url.clone()),
            license: if data_item.usage.is_empty() {
                None
            } else {
                Some(data_item.usage.clone())
            },
            license_url: None,
            primary_key: Some("ogc_fid".to_string()),
            columns,
        };

        Ok(table_metadata)
    }

    pub async fn create_dataset(&self, identifier: &str, dataset: &TableMetadata) -> Result<()> {
        let lowercase_identifier = identifier.to_lowercase();
        upsert(&self.client, &lowercase_identifier, dataset).await?;
        Ok(())
    }
}
