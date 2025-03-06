//! Loader for AdminiBoundary_CD.xslx
//! This module is responsible for loading the AdminiBoundary_CD.xslx file into the database.

use crate::{
    downloader,
    metadata::MetadataConnection,
    scraper::{
        data_page::{AttributeMetadata, DataPage, DataPageMetadata},
        initial::DataItem,
        Dataset,
    },
};
use anyhow::{Context, Result};
use calamine::{Reader, Xlsx};
use std::{collections::HashMap, sync::Arc, vec};
use tokio_postgres::{types::ToSql, NoTls};
use unicode_normalization::UnicodeNormalization;
use url::Url;

use super::xslx_helpers::data_to_string;

async fn download_admini_boundary_file() -> Result<downloader::DownloadedFile> {
    let url = Url::parse("https://nlftp.mlit.go.jp/ksj/gml/codelist/AdminiBoundary_CD.xlsx")?;
    downloader::download_to_tmp(&url).await
}

#[derive(Debug)]
struct ParsedFile {
    rows: Vec<Vec<Option<String>>>,
}

async fn parse() -> Result<ParsedFile> {
    let file = download_admini_boundary_file().await?;
    let path = file.path;
    let mut workbook: Xlsx<_> = calamine::open_workbook(&path)?;
    let sheet = workbook.worksheet_range("行政区域コード")?;
    let mut data_started = false;

    let mut out = Vec::new();

    for row in sheet.rows() {
        if !data_started {
            let first_cell_str = data_to_string(&row[0]);
            if first_cell_str.is_some_and(|s| s == "行政区域コード") {
                data_started = true;
            }
            continue;
        }

        let row_strings = row
            .iter()
            .map(|cell| {
                let str_opt = data_to_string(cell);
                if let Some(s) = str_opt {
                    if s.is_empty() {
                        None
                    } else {
                        Some(s.nfkc().to_string())
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<Option<String>>>();
        if row_strings.iter().all(|s| s.is_none()) {
            continue;
        }
        out.push(row_strings);
    }
    Ok(ParsedFile { rows: out })
}

async fn load(postgres_url: &str, parsed: &ParsedFile) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .with_context(|| "when connecting to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    client
        .execute(
            r#"
            DELETE FROM "admini_boundary_cd";
            "#,
            &[],
        )
        .await?;

    let query = r#"
        INSERT INTO "admini_boundary_cd" (
            "行政区域コード",
            "都道府県名（漢字）",
            "市区町村名（漢字）",
            "都道府県名（カナ）",
            "市区町村名（カナ）",
            "コードの改定区分",
            "改正年月日",
            "改正後のコード",
            "改正後の名称",
            "改正後の名称（カナ）",
            "改正事由等"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT ("行政区域コード") DO NOTHING
    "#;
    for row in parsed.rows.iter() {
        let params: Vec<&(dyn ToSql + Sync)> =
            row.iter().map(|v| v as &(dyn ToSql + Sync)).collect();
        client.execute(query, &params).await?;
    }
    Ok(())
}

async fn create_admini_boundary_metadata(postgres_url: &str) -> Result<()> {
    let metadata_conn = MetadataConnection::new(postgres_url).await?;

    let dataset = Dataset {
        initial_item: DataItem {
            category1_name: "行政区域".to_string(),
            category2_name: "行政区域コード".to_string(),
            name: "行政区域コード".to_string(),
            data_source: "".to_string(),
            data_accuracy: "".to_string(),
            metadata_xml: Url::parse(
                "https://nlftp.mlit.go.jp/ksj/gml/codelist/AdminiBoundary_CD.xlsx",
            )?,
            identifier: "admini_boundary_cd".to_string(),
            usage: "".to_string(),
            url: Url::parse("https://nlftp.mlit.go.jp/ksj/gml/codelist/AdminiBoundary_CD.xlsx")?,
        },
        page: Arc::new(DataPage {
            url: Url::parse("https://nlftp.mlit.go.jp/ksj/gml/codelist/AdminiBoundary_CD.xlsx")?,
            items: vec![],
            metadata: DataPageMetadata {
                fundamental: HashMap::from([
                    ("内容".to_string(), "コードリスト「行政区域コード」の定義。統廃合による欠番の関連付けに使ってください。このテーブルに位置情報は存在しません。行政堺は「改正後のコード」を n03_union の「全国地方公共団体コード」にジョインしてご利用ください。".to_string()),
                    ("データ形状".to_string(), "表データ".to_string()),
                ]),
                attribute: HashMap::from([
                    (
                        "行政区域コード".to_string(),
                        AttributeMetadata {
                            name: "行政区域コード".to_string(),
                            description: "統廃合前の行政区域コード".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "都道府県名（漢字）".to_string(),
                        AttributeMetadata {
                            name: "都道府県名（漢字）".to_string(),
                            description: "都道府県名（漢字）".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "市区町村名（漢字）".to_string(),
                        AttributeMetadata {
                            name: "市区町村名（漢字）".to_string(),
                            description: "市区町村名（漢字）".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "都道府県名（カナ）".to_string(),
                        AttributeMetadata {
                            name: "都道府県名（カナ）".to_string(),
                            description: "都道府県名（カナ）".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "市区町村名（カナ）".to_string(),
                        AttributeMetadata {
                            name: "市区町村名（カナ）".to_string(),
                            description: "市区町村名（カナ）".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "コードの改定区分".to_string(),
                        AttributeMetadata {
                            name: "コードの改定区分".to_string(),
                            description: "コードの改定区分".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "改正年月日".to_string(),
                        AttributeMetadata {
                            name: "改正年月日".to_string(),
                            description: "改正年月日".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "改正後のコード".to_string(),
                        AttributeMetadata {
                            name: "改正後のコード".to_string(),
                            description: "統廃合後の行政区域コード。全国地方公共団体コードに相当する値。".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "改正後の名称".to_string(),
                        AttributeMetadata {
                            name: "改正後の名称".to_string(),
                            description: "改正後の名称".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "改正後の名称（カナ）".to_string(),
                        AttributeMetadata {
                            name: "改正後の名称（カナ）".to_string(),
                            description: "改正後の名称（カナ）".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                    (
                        "改正事由等".to_string(),
                        AttributeMetadata {
                            name: "改正事由等".to_string(),
                            description: "改正事由等".to_string(),
                            attr_type: "String".to_string(),
                            ref_url: None,
                        },
                    ),
                ]),
            },
        }),
        zip_file_paths: vec![],
    };

    metadata_conn.create_dataset(&dataset).await?;
    Ok(())
}

pub async fn load_admini_boundary(postgres_url: &str) -> Result<()> {
    let parsed = parse().await?;
    load(postgres_url, &parsed).await?;
    create_admini_boundary_metadata(postgres_url).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_download_admini_boundary_file() {
        let file = download_admini_boundary_file().await.unwrap();
        assert!(file.path.exists());
    }

    #[tokio::test]
    async fn test_parse_admini() {
        let parsed_file = parse().await.unwrap();
        assert!(!parsed_file.rows.is_empty());
        assert_eq!(parsed_file.rows[0].len(), 11);
        assert_eq!(parsed_file.rows[0][0], Some("01000".to_string()));
        assert_eq!(parsed_file.rows[0][1], Some("北海道".to_string()));
        assert_eq!(parsed_file.rows[0][2], None);
        assert_eq!(parsed_file.rows[0][3], Some("ホッカイドウ".to_string()));
    }
}
