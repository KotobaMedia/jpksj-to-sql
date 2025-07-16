use anyhow::{anyhow, Result};
use scraper::{Html, Selector};
use serde::Serialize;
use std::collections::HashMap;
use url::Url;

#[derive(Debug, Clone, Serialize)]
pub enum RefType {
    Enum(Vec<String>),
    Code(HashMap<String, String>),
}

/// Parses reference data from a URL that contains a reference table
pub async fn parse_ref_from_url(url: &Url) -> Result<Option<RefType>> {
    if url.to_string().contains("PubFacAdminCd.html") {
        return Ok(None);
    }

    let response = reqwest::get(url.clone()).await?;
    let body = response.text().await?;
    let document = Html::parse_document(&body);

    // Selector for cells (<td> or <th>)
    let td_sel = Selector::parse("td, th").unwrap();
    // Selector for table rows
    let tr_sel = Selector::parse("table tr").unwrap();

    let mut headers = Vec::new();
    // Extract first row
    let first_row = document
        .select(&tr_sel)
        .next()
        .ok_or_else(|| anyhow!("no first row found"))?;

    for element in first_row.select(&td_sel) {
        headers.push(
            element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string(),
        );
    }

    if headers.is_empty() {
        return Err(anyhow!("no headers found"));
    }

    let code_idx_opt = headers.iter().position(|h| h == "コード");
    if let Some(code_idx) = code_idx_opt {
        let name_idx = headers
            .iter()
            .position(|h| {
                h == "対応する内容"
                    || h == "内容"
                    || h.contains("定義")
                    || h.contains("分類")
                    || h.contains("種別")
                    || h.contains("対象")
                    || h.contains("区分")
            })
            .ok_or_else(|| anyhow!("name index not found in headers: {:?}", headers))?;
        // code list
        let mut code_map = HashMap::new();
        for row in document.select(&tr_sel) {
            let tds = row.select(&td_sel).collect::<Vec<_>>();
            if tds.len() < 2 {
                continue;
            }
            // code_idx is the index of the code column
            let code = tds
                .get(code_idx)
                .ok_or(anyhow!("code not found"))?
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string();
            let name = tds
                .get(name_idx)
                .ok_or(anyhow!("name not found"))?
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string();
            if !code.is_empty() && code != "コード" && !name.is_empty() {
                code_map.insert(code, name);
            }
        }
        if code_map.is_empty() {
            return Err(anyhow!("no code found"));
        }
        return Ok(Some(RefType::Code(code_map)));
    } else if headers[0].contains("定数") {
        // enum list
        let mut enum_list = Vec::new();
        for cell in document.select(&td_sel) {
            let cell_text = cell.text().collect::<Vec<_>>().join(" ").trim().to_string();
            if !cell_text.is_empty() && cell_text != "定数" {
                enum_list.push(cell_text);
            }
        }
        if enum_list.is_empty() {
            return Err(anyhow!("no enum found"));
        }
        return Ok(Some(RefType::Enum(enum_list)));
    }

    Err(anyhow!("ref table not found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scraper::test_helpers::{fixture_url, setup_mock_server};

    async fn create_mock_server() -> (mockito::ServerGuard, Box<dyn Fn() -> Url>) {
        setup_mock_server().await
    }

    #[tokio::test]
    async fn test_parse_ref_enum() {
        let (_server, base_url_fn) = create_mock_server().await;
        let base_url = base_url_fn();
        let url = fixture_url(&base_url, "/ksj/gml/codelist/L01_v3_2_RoadEnumType.html");

        let ref_enum = parse_ref_from_url(&url).await.unwrap().unwrap();
        if let RefType::Enum(ref enum_list) = ref_enum {
            assert_eq!(enum_list.len(), 14);
            assert_eq!(enum_list[0], "国道");
            assert_eq!(enum_list[1], "都道");
        } else {
            panic!("Expected RefType::Enum, but got something else.");
        }
    }

    struct TestCase<'a> {
        url: &'a str,
        expected_len: usize,
        expected: HashMap<&'a str, &'a str>,
    }

    async fn run_parse_ref_code_test(test_case: TestCase<'_>) {
        let (_server, base_url_fn) = create_mock_server().await;
        let base_url = base_url_fn();
        let url = fixture_url(&base_url, test_case.url);

        let ref_enum = parse_ref_from_url(&url).await.unwrap().unwrap();

        match ref_enum {
            RefType::Code(ref code_map) => {
                assert_eq!(code_map.len(), test_case.expected_len);
                for (key, value) in test_case.expected.iter() {
                    assert_eq!(code_map.get(*key).unwrap(), value);
                }
            }
            _ => panic!("Expected RefType::Code, but got something else."),
        }
    }

    #[tokio::test]
    async fn test_parse_ref_code() {
        let test_cases = [
            TestCase {
                url: "/ksj/gml/codelist/reasonForDesignationCode.html",
                expected_len: 7,
                expected: HashMap::from([
                    ("1", "水害（河川）"),
                    ("2", "水害（海）"),
                    ("3", "水害（河川・海）"),
                    ("7", "その他"),
                ]),
            },
            TestCase {
                url: "/ksj/gml/codelist/CodeOfPhenomenon.html",
                expected_len: 3,
                expected: HashMap::from([
                    ("1", "急傾斜地の崩壊"),
                    ("2", "土石流"),
                    ("3", "地滑り"),
                ]),
            },
            TestCase {
                url: "/ksj/gml/codelist/MedClassCd.html",
                expected_len: 3,
                expected: HashMap::from([("1", "病院"), ("2", "診療所"), ("3", "歯科診療所")]),
            },
            TestCase {
                url: "/ksj/gml/codelist/ReferenceDataCd.html",
                expected_len: 6,
                expected: HashMap::from([
                    ("1", "10mDEM"),
                    ("2", "5m空中写真DEM"),
                    ("3", "5mレーザDEM"),
                    ("4", "2mDEM"),
                ]),
            },
            TestCase {
                url: "/ksj/gml/codelist/LandUseCd-09.html",
                expected_len: 17,
                expected: HashMap::from([("0100", "田"), ("1100", "河川地及び湖沼")]),
            },
            TestCase {
                url: "/ksj/gml/codelist/welfareInstitution_welfareFacilityMiddleClassificationCode.html",
                expected_len: 62,
                expected: HashMap::from([("0101", "救護施設"), ("0399", "その他")]),
            },
            TestCase {
                url: "/ksj/gml/codelist/water_depth_code.html",
                expected_len: 6,
                expected: HashMap::from([("1", "0m 以上 0.5m 未満"), ("6", "20.0m 以上")]),
            },
        ];

        for test_case in test_cases {
            run_parse_ref_code_test(test_case).await;
        }
    }
}
