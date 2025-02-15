use anyhow::Result;
use regex::Regex;
use serde::Serialize;
use url::Url;

#[derive(Debug, Clone, Serialize)]
pub struct DataItem {
    pub category1_name: String,
    pub category2_name: String,
    pub name: String,
    pub data_source: String,
    pub data_accuracy: String,
    pub metadata_xml: Url,
    pub usage: String,

    pub url: Url,
}

#[derive(Debug)]
pub struct ScrapeResult {
    pub url: Url,
    pub data: Vec<DataItem>,
}

pub async fn scrape() -> Result<ScrapeResult> {
    let root_url = Url::parse("https://nlftp.mlit.go.jp/ksj/gml/gml_datalist.html")?;
    let mut data: Vec<DataItem> = vec![];
    let response = reqwest::get(root_url.clone()).await?;
    let body = response.text().await?;
    let document = scraper::Html::parse_document(&body);
    let collapse_sel = scraper::Selector::parse("ul.collapsible").unwrap();
    let category1_re = Regex::new(r"\d(?:\.\s*)?([^\s]+)").unwrap();
    for collapse in document.select(&collapse_sel) {
        let header_sel = scraper::Selector::parse(".collapsible-header").unwrap();
        let Some(header) = collapse.select(&header_sel).next() else {
            continue;
        };
        let category1_txt = header.text().collect::<String>().trim().to_string();
        let Some(category1_name) = category1_re
            .captures(&category1_txt)
            .map(|c| c.get(1).unwrap().as_str())
        else {
            continue;
        };

        let table_tr_sel = scraper::Selector::parse("table tr").unwrap();
        let mut category2_name: Option<String> = None;
        for tr in collapse.select(&table_tr_sel) {
            let td_sel = scraper::Selector::parse("td").unwrap();
            let a_sel = scraper::Selector::parse("a").unwrap();
            let tds = tr.select(&td_sel).collect::<Vec<_>>();
            if tds.len() == 0 {
                continue;
            }
            let name_td = &tds[0];
            let name = name_td.text().collect::<String>().trim().to_string();
            if name.starts_with("【") {
                category2_name = Some(name);
                continue;
            }
            let Some(url_str) = name_td.select(&a_sel).next().and_then(|u| u.attr("href")) else {
                continue;
            };
            let url = root_url.join(url_str)?;

            let data_source = tds[2].text().collect::<String>().trim().to_string();
            let data_accuracy = tds[3].text().collect::<String>().trim().to_string();
            let metadata_xml_url: Url;
            if let Some(url) = tds[4].select(&a_sel).next().and_then(|a| a.attr("href")) {
                metadata_xml_url = root_url.join(url)?;
            } else {
                continue;
            }
            let usage = tds[5]
                .select(&a_sel)
                .next()
                .unwrap()
                .text()
                .collect::<String>()
                .trim()
                .to_string();

            data.push(DataItem {
                category1_name: category1_name.to_string(),
                category2_name: category2_name.clone().unwrap_or_default(),
                name,
                data_source,
                data_accuracy,
                metadata_xml: metadata_xml_url,
                usage,
                url,
            });
        }
    }

    Ok(ScrapeResult {
        url: root_url,
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scrape() {
        let result = scrape().await.unwrap();
        assert_eq!(result.data.len(), 125);
        let first = result.data.get(0).unwrap();
        assert_eq!(first.name, "海岸線");
    }
}
