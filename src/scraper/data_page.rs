use anyhow::Result;
use bytesize::ByteSize;
use once_cell::sync::Lazy;
use regex::Regex;
use url::Url;

// Compile the regex once for efficiency.
// This regex looks for one or more digits at the very start of the string, immediately followed by '年'.
static YEAR_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(\d+)年").unwrap());

#[derive(Debug)]
pub struct DataPage {
    pub url: Url,
    pub items: Vec<DataItem>,
}

#[derive(Debug)]
pub struct DataItem {
    pub area: String,
    pub crs: String,
    pub bytes: u64,
    pub year: Option<String>,  // 年
    pub nendo: Option<String>, // 年度
    pub file_url: Url,
}

pub async fn scrape(url: Url) -> Result<DataPage> {
    let response = reqwest::get(url.clone()).await?;
    let body = response.text().await?;
    let document = scraper::Html::parse_document(&body);
    let data_tr_sel = scraper::Selector::parse("table.dataTables tr").unwrap();

    let data_path_re = Regex::new(r"javascript:DownLd\('[^']*',\s*'[^']*',\s*'([^']+)'").unwrap();

    let mut items: Vec<DataItem> = Vec::new();
    let mut use_nendo = false;
    for row in document.select(&data_tr_sel) {
        let td_sel = scraper::Selector::parse("td").unwrap();
        let tds = row.select(&td_sel).collect::<Vec<_>>();
        if tds.len() == 0 {
            continue;
        }
        let area = tds[0].text().collect::<String>().trim().to_string();
        if area == "地域" {
            // header
            if tds[2].text().collect::<String>().contains("年度") {
                use_nendo = true;
            }
            continue;
        }

        let crs = tds[1].text().collect::<String>().trim().to_string();

        let year_str = tds[2].text().collect::<String>().trim().to_string();
        let year = if use_nendo == true {
            None
        } else {
            Some(year_str.clone())
        };
        let nendo = if use_nendo == false {
            None
        } else {
            Some(year_str)
        };
        let Some(bytes_str) = tds[3].text().next().map(|s| s.to_string()) else {
            continue;
        };
        // if we couldn't parse the bytes, we'll just use 10MB as a default. It's just used for progress.
        let bytes: ByteSize = bytes_str.parse().unwrap_or_else(|_| ByteSize::mb(10));
        let Some(file_js_onclick) = tds[5]
            .select(&scraper::Selector::parse("a").unwrap())
            .next()
            .and_then(|a| a.value().attr("onclick"))
        else {
            // panic!("file_js_onclick not found: {:?}", tds[5].html());
            continue;
        };
        let Some(file_url) = data_path_re
            .captures(file_js_onclick)
            .map(|c| c.get(1).unwrap().as_str())
            .map(|s| url.join(s).unwrap())
        else {
            // panic!("file_url not found: {:?}", file_js_onclick);
            continue;
        };

        let item = DataItem {
            area,
            bytes: bytes.0,
            crs,
            year,
            nendo,
            file_url,
        };
        items.push(item);
    }

    Ok(DataPage { url, items })
}

/// Extracts the numeric year from a field formatted like "2006年（平成18年）".
/// If the field does not match, returns None.
fn extract_year_from_field(field: &str) -> Option<u32> {
    YEAR_REGEX
        .captures(field)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

/// Determines the recency value for an item, preferring the `year` field.
/// Falls back to `nendo` if necessary.
fn parse_recency(item: &DataItem) -> Option<u32> {
    if let Some(ref y) = item.year {
        if let Some(year) = extract_year_from_field(y) {
            return Some(year);
        }
    }
    if let Some(ref n) = item.nendo {
        if let Some(year) = extract_year_from_field(n) {
            return Some(year);
        }
    }
    None
}

/**
 * データのリストから、CRSが世界測地系のものを抽出する
 * 全国データある場合はそれだけを返す
 * ない場合はそのまま帰す（殆どの場合は都道府県別）
 */
pub fn filter_data_items(items: Vec<DataItem>) -> Vec<DataItem> {
    // Step 1: Filter items by CRS.
    let crs_filtered: Vec<DataItem> = items
        .into_iter()
        .filter(|item| item.crs == "世界測地系")
        .collect();

    // Step 2: If any item has area == "全国", narrow to those items only.
    let area_filtered: Vec<DataItem> = if crs_filtered.iter().any(|item| item.area == "全国") {
        crs_filtered
            .into_iter()
            .filter(|item| item.area == "全国")
            .collect()
    } else {
        crs_filtered
    };

    // Step 3: Determine the most recent year among items that have one.
    let max_recency = area_filtered
        .iter()
        .filter_map(|item| parse_recency(item))
        .max();

    if let Some(max_year) = max_recency {
        area_filtered
            .into_iter()
            .filter(|item| parse_recency(item) == Some(max_year))
            .collect()
    } else {
        // No valid recency info; return all area_filtered items.
        area_filtered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scrape() {
        let url =
            Url::parse("https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-C23.html").unwrap();
        let page = scrape(url).await.unwrap();
        println!("{:?}", page);
        // assert_eq!(page.items.len(), 47);
    }
}
