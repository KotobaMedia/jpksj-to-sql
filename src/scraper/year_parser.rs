use once_cell::sync::Lazy;
use regex::Regex;
use url::Url;

use super::data_page::DataItem;

// Compile the regex once for efficiency.
// This regex looks for one or more digits at the very start of the string, immediately followed by '年'.
pub static YEAR_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(\d+)年").unwrap());

// Regex for extracting year ranges and URLs from data selection lines
// Supports patterns like:
// - "最新のデータは2021年"
// - "最新のデータはデータ作成年度 2021年度（令和3年度）版です"
// - "データ基準年：2020年"
// - "データ作成年度：2020年度（令和2年度）～2014年度（平成26年度）版"
// - "データ作成年度：2013年度（平成25年度）版"
// - "データ基準年：2015年以前" (creates range from 2015 to 0)
static YEAR_RANGE_REGEX: Lazy<Regex> = Lazy::new(|| {
    // 1: prefix, 2: start year, 3: start year suffix, 4: end year, 5: end year suffix, 6: 以前 indicator
    Regex::new(r"(データ基準年：|最新のデータは(?:データ作成年度\s*)?|データ作成年度：)(\d{4})(年度?|年)(?:（[^）]*）)?(?:～(\d{4})(年度?|年)(?:（[^）]*）)?|(以前))?").unwrap()
});

/// Extracts the numeric year from a field formatted like "2006年（平成18年）".
/// If the field does not match, returns None.
pub fn extract_year_from_field(field: &str) -> Option<u32> {
    YEAR_REGEX
        .captures(field)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

/// Determines the recency value for an item, preferring the `year` field.
/// Falls back to `nendo` if necessary.
pub fn parse_recency(item: &DataItem) -> Option<u32> {
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

/// Extracts multiple years from comma-separated year lists
pub fn extract_multiple_years(text: &str) -> Option<Vec<u32>> {
    let mut years = Vec::new();

    // Extract all 4-digit numbers followed by 年度 or 年
    let year_pattern = Regex::new(r"(\d{4})年度?").unwrap();

    for capture in year_pattern.captures_iter(text) {
        if let Some(year_match) = capture.get(1) {
            if let Ok(year) = year_match.as_str().parse::<u32>() {
                years.push(year);
            }
        }
    }

    if years.is_empty() {
        None
    } else {
        Some(years)
    }
}

/// Parses a year range from text that matches the YEAR_RANGE_REGEX pattern.
/// Returns a RangeInclusive<u32> if successful.
/// For "YYYY年以前" patterns, returns a range from 0 to YYYY to indicate "and earlier".
pub fn parse_year_range(text: &str) -> Option<std::ops::RangeInclusive<u32>> {
    let captures = YEAR_RANGE_REGEX.captures(text)?;

    let first_year = captures
        .get(2)
        .and_then(|m| m.as_str().parse::<u32>().ok())?;

    // Check if this is a "以前" pattern (group 6 would be captured)
    let second_year = if captures.get(6).is_some() {
        0 // "以前" means "and earlier", so range goes down to 0
    } else {
        captures
            .get(4)
            .and_then(|m| m.as_str().parse::<u32>().ok())
            .unwrap_or(first_year)
    };

    // Ensure the lower bound is always the smaller integer
    let (lower, upper) = if first_year <= second_year {
        (first_year, second_year)
    } else {
        (second_year, first_year)
    };

    Some(lower..=upper)
}

/// Parses yearly version information from a line of text.
/// Returns a YearlyVersion if the line contains valid year range information.
pub fn parse_yearly_version_from_line(
    line_text: &str,
    line_document: &scraper::Html,
    base_url: &Url,
) -> Option<super::data_page::YearlyVersion> {
    // Try to match the year range pattern first
    if let Some(year_range) = parse_year_range(line_text) {
        let a_selector = scraper::Selector::parse("a").unwrap();
        let url = line_document
            .select(&a_selector)
            .next()
            .and_then(|a| a.value().attr("href"))
            .and_then(|href| base_url.join(href).ok());

        let url = if url.is_none() && line_text.contains("最新のデータは") {
            base_url.clone()
        } else {
            url?
        };

        return Some(super::data_page::YearlyVersion {
            year: year_range,
            url,
        });
    }

    // Handle comma-separated years pattern
    if line_text.contains("データ作成年度：") {
        if let Some(years) = extract_multiple_years(line_text) {
            let a_selector = scraper::Selector::parse("a").unwrap();
            let url = line_document
                .select(&a_selector)
                .next()
                .and_then(|a| a.value().attr("href"))
                .and_then(|href| base_url.join(href).ok())?;

            // Create a range from min to max year
            let min_year = *years.iter().min()?;
            let max_year = *years.iter().max()?;

            return Some(super::data_page::YearlyVersion {
                year: min_year..=max_year,
                url,
            });
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_year_from_field() {
        assert_eq!(extract_year_from_field("2006年（平成18年）"), Some(2006));
        assert_eq!(extract_year_from_field("2021年度"), Some(2021));
        assert_eq!(extract_year_from_field("平成18年"), None);
        assert_eq!(extract_year_from_field(""), None);
    }

    #[test]
    fn test_extract_multiple_years() {
        let text = "データ作成年度：2013年度（平成25年度）、2014年度（平成26年度）、2015年度（平成27年度）";
        let years = extract_multiple_years(text);
        assert_eq!(years, Some(vec![2013, 2014, 2015]));
    }

    #[test]
    fn test_debug_regex() {
        let test_text = "データ基準年：2015年以前（平成27年～18年,12年,7年,昭和60年,55年,50年,45年,40年,35年,30年,25年,大正9年）版のデータ詳細";
        let captures = YEAR_RANGE_REGEX.captures(test_text);
        println!("Captures: {:?}", captures);
        if let Some(caps) = captures {
            for (i, cap) in caps.iter().enumerate() {
                if let Some(m) = cap {
                    println!("Group {}: '{}'", i, m.as_str());
                }
            }
        }
    }

    #[test]
    fn test_parse_year_range() {
        assert_eq!(parse_year_range("最新のデータは2021年"), Some(2021..=2021));
        assert_eq!(parse_year_range("データ基準年：2020年"), Some(2020..=2020));

        assert_eq!(
            parse_year_range("データ作成年度：2020年度（令和2年度）～2014年度（平成26年度）版"),
            Some(2014..=2020)
        );
        assert_eq!(
            parse_year_range("データ作成年度：2013年度（平成25年度）版"),
            Some(2013..=2013)
        );
        assert_eq!(parse_year_range("invalid text"), None);

        assert_eq!(
            parse_year_range("データ基準年：2015年以前（平成27年～18年,12年,7年,昭和60年,55年,50年,45年,40年,35年,30年,25年,大正9年）版のデータ詳細"),
            Some(0..=2015)
        );
    }
}
