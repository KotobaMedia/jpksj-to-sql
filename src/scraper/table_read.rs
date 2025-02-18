use ndarray::Array2;
use scraper::{selectable::Selectable, ElementRef, Selector};

/// Parses an HTML table (handling colspan/rowspan) and returns a 2D ndarray
/// where each cell is an Option<String>.
pub fn parse_table<'a, S: Selectable<'a>>(table: S) -> Array2<Option<ElementRef<'a>>> {
    let tr_selector = Selector::parse("tr").unwrap();
    let cell_selector = Selector::parse("th, td").unwrap();

    // We'll accumulate rows in a Vec<Vec<Option<String>>>
    let mut grid: Vec<Vec<Option<ElementRef<'a>>>> = Vec::new();
    // pending holds cells that span into future rows:
    // (target_row, col_index, cell)
    let mut pending: Vec<(usize, usize, ElementRef<'a>)> = Vec::new();

    for (row_index, tr) in table.select(&tr_selector).enumerate() {
        // Ensure our grid has an entry for this row.
        if grid.len() <= row_index {
            grid.push(Vec::new());
        }
        let row = &mut grid[row_index];

        // First, insert any cells carried over from previous rows (rowspan)
        for &(_target_row, col_index, ref cell) in
            pending.iter().filter(|&&(r, _, _)| r == row_index)
        {
            while row.len() <= col_index {
                row.push(None);
            }
            row[col_index] = Some(cell.clone());
        }
        // Remove pending entries that have been used
        pending.retain(|&(r, _, _)| r > row_index);

        // Now process the current row’s cells.
        let mut col_index = 0;
        for cell_node in tr.select(&cell_selector) {
            // Skip any columns already filled by pending cells.
            while row.get(col_index).is_some() {
                col_index += 1;
            }
            // Extract cell content.
            // let content = cell_node
            //     .text()
            //     .collect::<Vec<_>>()
            //     .join(" ")
            //     .trim()
            //     .to_string();
            let content = cell_node;
            let colspan = cell_node
                .value()
                .attr("colspan")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1);
            let rowspan = cell_node
                .value()
                .attr("rowspan")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1);
            let cell = content;

            // Place the cell in the current row for each column it spans.
            for offset in 0..colspan {
                while row.len() <= col_index + offset {
                    row.push(None);
                }
                row[col_index + offset] = Some(cell.clone());
            }
            // For rowspan > 1, schedule this cell for future rows.
            if rowspan > 1 {
                for r in 1..rowspan {
                    pending.push((row_index + r, col_index, cell.clone()));
                }
            }
            col_index += colspan;
        }
    }

    // Determine the final dimensions.
    let nrows = grid.len();
    let ncols = grid.iter().map(|r| r.len()).max().unwrap_or(0);

    // Build a single Vec of length nrows*ncols, padding missing cells with None.
    let mut data = Vec::with_capacity(nrows * ncols);
    for row in grid.iter() {
        let mut new_row = row.clone();
        new_row.resize(ncols, None);
        data.extend(new_row);
    }

    // Create the ndarray (note: Array2 is row-major by default).
    Array2::from_shape_vec((nrows, ncols), data).expect("Shape mismatch")
}

pub fn parsed_to_string_array(parsed: Array2<Option<ElementRef>>) -> Array2<Option<String>> {
    parsed.map(|x| x.map(|y| y.text().collect::<String>()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table() {
        // Example HTML (you would replace this with your actual table HTML)
        let html_str = r#"
        <table>
          <tr>
            <th>Header 1</th>
            <th colspan="2">Header 2</th>
          </tr>
          <tr>
            <td rowspan="2">A</td>
            <td>B</td>
            <td>C</td>
          </tr>
          <tr>
            <td colspan="2">D</td>
          </tr>
        </table>
        "#;
        let html = scraper::Html::parse_document(&html_str);
        let table_array = parse_table(&html);
        let table_array_str = parsed_to_string_array(table_array);

        let expected = Array2::from_shape_vec(
            (3, 3),
            vec![
                Some("Header 1".into()),
                Some("Header 2".into()),
                Some("Header 2".into()),
                Some("A".into()),
                Some("B".into()),
                Some("C".into()),
                Some("A".into()),
                Some("D".into()),
                Some("D".into()),
            ],
        )
        .expect("Failed to create expected array");

        assert_eq!(table_array_str, expected);
    }
}
