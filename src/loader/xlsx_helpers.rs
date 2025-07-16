use calamine::{Data, DataType as _};

pub fn data_to_string(data: &Data) -> Option<String> {
    data.get_string()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}
