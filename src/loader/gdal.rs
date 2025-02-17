use super::mapping::ShapefileMetadata;
use anyhow::{Context, Result};
use encoding_rs::{Encoding, SHIFT_JIS, UTF_8};
use jsonpath_rust::JsonPath;
use serde_json::Value;
use std::path::PathBuf;
use tokio::process::Command;

pub async fn create_vrt(
    out: &PathBuf,
    shapes: &Vec<PathBuf>,
    metadata: &ShapefileMetadata,
) -> Result<()> {
    if shapes.is_empty() {
        anyhow::bail!("No shapefiles found");
    }

    let bare_vrt = out.with_extension("");
    let layer_name = bare_vrt.file_name().unwrap().to_str().unwrap();
    // let vrt_path = shape.with_extension("vrt");

    let mut fields = String::new();
    let attributes = get_attribute_list(&shapes[0]).await?;
    for (field_name, shape_name) in metadata.field_mappings.iter() {
        // ignore attributes in the mapping that are not in the shapefile
        if attributes.iter().find(|&attr| attr == shape_name).is_none() {
            continue;
        }
        fields.push_str(&format!(
            r#"<Field name="{}" src="{}" />"#,
            field_name, shape_name
        ));
    }
    if fields.is_empty() {
        anyhow::bail!("No fields found in shapefile");
    }

    let mut layers = String::new();
    for shape in shapes {
        let bare_shape = shape.with_extension("");
        let shape_filename = bare_shape.file_name().unwrap().to_str().unwrap();
        let encoding = detect_encoding(shape).await?;
        layers.push_str(&format!(
            r#"
                <OGRVRTLayer name="{}">
                <SrcDataSource>{}</SrcDataSource>
                <OpenOptions><OOI key="ENCODING">{}</OOI></OpenOptions>
                {}
                </OGRVRTLayer>
            "#,
            shape_filename,
            shape.canonicalize().unwrap().to_str().unwrap(),
            encoding,
            fields
        ));
    }

    let vrt = format!(
        r#"
        <OGRVRTDataSource>
        <OGRVRTUnionLayer name="{}">
        {}
        </OGRVRTUnionLayer>
        </OGRVRTDataSource>
    "#,
        layer_name, layers
    );

    tokio::fs::write(&out, vrt).await?;

    Ok(())
}

pub async fn load_to_postgres(vrt: &PathBuf, postgres_url: &str) -> Result<()> {
    let mut cmd = Command::new("ogr2ogr");
    let output = cmd
        .arg("-f")
        .arg("PostgreSQL")
        .arg(format!("PG:{}", postgres_url))
        // .arg("-skipfailures")
        .arg("-lco")
        .arg("GEOM_TYPE=geometry")
        .arg("-lco")
        .arg("OVERWRITE=YES")
        .arg("-lco")
        .arg("GEOMETRY_NAME=geom")
        .arg("-nlt")
        .arg("PROMOTE_TO_MULTI")
        .arg("--config")
        .arg("PG_USE_COPY=YES")
        .arg(vrt)
        .output()
        .await?;

    if !output.status.success() {
        // the error message may contain malformed UTF8
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ogr2ogr failed: {}", stderr);
    }

    Ok(())
}

pub async fn has_layer(postgres_url: &str, layer_name: &str) -> Result<bool> {
    let layer_name_lower = layer_name.to_lowercase();
    let output = Command::new("ogrinfo")
        .arg("-if")
        .arg("postgresql")
        .arg(format!("PG:{}", postgres_url))
        .arg("-sql")
        .arg(&format!("SELECT 1 FROM \"{}\" LIMIT 1", layer_name_lower))
        .output()
        .await?;

    Ok(output.status.success())
}

async fn get_attribute_list(shape: &PathBuf) -> Result<Vec<String>> {
    let ogrinfo = Command::new("ogrinfo")
        .arg("-json")
        .arg(shape)
        .output()
        .await?;

    if !ogrinfo.status.success() {
        let stderr = String::from_utf8_lossy(&ogrinfo.stderr);
        anyhow::bail!("ogrinfo failed: {}", stderr);
    }

    let stdout_str = String::from_utf8_lossy(&ogrinfo.stdout);
    let json: Value =
        serde_json::from_str(&stdout_str).with_context(|| "when parsing ogrinfo JSON output")?;

    let encoding_path = JsonPath::try_from("$.layers[0].fields[*].name")?;
    let encoding_val = encoding_path.find_slice(&json);
    let mut attributes = vec![];
    for val in encoding_val {
        if let Value::String(attr) = val.clone().to_data() {
            attributes.push(attr);
        }
    }
    Ok(attributes)
}

// PC932 is almost the same as Shift-JIS, but most GIS software outputs as CP932 when using Shift-JIS
static ENCODINGS: &[(&str, &Encoding)] = &[("CP932", SHIFT_JIS), ("UTF-8", UTF_8)];

// We get the bytes from the ogrinfo output after "successful"
// this is because before "successful" is the filename, and the filename
// can contain non-UTF8 characters
fn bytes_after_successful(data: &Vec<u8>) -> Option<&[u8]> {
    let needle = b"successful"; // equivalent to "successful".as_bytes()
    data.windows(needle.len())
        .position(|window| window == needle)
        .map(|pos| &data[pos + needle.len()..])
}

async fn detect_encoding_fallback(shape: &PathBuf) -> Result<Option<String>> {
    let ogrinfo = Command::new("ogrinfo")
        .arg("-al")
        .arg("-geom=NO")
        .arg("-limit")
        .arg("100")
        .arg(shape)
        .output()
        .await?;

    if !ogrinfo.status.success() {
        let stderr = String::from_utf8_lossy(&ogrinfo.stderr);
        anyhow::bail!("ogrinfo failed: {}", stderr);
    }

    let Some(data) = bytes_after_successful(&ogrinfo.stdout) else {
        anyhow::bail!("ogrinfo failed to open {}", shape.display());
    };
    for (name, encoding) in ENCODINGS {
        // decode() returns a tuple: (decoded string, bytes read, had_errors)
        let (_decoded, _, had_errors) = encoding.decode(data);
        if !had_errors {
            return Ok(Some(name.to_string()));
        }
    }

    Ok(None)
}

async fn detect_encoding_ogrinfo(shape: &PathBuf) -> Result<Option<String>> {
    let ogrinfo = Command::new("ogrinfo")
        .arg("-json")
        .arg(shape)
        .output()
        .await?;

    if !ogrinfo.status.success() {
        let stderr = String::from_utf8_lossy(&ogrinfo.stderr);
        anyhow::bail!("ogrinfo failed: {}", stderr);
    }

    let stdout_str = String::from_utf8_lossy(&ogrinfo.stdout);
    let json: Value =
        serde_json::from_str(&stdout_str).with_context(|| "when parsing ogrinfo JSON output")?;

    let encoding_path = JsonPath::try_from("$.layers[0].metadata.SHAPEFILE.SOURCE_ENCODING")?;
    let encoding_val = encoding_path.find_slice(&json);
    let encoding_data = encoding_val[0].clone().to_data();

    if let Value::String(encoding) = encoding_data {
        let encoding = encoding.to_string();
        if encoding == "" {
            return Ok(None);
        }
        return Ok(Some(encoding));
    }

    Ok(None)
}

pub async fn detect_encoding(shape: &PathBuf) -> Result<String> {
    let encoding = detect_encoding_ogrinfo(shape).await?;
    if let Some(encoding) = encoding {
        return Ok(encoding);
    }

    if let Some(encoding) = detect_encoding_fallback(shape).await? {
        return Ok(encoding);
    }

    anyhow::bail!("Failed to detect encoding for {}", shape.display());
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_detect_encoding() {
        let shape = std::path::PathBuf::from("./test_data/shp/cp932.shp");
        let encoding = super::detect_encoding(&shape).await.unwrap();
        assert_eq!(encoding, "CP932");

        let shape = std::path::PathBuf::from("./test_data/shp/src_blank.shp");
        let encoding = super::detect_encoding(&shape).await.unwrap();
        assert_eq!(encoding, "CP932");
    }

    #[tokio::test]
    async fn test_get_attribute_list() {
        let shape = std::path::PathBuf::from("./test_data/shp/cp932.shp");
        let attributes = super::get_attribute_list(&shape).await.unwrap();
        assert_eq!(attributes, vec!["W09_001", "W09_002", "W09_003", "W09_004"]);
    }
}
