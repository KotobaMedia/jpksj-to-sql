use super::mapping::ShapefileMetadata;
use anyhow::Result;
use encoding_rs::{Encoding, SHIFT_JIS, UTF_8};
use std::path::PathBuf;
use tokio::process::Command;

pub async fn create_vrt(
    out: &PathBuf,
    shapes: &Vec<PathBuf>,
    metadata: &ShapefileMetadata,
) -> Result<()> {
    let bare_vrt = out.with_extension("");
    let layer_name = bare_vrt.file_name().unwrap().to_str().unwrap();
    // let vrt_path = shape.with_extension("vrt");

    let mut fields = String::new();
    for (field_name, shape_name) in metadata.field_mappings.iter() {
        fields.push_str(&format!(
            r#"<Field name="{}" src="{}" />"#,
            field_name, shape_name
        ));
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
    let output = Command::new("ogr2ogr")
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

// PC932 is almost the same as Shift-JIS, but most GIS software outputs as CP932 when using Shift-JIS
static ENCODINGS: &[(&str, &Encoding)] = &[("CP932", SHIFT_JIS), ("UTF-8", UTF_8)];

pub async fn detect_encoding(shape: &PathBuf) -> Result<&str> {
    let ogrinfo = Command::new("ogrinfo")
        .arg("-al")
        .arg("-geom=NO")
        .arg("-limit")
        .arg("100")
        .arg(shape)
        .output()
        .await?;

    if !ogrinfo.status.success() {
        anyhow::bail!("ogrinfo failed");
    }

    let data = &ogrinfo.stdout;
    for (name, encoding) in ENCODINGS {
        // decode() returns a tuple: (decoded string, bytes read, had_errors)
        let (_decoded, _, had_errors) = encoding.decode(data);
        if !had_errors {
            return Ok(name);
        }
    }

    anyhow::bail!("Could not detect encoding for {}", shape.display());
}
