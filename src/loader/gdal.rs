use super::mapping::ShapefileMetadata;
use anyhow::Result;
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
        layers.push_str(&format!(
            r#"
                <OGRVRTLayer name="{}">
                <SrcDataSource>{}</SrcDataSource>
                {}
                </OGRVRTLayer>
            "#,
            shape_filename,
            shape.canonicalize().unwrap().to_str().unwrap(),
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
        .arg(postgres_url)
        .arg("-skipfailures")
        .arg("-lco")
        .arg("GEOM_TYPE=geometry")
        .arg("-lco")
        .arg("OVERWRITE=YES")
        .arg("-lco")
        .arg("GEOMETRY_NAME=geom")
        .arg("-nlt")
        .arg("PROMOTE_TO_MULTI")
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
    let output = Command::new("ogrinfo")
        .arg("-if")
        .arg(postgres_url)
        .arg("-sql")
        .arg(&format!("SELECT 1 FROM {} LIMIT 1", layer_name))
        .output()
        .await?;

    Ok(output.status.success())
}
