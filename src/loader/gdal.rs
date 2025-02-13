use super::mapping::ShapefileMetadata;
use anyhow::Result;
use std::path::PathBuf;

pub async fn create_vrt(shape: &PathBuf, metadata: &ShapefileMetadata) -> Result<PathBuf> {
    let bare_shape = shape.with_extension("");
    let shape_filename = bare_shape.file_name().unwrap().to_str().unwrap();
    let vrt_path = shape.with_extension("vrt");

    let mut fields = String::new();
    for (field_name, shape_name) in metadata.field_mappings.iter() {
        fields.push_str(&format!(
            r#"
            <Field name="{}" src="{}" />
        "#,
            field_name, shape_name
        ));
    }

    let vrt = format!(
        r#"
        <OGRVRTDataSource>
        <OGRVRTLayer name="{}">
        <SrcDataSource>{}</SrcDataSource>
        {}
        </OGRVRTLayer>
        </OGRVRTDataSource>"
    "#,
        shape_filename,
        shape.to_str().unwrap(),
        fields,
    );

    tokio::fs::write(&vrt_path, vrt).await?;

    Ok(vrt_path)
}

pub async fn load_to_postgres(vrt: &PathBuf, postgres_url: &str) -> Result<()> {
    let output = tokio::process::Command::new("ogr2ogr")
        .arg("-f")
        .arg("PostgreSQL")
        .arg(postgres_url)
        .arg("-lco")
        .arg("GEOM_TYPE=geometry")
        .arg("-lco")
        .arg("OVERWRITE=YES")
        .arg("-lco")
        .arg("GEOMETRY_NAME=geom")
        .arg(vrt)
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)?;
        anyhow::bail!("ogr2ogr failed: {}", stderr);
    }

    Ok(())
}
