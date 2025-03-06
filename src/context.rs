use std::{path::PathBuf, sync::OnceLock};

fn default_tmp() -> PathBuf {
    PathBuf::from("./tmp")
}

static TMP: OnceLock<PathBuf> = OnceLock::new();
pub fn set_tmp(tmp: PathBuf) {
    TMP.set(tmp).unwrap();
}
pub fn tmp() -> &'static PathBuf {
    TMP.get_or_init(|| default_tmp())
}
