use std::{path::PathBuf, sync::OnceLock};

static TMP: OnceLock<PathBuf> = OnceLock::new();
pub fn set_tmp(tmp: PathBuf) {
    TMP.set(tmp).unwrap();
}
pub fn tmp() -> &'static PathBuf {
    TMP.get().unwrap()
}
