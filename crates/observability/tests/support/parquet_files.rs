//! Finding the block files a store wrote on local disk.

use std::path::{Path, PathBuf};

/// Every `.parquet` file under `dir`, in directory-walk order.
pub fn parquet_files_under(dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, found: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                found.push(path);
            }
        }
    }

    let mut found = Vec::new();
    walk(dir, &mut found);
    found
}
