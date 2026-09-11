use std::path::Path;

use super::ServerSecurityError;

/// Reads a whole file, with an error that names it.
pub fn read_file(path: &Path) -> Result<Vec<u8>, ServerSecurityError> {
    std::fs::read(path).map_err(|source| ServerSecurityError::ReadFile {
        path: path.to_owned(),
        source,
    })
}
