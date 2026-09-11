use std::path::Path;

use super::{ServerSecurityError, credentials::Credentials, read_file::read_file};

/// Reads and validates the credentials file at `path`.
pub fn load_credentials(path: &Path) -> Result<Credentials, ServerSecurityError> {
    Credentials::from_yaml(&read_file(path)?).map_err(|source| ServerSecurityError::Credentials {
        path: path.to_owned(),
        source,
    })
}
