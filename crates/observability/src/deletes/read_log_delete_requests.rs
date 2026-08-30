use super::{CompactorDeleteRequests, ErrorKind, FsPath, LogDeleteRequestStoreError};

pub(crate) fn read_log_delete_requests(
    path: &FsPath,
) -> Result<CompactorDeleteRequests, LogDeleteRequestStoreError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == ErrorKind::NotFound => {
            return Ok(CompactorDeleteRequests::default());
        }
        Err(source) => {
            return Err(LogDeleteRequestStoreError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    serde_json::from_slice(&bytes).map_err(|source| LogDeleteRequestStoreError::Json {
        path: path.to_path_buf(),
        source,
    })
}
