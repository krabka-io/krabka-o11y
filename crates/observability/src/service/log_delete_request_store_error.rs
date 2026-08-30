use super::{Error, PathBuf};

#[derive(Debug, Error)]
pub enum LogDeleteRequestStoreError {
    #[error("delete request store I/O error for {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("delete request store JSON error for {path:?}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}
