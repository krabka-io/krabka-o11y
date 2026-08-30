use super::*;

#[derive(Debug, Error)]
pub enum LokiRuleStoreError {
    #[error("Loki rule store I/O error for {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Loki rule store JSON error for {path:?}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}
