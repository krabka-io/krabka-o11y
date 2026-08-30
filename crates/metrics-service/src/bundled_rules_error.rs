use super::{PathBuf, StatusCode};

/// Errors that stop the ruler from installing a bundled rule file.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BundledRulesError {
    #[error("bundled rule file `{path}` is unreadable: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("bundled rule file `{path}` is not a Prometheus rule file: {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("bundled rule file `{path}` holds no rule group")]
    NoGroups { path: PathBuf },

    #[error("bundled rule file `{path}` has no file stem to name the rule namespace")]
    NoNamespace { path: PathBuf },

    #[error("bundled rule group `{group}` does not encode back to YAML: {source}")]
    Encode {
        group: String,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("the ruler config request for bundled rule group `{group}` is not valid: {source}")]
    Request {
        group: String,
        #[source]
        source: axum::http::Error,
    },

    #[error("the ruler config API rejected bundled rule group `{group}`: HTTP {status}, {body}")]
    Rejected {
        group: String,
        status: StatusCode,
        body: String,
    },

    #[error("the ruler config response for bundled rule group `{group}` is unreadable: {source}")]
    ResponseBody {
        group: String,
        #[source]
        source: axum::Error,
    },
}
