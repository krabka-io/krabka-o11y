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

    /// The internal client credentials do not make a usable HTTP client.
    #[error("the HTTP client for the ruler config API does not build: {source}")]
    Client {
        #[source]
        source: reqwest::Error,
    },

    /// The URL of the ruler's own listener does not parse.
    #[error("the ruler config URL `{address}` is not valid: {source}")]
    Url {
        address: String,
        #[source]
        source: url::ParseError,
    },

    /// The request did not reach the ruler's listener, or got no response.
    #[error("the ruler config request for bundled rule group `{group}` failed: {source}")]
    Send {
        group: String,
        #[source]
        source: reqwest::Error,
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
        source: reqwest::Error,
    },
}
