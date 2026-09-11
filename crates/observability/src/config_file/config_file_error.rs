use super::{Error, PathBuf};

/// Why a `--config.file` could not be turned into arguments.
///
/// Every variant names the file and the key at fault. A config file that a
/// process reads and silently half-applies is the failure this whole module
/// exists to prevent, so nothing here is a warning.
#[derive(Debug, Error)]
pub enum ConfigFileError {
    #[error("cannot read config file {}: {source}", path.display())]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot parse config file {}: {source}", path.display())]
    Parse {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    #[error("config file {} must be a mapping of flag names to values", path.display())]
    NotAMapping { path: PathBuf },
    #[error("config file {} has a key that is not a name: {key}", path.display())]
    NonStringKey { path: PathBuf, key: String },
    #[error("config file {} sets `{key}`, which is not a flag of this binary", path.display())]
    UnknownKey { path: PathBuf, key: String },
    #[error("config file {} sets `{key}`, which is a positional argument", path.display())]
    PositionalKey { path: PathBuf, key: String },
    #[error("config file {} needs {wanted} for `{key}`", path.display())]
    UnsupportedValue {
        path: PathBuf,
        key: String,
        wanted: &'static str,
    },
    #[error("config file {} sets `{key}`, which only the command line or the environment can set", path.display())]
    SelfReferentialKey { path: PathBuf, key: String },
    #[error("config file {} has a `${{` that is never closed", path.display())]
    UnterminatedExpansion { path: PathBuf },
    #[error("config file {} expands `${{{name}}}`, which is unset and has no default", path.display())]
    UndefinedVariable { path: PathBuf, name: String },
}
