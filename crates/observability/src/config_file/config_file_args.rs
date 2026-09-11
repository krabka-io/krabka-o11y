use super::{Parser, PathBuf};

/// The config-file arguments every Krabka service binary flattens.
///
/// Flattening this into a binary's `Cli` is what makes
/// [`argv_with_config_file`](super::argv_with_config_file) work for it: the
/// helper reads `--config.file` back out of the first parse, so the file path
/// itself obeys the same flag-over-environment rule as everything the file
/// sets.
#[derive(Clone, Debug, Default, Eq, Parser, PartialEq)]
pub struct ConfigFileArgs {
    /// YAML file whose keys name this binary's long flags.
    ///
    /// A flag or an environment variable overrides the value the file gives
    /// for the same key.
    #[arg(
        long = "config.file",
        alias = "config-file",
        env = "KRABKA_CONFIG_FILE",
        value_name = "PATH"
    )]
    pub config_file: Option<PathBuf>,

    /// Expand `${VAR}` and `${VAR:default}` in the config file before reading
    /// it, so a secret reaches the process through the environment and the
    /// file stays reviewable.
    #[arg(
        long = "config.expand-env",
        alias = "config-expand-env",
        env = "KRABKA_CONFIG_EXPAND_ENV"
    )]
    pub config_expand_env: bool,
}
