//! A YAML configuration file for the service binaries.
//!
//! Five binaries in this workspace carry between twenty and sixty flags, each
//! with an environment binding. At that size a deployment is a wall of
//! `KRABKA_*` variables that nothing can review, diff or check into a
//! repository, so this module lets one file stand in for the flags.
//!
//! The file needs no schema of its own. Its keys are the binary's own long
//! flag names, and its values go back through the same `clap` value parsers
//! the flags use, so there is one definition of the configuration and it
//! cannot drift from the one `--help` prints. `listen_addr` and `listen-addr`
//! both name `--listen-addr`; a key that names no flag is an error rather than
//! a line the process ignores.
//!
//! ## Precedence
//!
//! Highest wins:
//!
//! 1. the command line,
//! 2. the environment,
//! 3. the config file,
//! 4. the flag's default.
//!
//! This is the order Loki, Mimir and Tempo use, and the order an operator
//! expects: the file is the reviewed baseline, and a flag is the deliberate
//! override in front of it. [`argv_with_config_file`] enforces it by asking
//! `clap` where each value came from and supplying a file value only for the
//! arguments that neither the command line nor the environment set.

use crate::{
    ArgAction, ArgMatches, Command, CommandFactory, Error, FsPath, OsString, Parser, PathBuf,
    ValueSource, YamlValue,
};

#[cfg(test)]
mod tests;


/// Renders one YAML scalar the way the command line would have spelled it.
///
/// The rendered text goes straight back through the argument's own
/// `value_parser`, so `1m`, `32MiB` and `0.0.0.0:3100` mean in the file
/// exactly what they mean on the command line.
pub(crate) fn yaml_scalar(
    path: &FsPath,
    key: &str,
    value: &YamlValue,
) -> Result<String, ConfigFileError> {
    match value {
        YamlValue::String(text) => Ok(text.clone()),
        YamlValue::Bool(flag) => Ok(flag.to_string()),
        YamlValue::Number(number) => Ok(number.to_string()),
        _ => Err(ConfigFileError::UnsupportedValue {
            path: path.to_path_buf(),
            key: key.to_owned(),
            wanted: "a string, a number or a boolean",
        }),
    }
}

/// Rewrites `argv` so that a `--config.file` supplies the arguments nothing
/// else set.
///
/// Call it in place of [`std::env::args_os`] and hand the result to
/// `Cli::parse_from`. `C` is the binary's own `clap` command, so the file is
/// checked against the flags that binary actually has -- an unknown key stops
/// start-up instead of being dropped.
///
/// The file's arguments go in immediately after the program name, ahead of
/// everything the caller passed, so a `--` on the real command line still ends
/// the options it was written to end.
///
/// # Errors
/// Returns an error when the file cannot be read or parsed, when a key names
/// no flag of `C`, or when a value is not a kind a flag can take.
pub fn argv_with_config_file<C>(
    argv: impl IntoIterator<Item = OsString>,
) -> Result<Vec<OsString>, ConfigFileError>
where
    C: CommandFactory,
{
    let mut argv: Vec<OsString> = argv.into_iter().collect();
    let command = C::command();
    // A tolerant first pass. It exists only to learn which arguments the
    // command line and the environment already supplied, so every error it
    // could raise is the real parse's to report with the real message -- and
    // an argument the file is about to supply is still `required` here, which
    // is why the requirement is lifted for this pass alone.
    let Ok(matches) = command
        .clone()
        .ignore_errors(true)
        .mut_args(|argument| argument.required(false))
        .try_get_matches_from(argv.clone())
    else {
        return Ok(argv);
    };
    let Ok(Some(path)) = matches.try_get_one::<PathBuf>("config_file") else {
        return Ok(argv);
    };
    let path = path.clone();

    let text = std::fs::read_to_string(&path).map_err(|source| ConfigFileError::Read {
        path: path.clone(),
        source,
    })?;
    let text = if matches
        .try_get_one::<bool>("config_expand_env")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false)
    {
        expand_config_env(&path, &text, &|name| std::env::var(name).ok())?
    } else {
        text
    };
    let document: YamlValue =
        serde_yaml::from_str(&text).map_err(|source| ConfigFileError::Parse {
            path: path.clone(),
            source,
        })?;

    let from_file = file_argument_overrides(&command, &matches, &path, &document)?;
    let at = usize::from(!argv.is_empty());
    argv.splice(at..at, from_file);
    Ok(argv)
}

/// The config-file arguments every Krabka service binary flattens.
///
/// Flattening this into a binary's `Cli` is what makes
/// [`argv_with_config_file`](argv_with_config_file) work for it: the
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

struct ConfigExpansion {
    name: String,
    default: Option<String>,
}

/// Substitutes `${VAR}` and `${VAR:default}` from the environment.
///
/// `--config.expand-env` turns this on, as Loki's `-config.expand-env` does.
/// A reference with neither a value nor a default is an error: the alternative
/// is a process that starts with an empty password or an empty bucket name and
/// fails much later, somewhere that does not mention the config file.
pub(crate) fn expand_config_env(
    path: &FsPath,
    text: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<String, ConfigFileError> {
    let mut expanded = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find("${") {
        expanded.push_str(&rest[..open]);
        let tail = &rest[open + 2..];
        let close = tail
            .find('}')
            .ok_or_else(|| ConfigFileError::UnterminatedExpansion {
                path: path.to_path_buf(),
            })?;
        // `${VAR:default}` is Loki's spelling and `${VAR:-default}` is the
        // shell's. Both reach the same place, so a config file copied from
        // either habit behaves the way it reads.
        let variable = &tail[..close];
        let expansion = if let Some(separator) = variable.find(':') {
            let default = &variable[separator + 1..];
            ConfigExpansion {
                name: variable[..separator].to_string(),
                default: Some(default.strip_prefix('-').unwrap_or(default).to_string()),
            }
        } else {
            ConfigExpansion {
                name: variable.to_string(),
                default: None,
            }
        };
        let value = lookup(&expansion.name)
            .or(expansion.default)
            .ok_or_else(|| ConfigFileError::UndefinedVariable {
                path: path.to_path_buf(),
                name: expansion.name,
            })?;
        expanded.push_str(&value);
        rest = &tail[close + 1..];
    }
    expanded.push_str(rest);
    Ok(expanded)
}

/// Turns a parsed config file into the arguments the file is still entitled to
/// set.
///
/// `matches` is a first, error-tolerant parse of the real command line. An
/// argument it reports as coming from [`ValueSource::CommandLine`] or
/// [`ValueSource::EnvVariable`] is skipped here, which is the whole of the
/// precedence rule: the file only fills in what nothing louder already said.
pub(crate) fn file_argument_overrides(
    command: &Command,
    matches: &ArgMatches,
    path: &FsPath,
    document: &YamlValue,
) -> Result<Vec<OsString>, ConfigFileError> {
    let mapping = match document {
        // An empty file is a file that sets nothing, not a malformed one.
        YamlValue::Null => return Ok(Vec::new()),
        YamlValue::Mapping(mapping) => mapping,
        _ => {
            return Err(ConfigFileError::NotAMapping {
                path: path.to_path_buf(),
            });
        }
    };

    let mut arguments = Vec::new();
    for entry in mapping {
        let value = entry.1;
        let key = entry
            .0
            .as_str()
            .ok_or_else(|| ConfigFileError::NonStringKey {
                path: path.to_path_buf(),
                key: format!("{:?}", entry.0),
            })?;
        let wanted = key.replace('_', "-");
        if wanted == "config.file" || wanted == "config.expand-env" {
            return Err(ConfigFileError::SelfReferentialKey {
                path: path.to_path_buf(),
                key: key.to_owned(),
            });
        }
        let argument = command
            .get_arguments()
            .find(|argument| {
                argument
                    .get_long()
                    .is_some_and(|long| long.replace('_', "-") == wanted)
                    || argument.get_id().as_str().replace('_', "-") == wanted
            })
            .ok_or_else(|| ConfigFileError::UnknownKey {
                path: path.to_path_buf(),
                key: key.to_owned(),
            })?;
        let Some(long) = argument.get_long() else {
            return Err(ConfigFileError::PositionalKey {
                path: path.to_path_buf(),
                key: key.to_owned(),
            });
        };
        if matches!(
            matches.value_source(argument.get_id().as_str()),
            Some(ValueSource::CommandLine | ValueSource::EnvVariable)
        ) {
            continue;
        }

        let flag = format!("--{long}");
        match argument.get_action() {
            ArgAction::SetTrue | ArgAction::SetFalse => {
                let wanted_state = matches!(argument.get_action(), ArgAction::SetTrue);
                let stated = value
                    .as_bool()
                    .ok_or_else(|| ConfigFileError::UnsupportedValue {
                        path: path.to_path_buf(),
                        key: key.to_owned(),
                        wanted: "a boolean",
                    })?;
                // A switch is present or absent on a command line; `false` for
                // a `SetTrue` switch is the default, so it contributes nothing.
                if stated == wanted_state {
                    arguments.push(OsString::from(flag));
                }
            }
            ArgAction::Count => {
                let count = value
                    .as_u64()
                    .ok_or_else(|| ConfigFileError::UnsupportedValue {
                        path: path.to_path_buf(),
                        key: key.to_owned(),
                        wanted: "a whole number",
                    })?;
                for _ in 0..count {
                    arguments.push(OsString::from(flag.clone()));
                }
            }
            _ => match value {
                YamlValue::Sequence(items) => {
                    for item in items {
                        arguments.push(OsString::from(format!(
                            "{flag}={}",
                            yaml_scalar(path, key, item)?
                        )));
                    }
                }
                scalar => arguments.push(OsString::from(format!(
                    "{flag}={}",
                    yaml_scalar(path, key, scalar)?
                ))),
            },
        }
    }
    Ok(arguments)
}
