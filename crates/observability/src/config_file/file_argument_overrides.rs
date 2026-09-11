use super::{
    ArgAction, ArgMatches, Command, ConfigFileError, FsPath, OsString, ValueSource, YamlValue,
    yaml_scalar,
};

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
    for (key, value) in mapping {
        let key = key.as_str().ok_or_else(|| ConfigFileError::NonStringKey {
            path: path.to_path_buf(),
            key: format!("{key:?}"),
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
