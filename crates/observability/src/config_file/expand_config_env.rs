use super::{ConfigFileError, FsPath};

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
