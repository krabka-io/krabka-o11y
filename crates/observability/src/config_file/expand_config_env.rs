use super::{ConfigFileError, FsPath};

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
        let (name, default) = match tail[..close].split_once(':') {
            // `${VAR:default}` is Loki's spelling and `${VAR:-default}` is the
            // shell's. Both reach the same place, so a config file copied from
            // either habit behaves the way it reads.
            Some((name, default)) => (name, Some(default.strip_prefix('-').unwrap_or(default))),
            None => (&tail[..close], None),
        };
        let value = lookup(name)
            .or_else(|| default.map(str::to_owned))
            .ok_or_else(|| ConfigFileError::UndefinedVariable {
                path: path.to_path_buf(),
                name: name.to_owned(),
            })?;
        expanded.push_str(&value);
        rest = &tail[close + 1..];
    }
    expanded.push_str(rest);
    Ok(expanded)
}
