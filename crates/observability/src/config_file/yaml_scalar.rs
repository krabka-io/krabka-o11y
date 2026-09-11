use super::{ConfigFileError, FsPath, YamlValue};

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
