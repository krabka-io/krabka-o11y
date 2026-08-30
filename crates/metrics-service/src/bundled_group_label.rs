/// Names one group of a bundled rule file for an error message.
///
/// The ruler config API rejects a group that carries no name. The position in
/// the file names that group instead, so an operator can find it.
pub(crate) fn bundled_group_label(index: usize, group: &serde_yaml::Value) -> String {
    group
        .get("name")
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
        .map_or_else(|| format!("#{index}"), str::to_string)
}
