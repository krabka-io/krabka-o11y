use super::yaml_duration_ns_field;

pub(crate) fn yaml_duration_seconds_field(
    fields: &serde_yaml::Mapping,
    name: &'static str,
) -> Option<i64> {
    yaml_duration_ns_field(fields, name)
        .and_then(|duration_ns| duration_ns.checked_div(1_000_000_000))
}
