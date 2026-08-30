use super::*;

pub(crate) fn yaml_duration_ns_field(
    fields: &serde_yaml::Mapping,
    name: &'static str,
) -> Option<i64> {
    let duration = yaml_string_field(fields, name)?;
    parse_prometheus_duration(duration)
}
