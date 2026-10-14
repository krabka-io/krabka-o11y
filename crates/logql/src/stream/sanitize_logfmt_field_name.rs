pub(crate) fn sanitize_logfmt_field_name(name: &str) -> String {
    super::sanitize_field_name(name, true)
}
