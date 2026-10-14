pub(crate) fn sanitize_json_field_name(name: &str) -> String {
    super::sanitize_field_name(name, false)
}
