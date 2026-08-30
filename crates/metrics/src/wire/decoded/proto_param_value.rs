
pub(crate) fn proto_param_value(param: &str) -> Option<String> {
    let (name, value) = param.trim().split_once('=')?;
    name.trim()
        .eq_ignore_ascii_case("proto")
        .then(|| value.trim().trim_matches('"').to_string())
}
