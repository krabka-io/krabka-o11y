pub(crate) fn proto_param_value(param: &str) -> Option<String> {
    let pair = param.trim().split_once('=')?;
    pair.0
        .trim()
        .eq_ignore_ascii_case("proto")
        .then(|| pair.1.trim().trim_matches('"').to_string())
}
