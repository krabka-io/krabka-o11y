pub(crate) fn accept_parameter_is_zero_quality(parameter: &str) -> bool {
    let Some(parts) = parameter.trim().split_once('=') else {
        return false;
    };
    if !parts.0.trim().eq_ignore_ascii_case("q") {
        return false;
    }

    parts
        .1
        .trim()
        .parse::<f32>()
        .is_ok_and(|quality| quality <= 0.0)
}
