pub(crate) fn accept_parameter_is_zero_quality(parameter: &str) -> bool {
    let Some((name, value)) = parameter.trim().split_once('=') else {
        return false;
    };
    if !name.trim().eq_ignore_ascii_case("q") {
        return false;
    }

    value
        .trim()
        .parse::<f32>()
        .is_ok_and(|quality| quality <= 0.0)
}
