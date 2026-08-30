use super::*;

pub(crate) fn format_template_float_round(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let (Ok(value), Ok(precision)) = (args[0].parse::<f64>(), args[1].parse::<i32>()) else {
        return String::new();
    };
    if !value.is_finite() {
        return String::new();
    }
    let round_on = args
        .get(2)
        .map_or(Some(0.5), |value| value.parse::<f64>().ok());
    let Some(round_on) = round_on.filter(|value| value.is_finite()) else {
        return String::new();
    };
    let factor = 10f64.powi(precision);
    if !factor.is_finite() {
        return String::new();
    }
    let shifted = value * factor;
    if !shifted.is_finite() {
        return String::new();
    }
    let rounded = if shifted.is_sign_negative() {
        (shifted - round_on).ceil()
    } else {
        (shifted + round_on).floor()
    } / factor;
    if rounded.is_finite() {
        format_template_float(rounded)
    } else {
        String::new()
    }
}
