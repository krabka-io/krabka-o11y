use super::format_template_float;

pub(crate) fn format_template_float_round(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let value = args[0].parse::<f64>().unwrap_or_default();
    let precision = args[1].parse::<f64>().unwrap_or_default();
    let round_on = args
        .get(2)
        .map_or(0.5, |value| value.parse::<f64>().unwrap_or_default());
    let factor = 10f64.powf(precision);
    let shifted = value * factor;
    let rounded = if shifted.fract() >= round_on {
        shifted.ceil()
    } else {
        shifted.floor()
    } / factor;
    format_template_float(rounded)
}
