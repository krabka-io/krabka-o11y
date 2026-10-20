use std::str::FromStr;

use bigdecimal::{BigDecimal, RoundingMode};
use num_traits::ToPrimitive;

pub(crate) fn format_template_float(value: f64) -> String {
    if value == f64::INFINITY {
        return "+Inf".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".to_string();
    }
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value != 0.0 && (value.abs() < 0.0001 || value.abs() >= 1_000_000.0) {
        let text = format!("{value:e}");
        let exponent_separator = text.find('e').expect("scientific float has exponent");
        let mantissa = &text[..exponent_separator];
        let exponent: i32 = text[exponent_separator + 1..]
            .parse()
            .expect("float exponent fits in i32");
        return format!("{mantissa}e{exponent:+03}");
    }
    value.to_string()
}

pub(crate) fn format_template_float_fold(name: &str, args: &[String]) -> String {
    let mut values = args.iter().map(|value| {
        let value = value.parse::<f64>().unwrap_or_default();
        BigDecimal::from_str(&value.to_string()).unwrap_or_default()
    });
    let initial = if name == "addf" {
        BigDecimal::from(0)
    } else {
        values.next().unwrap_or_default()
    };
    let value = values.fold(initial, |left, right| match name {
        "addf" => left + right,
        "subf" => left - right,
        "mulf" => left * right,
        "divf" => (left / right).with_scale_round(16, RoundingMode::HalfUp),
        _ => unreachable!("decimal operation was validated"),
    });
    format_template_float(value.to_f64().unwrap_or_default())
}
