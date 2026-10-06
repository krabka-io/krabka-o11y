use std::str::FromStr;

use bigdecimal::{BigDecimal, RoundingMode};
use num_traits::ToPrimitive;

use super::format_template_float;

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
