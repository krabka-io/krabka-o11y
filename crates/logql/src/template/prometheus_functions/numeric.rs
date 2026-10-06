use std::sync::Arc;

use num_traits::ToPrimitive;

use super::super::{
    TemplateRuntimeValue as V, format_template_printf::format_template_printf_values,
    template_time::methods,
};

pub(super) fn evaluate(name: &str, args: &[V]) -> Option<Result<V, String>> {
    if !matches!(
        name,
        "humanize"
            | "humanize1024"
            | "humanizeDuration"
            | "humanizePercentage"
            | "humanizeTimestamp"
            | "toTime"
            | "toDuration"
            | "parseDuration"
    ) {
        return None;
    }
    Some(run(name, &args[0]))
}
fn run(name: &str, arg: &V) -> Result<V, String> {
    if name == "parseDuration" {
        let text = arg
            .string_bytes()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .ok_or("parseDuration requires a string")?;
        let (negative, text) = text
            .strip_prefix('-')
            .map_or((false, text), |text| (true, text));
        let value = if text == "0" {
            0
        } else {
            if text.contains("us") || text.contains("ns") {
                return Err("unknown duration unit".into());
            }
            crate::util::parse_prometheus_duration_literal(text)
                .ok_or("invalid Prometheus duration")?
        };
        return Ok(V::Float(
            if negative {
                -(value.to_f64().expect("i64 fits finite f64"))
            } else {
                value.to_f64().expect("i64 fits finite f64")
            } / 1_000_000_000.0,
        ));
    }
    let mut value = to_float(arg)?;
    Ok(match name {
        "humanize" | "humanize1024" => {
            let mut prefix = "";
            if value != 0.0 && value.is_finite() {
                if name == "humanize" && value.abs() < 1.0 {
                    for next in ["m", "u", "n", "p", "f", "a", "z", "y"] {
                        if value.abs() >= 1.0 {
                            break;
                        }
                        value *= 1000.0;
                        prefix = next;
                    }
                } else {
                    let scale = if name == "humanize" { 1000.0 } else { 1024.0 };
                    let prefixes = if name == "humanize" {
                        ["k", "M", "G", "T", "P", "E", "Z", "Y"]
                    } else {
                        ["ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"]
                    };
                    for next in prefixes {
                        if value.abs() < scale {
                            break;
                        }
                        value /= scale;
                        prefix = next;
                    }
                }
            }
            V::String(format!("{}{prefix}", four(value)))
        }
        "humanizePercentage" => V::String(format!("{}%", four(value * 100.0))),
        "humanizeDuration" => V::String(humanize_duration(value)),
        "toTime" => V::TimePointer(Arc::new(to_time(value)?)),
        "toDuration" => V::DurationPointer(Arc::new(go_integer(value * 1_000_000_000.0))),
        "humanizeTimestamp" => V::String(if value.is_finite() {
            to_time(value)?.as_string()
        } else {
            four(value)
        }),
        _ => unreachable!(),
    })
}
fn to_float(arg: &V) -> Result<f64, String> {
    match arg {
        V::Float(value) => Ok(*value),
        V::Integer(value) | V::Integer64(value) => Ok(value.to_f64().expect("i64 fits finite f64")),
        V::Duration(value) => Ok((*value / 1_000_000_000)
            .to_f64()
            .expect("i64 fits finite f64")
            + (*value % 1_000_000_000)
                .to_f64()
                .expect("i64 fits finite f64")
                / 1_000_000_000.0),
        V::String(_) | V::Bytes(_) | V::Json(serde_json::Value::String(_)) => {
            super::super::template_value::number::parse_float(&arg.as_rendered_string())
                .ok_or("invalid floating-point string".into())
        }
        _ => Err("cannot convert argument to float".into()),
    }
}
fn four(value: f64) -> String {
    format_template_printf_values(&[V::String("%.4g".into()), V::Float(value)])
}
fn go_integer(value: f64) -> i64 {
    if value.is_finite()
        && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value)
    {
        value.to_i64().expect("checked Go integer conversion")
    } else {
        i64::MIN
    }
}
fn to_time(value: f64) -> Result<super::super::TemplateTime, String> {
    let timestamp = value * 1_000_000_000.0;
    if !value.is_finite() {
        return Err("value is NaN or Inf".into());
    }
    if timestamp > i64::MAX.to_f64().expect("i64 fits finite f64")
        || timestamp < i64::MIN.to_f64().expect("i64 fits finite f64")
    {
        return Err("nanoseconds timestamp overflows int64".into());
    }
    // model.TimeFromUnixNano truncates to milliseconds before constructing UTC.
    let nanos = go_integer(timestamp) / 1_000_000 * 1_000_000;
    let V::Time(time) = methods::call(
        &V::Time(super::super::TemplateTime::from_unix_nanos(nanos)),
        "UTC",
        &[],
    )?
    else {
        unreachable!()
    };
    Ok(time)
}
fn humanize_duration(mut value: f64) -> String {
    if !value.is_finite() {
        return four(value);
    }
    if value == 0.0 {
        return format!("{}s", four(value));
    }
    if value.abs() >= 1.0 {
        let sign = if value < 0.0 { "-" } else { "" };
        value = value.abs();
        let duration = go_integer(value);
        let seconds = duration % 60;
        let minutes = duration / 60 % 60;
        let hours = duration / 3600 % 24;
        let days = duration / 86400;
        if days != 0 {
            return format!("{sign}{days}d {hours}h {minutes}m {seconds}s");
        }
        if hours != 0 {
            return format!("{sign}{hours}h {minutes}m {seconds}s");
        }
        if minutes != 0 {
            return format!("{sign}{minutes}m {seconds}s");
        }
        return format!("{sign}{}s", four(value));
    }
    let mut prefix = "";
    for next in ["m", "u", "n", "p", "f", "a", "z", "y"] {
        if value.abs() >= 1.0 {
            break;
        }
        value *= 1000.0;
        prefix = next;
    }
    format!("{}{prefix}s", four(value))
}
