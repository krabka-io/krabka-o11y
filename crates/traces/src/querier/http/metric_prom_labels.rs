use std::collections::BTreeMap;

use krabka_traceql::{MetricLabel, TraceMetricLabelType};

/// Tempo's `Labels.String`: sorted verbatim names and typed display values.
pub(crate) fn metric_prom_labels(
    labels: &[MetricLabel],
    types: &BTreeMap<String, TraceMetricLabelType>,
) -> String {
    let mut labels = labels.iter().collect::<Vec<_>>();
    labels.sort_by(|left, right| left.key.cmp(&right.key));
    let inner = labels
        .into_iter()
        .map(|label| {
            let key = &label.key;
            let value = &label.value;
            let kind = types.get(key).copied().or_else(|| {
                matches!(key.as_str(), "p" | "__bucket").then_some(TraceMetricLabelType::Double)
            });
            let value = match kind {
                Some(TraceMetricLabelType::Int) => format!("int({value})"),
                Some(TraceMetricLabelType::Double) => {
                    let mut text = float_label(value.parse().expect("metric double is f64"));
                    if !text.contains(['e', '.']) {
                        text.push_str(".0");
                    }
                    text
                }
                Some(TraceMetricLabelType::Array) => array_label(value),
                Some(TraceMetricLabelType::Bool) => value.clone(),
                Some(TraceMetricLabelType::Nil) => "<nil>".into(),
                None => match value.as_str() {
                    "nil" => "<nil>".into(),
                    "" => "<empty>".into(),
                    _ => value.clone(),
                },
            };
            let mut characters = key.chars();
            let valid = characters
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && characters.all(|c| c.is_ascii_alphanumeric() || c == '_');
            let name = if valid { key.clone() } else { quote_label(key) };
            format!("{name}={}", quote_label(&value))
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{inner}}}")
}

fn array_label(value: &str) -> String {
    let array: serde_json::Value = serde_json::from_str(value).expect("array label is OTLP JSON");
    let values = array["arrayValue"]["values"]
        .as_array()
        .expect("array label has values");
    let values = values
        .iter()
        .map(|value| {
            if let Some(value) = value.get("stringValue").and_then(serde_json::Value::as_str) {
                format!("`{value}`")
            } else if let Some(value) = value.get("intValue").and_then(serde_json::Value::as_str) {
                value.to_string()
            } else if let Some(value) = value.get("boolValue").and_then(serde_json::Value::as_bool)
            {
                value.to_string()
            } else {
                let value = &value["doubleValue"];
                float_label(value.as_f64().unwrap_or_else(|| match value.as_str() {
                    Some("NaN") => f64::NAN,
                    Some("Infinity") => f64::INFINITY,
                    Some("-Infinity") => f64::NEG_INFINITY,
                    _ => panic!("array double is f64"),
                }))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

// Go strconv.FormatFloat('g', -1, 64) uses exponent form outside [-4, 6).
fn float_label(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Inf"
        } else {
            "+Inf"
        }
        .into();
    }
    let exponent = format!("{value:e}");
    let separator = exponent.find('e').expect("float exponent has e");
    let mantissa = &exponent[..separator];
    let power = &exponent[separator + 1..];
    let power = power.parse::<i32>().expect("float exponent is integer");
    if (-4..6).contains(&power) {
        value.to_string()
    } else {
        format!("{mantissa}e{power:+03}")
    }
}

fn quote_label(value: &str) -> String {
    static NONPRINTABLE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    // Go strconv.IsPrint admits Unicode L/M/N/P/S and ASCII space.
    let nonprintable = NONPRINTABLE
        .get_or_init(|| regex::Regex::new(r"[\p{C}\p{Z}]").expect("static Unicode pattern"));
    let mut output = String::from("\"");
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{7}' => output.push_str("\\a"),
            '\u{8}' => output.push_str("\\b"),
            '\u{c}' => output.push_str("\\f"),
            '\u{b}' => output.push_str("\\v"),
            c if c.is_ascii_control() => {
                use std::fmt::Write as _;
                write!(output, "\\x{:02x}", u32::from(c)).expect("string writes are infallible");
            }
            c if c != ' ' && nonprintable.is_match(c.encode_utf8(&mut [0; 4])) => {
                use std::fmt::Write as _;
                let number = u32::from(c);
                if number < 256 {
                    write!(output, "\\x{number:02x}")
                } else if number < 65536 {
                    write!(output, "\\u{number:04x}")
                } else {
                    write!(output, "\\U{number:08x}")
                }
                .expect("string writes are infallible");
            }
            c => output.push(c),
        }
    }
    output.push('"');
    output
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_traceql::MetricLabel;

    use super::{BTreeMap, TraceMetricLabelType, float_label, metric_prom_labels, quote_label};

    struct FloatLabelCase {
        value: f64,
        expected: &'static str,
    }

    #[test]
    fn legends_preserve_names_types_missing_values_and_go_escaping() {
        let labels = vec![
            MetricLabel::new("span.value", "1"),
            MetricLabel::new("span:duration", "1ms"),
            MetricLabel::new("empty", ""),
            MetricLabel::new("absent", "nil"),
            MetricLabel::new("text", "a\"\\\n\0"),
            MetricLabel::new("float", "1"),
        ];
        let mut types = BTreeMap::new();
        types.insert("span.value".into(), TraceMetricLabelType::Int);
        types.insert("float".into(), TraceMetricLabelType::Double);
        assert!(
            metric_prom_labels(&labels, &types)
                == r#"{absent="<nil>", empty="<empty>", float="1.0", "span.value"="int(1)", "span:duration"="1ms", text="a\"\\\n\x00"}"#
        );
        let mut strings = types.clone();
        strings.remove("span.value");
        assert!(metric_prom_labels(&labels, &strings) != metric_prom_labels(&labels, &types));
    }

    #[test]
    fn quotes_follow_go_unicode_printability() {
        assert!(
            quote_label("café \u{a0}\u{2028}\u{e000}\u{1d173}")
                == r#""café \xa0\u2028\ue000\U0001d173""#
        );
    }

    #[test]
    fn float_and_array_legends_follow_pinned_static_formatting() {
        for case in [
            FloatLabelCase {
                value: 1e6,
                expected: "1e+06",
            },
            FloatLabelCase {
                value: 1e-5,
                expected: "1e-05",
            },
            FloatLabelCase {
                value: 0.0001,
                expected: "0.0001",
            },
            FloatLabelCase {
                value: -0.0,
                expected: "-0",
            },
            FloatLabelCase {
                value: f64::INFINITY,
                expected: "+Inf",
            },
        ] {
            assert!(float_label(case.value) == case.expected);
        }
        let array =
            serde_json::json!({"arrayValue":{"values":[{"intValue":"2"},{"intValue":"4"}]}})
                .to_string();
        assert!(
            metric_prom_labels(&[MetricLabel::new("span.numbers", array)], &{
                let mut types = BTreeMap::new();
                types.insert("span.numbers".into(), TraceMetricLabelType::Array);
                types
            }) == r#"{"span.numbers"="[2, 4]"}"#
        );
    }
}
