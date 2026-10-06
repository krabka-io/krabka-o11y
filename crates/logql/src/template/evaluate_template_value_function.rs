use super::{
    TemplateRuntimeValue, evaluate_template_index, evaluate_template_slice, format_template_print,
    html_escape_template_string, js_escape_template_string,
};

pub(crate) fn evaluate_template_value_function(
    name: &str,
    args: &[TemplateRuntimeValue],
) -> Option<TemplateRuntimeValue> {
    if let Some(value) = super::evaluate_template_byte_function(name, args) {
        return Some(value);
    }
    let time = match name {
        "now" => Some(super::TemplateTime::now()),
        "unixToTime" => args
            .first()
            .and_then(|value| super::TemplateTime::from_epoch_string(&value.as_rendered_string())),
        "toDate" | "toDateInZone" => Some(
            super::TemplateTime::parse(
                &args[0].as_rendered_string(),
                if name == "toDate" {
                    "Local".to_string()
                } else {
                    args[1].as_rendered_string()
                }
                .as_str(),
                &args[if name == "toDate" { 1 } else { 2 }].as_rendered_string(),
            )
            .unwrap_or_else(super::TemplateTime::zero),
        ),
        _ => None,
    };
    if let Some(time) = time {
        return Some(TemplateRuntimeValue::Time(time));
    }
    if name == "date" {
        let time = match &args[1] {
            TemplateRuntimeValue::Time(time) => time.clone(),
            TemplateRuntimeValue::Integer(seconds) | TemplateRuntimeValue::Integer64(seconds) => {
                super::TemplateTime::from_unix_seconds(*seconds)
            }
            _ => super::TemplateTime::now(),
        };
        return Some(TemplateRuntimeValue::String(
            time.in_local().format(&args[0].as_rendered_string()),
        ));
    }
    if matches!(name, "unixEpoch" | "unixEpochMillis" | "unixEpochNanos") {
        let TemplateRuntimeValue::Time(time) = &args[0] else {
            return None;
        };
        let value = match name {
            "unixEpoch" => time.unix_seconds(),
            "unixEpochMillis" => time.unix_millis(),
            _ => time.unix_nanos(),
        };
        return Some(TemplateRuntimeValue::String(value.to_string()));
    }
    if name == "printf" {
        return Some(TemplateRuntimeValue::Bytes(
            super::format_template_printf_bytes(args),
        ));
    }
    if matches!(name, "eq" | "ne" | "lt" | "le" | "gt" | "ge") {
        return Some(evaluate_template_comparison(name, args));
    }
    if name == "len" {
        let value = match args.first()? {
            TemplateRuntimeValue::String(value)
            | TemplateRuntimeValue::Json(serde_json::Value::String(value)) => value.len(),
            TemplateRuntimeValue::Json(serde_json::Value::Array(value)) => value.len(),
            TemplateRuntimeValue::Json(serde_json::Value::Object(value)) => value.len(),
            TemplateRuntimeValue::Labels(value) => value.len(),
            TemplateRuntimeValue::FloatSlice(value)
            | TemplateRuntimeValue::HistogramSpans(value) => value.len(),
            TemplateRuntimeValue::QueryResult(value) => value.len(),
            TemplateRuntimeValue::Bytes(value) | TemplateRuntimeValue::ByteSlice(value) => {
                value.len()
            }
            TemplateRuntimeValue::ByteLabels(value) => value.len(),
            TemplateRuntimeValue::Object(value) => value.len(),
            TemplateRuntimeValue::Array(value) => value.len(),
            _ => 0,
        };
        return Some(TemplateRuntimeValue::Integer(
            i64::try_from(value).expect("template collection length fits in i64"),
        ));
    }
    if name == "first" {
        let value = args.first()?;
        let first = match value {
            TemplateRuntimeValue::Array(values) => {
                return Some(
                    values
                        .first()
                        .cloned()
                        .unwrap_or(TemplateRuntimeValue::Json(serde_json::Value::Null)),
                );
            }
            TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => {
                values.first().cloned().unwrap_or(serde_json::Value::Null)
            }
            _ => serde_json::Value::Null,
        };
        return Some(TemplateRuntimeValue::Json(first));
    }
    if name == "label" {
        let [name, sample, ..] = args else {
            return Some(TemplateRuntimeValue::String(String::new()));
        };
        let name = name.as_rendered_string();
        if let TemplateRuntimeValue::Object(sample) = sample {
            let labels = sample.get("Labels").or_else(|| sample.get("labels"));
            return Some(
                labels
                    .and_then(|labels| super::template_index_value(labels, &name))
                    .unwrap_or(TemplateRuntimeValue::Json(serde_json::Value::Null)),
            );
        }
        let value = match sample {
            TemplateRuntimeValue::Json(serde_json::Value::Object(sample)) => sample
                .get("Labels")
                .or_else(|| sample.get("labels"))
                .and_then(serde_json::Value::as_object)
                .and_then(|labels| labels.get(&name))
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        };
        return Some(TemplateRuntimeValue::Json(value));
    }
    if name == "value" {
        if let Some(TemplateRuntimeValue::Object(sample)) = args.first() {
            return Some(
                sample
                    .get("Value")
                    .or_else(|| sample.get("value"))
                    .cloned()
                    .unwrap_or(TemplateRuntimeValue::Json(serde_json::Value::Null)),
            );
        }
        let value = match args.first() {
            Some(TemplateRuntimeValue::Json(serde_json::Value::Object(sample))) => sample
                .get("Value")
                .or_else(|| sample.get("value"))
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        };
        return Some(TemplateRuntimeValue::Json(value));
    }
    if name == "fromJson" {
        let Some(value) = args.first() else {
            return Some(TemplateRuntimeValue::String(String::new()));
        };
        let Ok(mut value) = serde_json::from_str::<serde_json::Value>(&value.as_rendered_string())
        else {
            return Some(TemplateRuntimeValue::String(String::new()));
        };
        normalize_json_numbers(&mut value);
        return Some(TemplateRuntimeValue::Json(value));
    }

    if name == "index" {
        return Some(evaluate_template_index(args));
    }
    if name == "slice" {
        return Some(evaluate_template_slice(args));
    }
    if name == "print" {
        return Some(TemplateRuntimeValue::Bytes(
            super::format_template_print_bytes(args, false),
        ));
    }
    if name == "println" {
        return Some(TemplateRuntimeValue::Bytes(
            super::format_template_print_bytes(args, true),
        ));
    }
    if name == "html" {
        return Some(TemplateRuntimeValue::String(html_escape_template_string(
            &format_template_print(args, false),
        )));
    }
    if name == "js" {
        return Some(TemplateRuntimeValue::String(js_escape_template_string(
            &format_template_print(args, false),
        )));
    }
    if name == "default" {
        let fallback = args.first()?.clone();
        return Some(
            args.get(1)
                .filter(|value| value.is_truthy())
                .cloned()
                .unwrap_or(fallback),
        );
    }
    if name == "not" {
        return Some(TemplateRuntimeValue::Json(serde_json::Value::Bool(
            args.first().is_none_or(|value| !value.is_truthy()),
        )));
    }

    None
}

fn evaluate_template_comparison(name: &str, args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let [left, right, ..] = args else {
        return TemplateRuntimeValue::Json(serde_json::Value::Bool(false));
    };
    let compare = |right: &TemplateRuntimeValue| {
        if let (TemplateRuntimeValue::Time(left), TemplateRuntimeValue::Time(right)) = (left, right)
        {
            return Some(if left == right {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Less
            });
        }
        let same_pointer = match (left, right) {
            (TemplateRuntimeValue::TimePointer(left), TemplateRuntimeValue::TimePointer(right)) => {
                Some(std::sync::Arc::ptr_eq(left, right))
            }
            (
                TemplateRuntimeValue::DurationPointer(left),
                TemplateRuntimeValue::DurationPointer(right),
            ) => Some(std::sync::Arc::ptr_eq(left, right)),
            (TemplateRuntimeValue::Sample(left), TemplateRuntimeValue::Sample(right)) => {
                Some(std::sync::Arc::ptr_eq(left, right))
            }
            (
                TemplateRuntimeValue::FloatHistogram(left),
                TemplateRuntimeValue::FloatHistogram(right),
            ) => Some(left.ptr_eq(right)),
            (
                TemplateRuntimeValue::HistogramIterator(left),
                TemplateRuntimeValue::HistogramIterator(right),
            ) => Some(left == right),
            (
                TemplateRuntimeValue::HistogramBucket(left),
                TemplateRuntimeValue::HistogramBucket(right),
            ) => Some(left == right),
            (
                TemplateRuntimeValue::HistogramSpan(left),
                TemplateRuntimeValue::HistogramSpan(right),
            ) => Some(left == right),
            (
                TemplateRuntimeValue::HistogramError(left),
                TemplateRuntimeValue::HistogramError(right),
            ) => Some(std::sync::Arc::ptr_eq(left, right)),
            _ => None,
        };
        if let Some(equal) = same_pointer {
            return Some(if equal {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Less
            });
        }
        let null = |value: &TemplateRuntimeValue| match value {
            TemplateRuntimeValue::FloatSlice(slice)
            | TemplateRuntimeValue::HistogramSpans(slice) => slice.is_nil(),
            TemplateRuntimeValue::Json(serde_json::Value::Null)
            | TemplateRuntimeValue::NilError => true,
            _ => false,
        };
        if null(left) || null(right) {
            return Some(null(left).cmp(&null(right)));
        }
        if left.is_template_string() && right.is_template_string() {
            return Some(left.string_bytes().cmp(&right.string_bytes()));
        }
        if let (Some(left), Some(right)) = (left.signed_integer(), right.signed_integer()) {
            return Some(left.cmp(&right));
        }
        if let (TemplateRuntimeValue::Byte(left), TemplateRuntimeValue::Byte(right)) = (left, right)
        {
            return Some(left.cmp(right));
        }
        if let (Some(left), TemplateRuntimeValue::Byte(right)) = (left.signed_integer(), right) {
            return Some(
                u8::try_from(left).map_or(std::cmp::Ordering::Less, |left| left.cmp(right)),
            );
        }
        if let (TemplateRuntimeValue::Byte(left), Some(right)) = (left, right.signed_integer()) {
            return Some(
                u8::try_from(right).map_or(std::cmp::Ordering::Greater, |right| left.cmp(&right)),
            );
        }
        if let (TemplateRuntimeValue::Complex(lr, li), TemplateRuntimeValue::Complex(rr, ri)) =
            (left, right)
        {
            return Some(
                if lr.partial_cmp(rr) == Some(std::cmp::Ordering::Equal)
                    && li.partial_cmp(ri) == Some(std::cmp::Ordering::Equal)
                {
                    std::cmp::Ordering::Equal
                } else {
                    std::cmp::Ordering::Less
                },
            );
        }
        let number = |value: &TemplateRuntimeValue| match value {
            TemplateRuntimeValue::Float(value) => Some(*value),
            TemplateRuntimeValue::Json(serde_json::Value::Number(value)) => value.as_f64(),
            _ => None,
        };
        if let (Some(left), Some(right)) = (number(left), number(right)) {
            return left.partial_cmp(&right);
        }
        if let (
            TemplateRuntimeValue::Json(serde_json::Value::Bool(left)),
            TemplateRuntimeValue::Json(serde_json::Value::Bool(right)),
        ) = (left, right)
        {
            return Some(left.cmp(right));
        }
        None
    };
    let value = match name {
        "eq" => args[1..]
            .iter()
            .any(|right| compare(right).is_some_and(std::cmp::Ordering::is_eq)),
        "ne" => compare(right).is_none_or(std::cmp::Ordering::is_ne),
        "lt" => compare(right).is_some_and(std::cmp::Ordering::is_lt),
        "le" => compare(right).is_some_and(std::cmp::Ordering::is_le),
        "gt" => compare(right).is_some_and(std::cmp::Ordering::is_gt),
        "ge" => compare(right).is_some_and(std::cmp::Ordering::is_ge),
        _ => false,
    };
    TemplateRuntimeValue::Json(serde_json::Value::Bool(value))
}

// Go encoding/json unmarshals every JSON number to float64.
fn normalize_json_numbers(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Number(number) => {
            if let Some(number) = number.as_f64().and_then(serde_json::Number::from_f64) {
                *value = serde_json::Value::Number(number);
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(normalize_json_numbers),
        serde_json::Value::Object(values) => values.values_mut().for_each(normalize_json_numbers),
        _ => {}
    }
}
