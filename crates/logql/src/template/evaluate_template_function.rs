use super::{
    NoExpand, Regex, TemplateRuntimeValue, epoch_template_timestamp,
    evaluate_common_string_function, evaluate_template_value_function, format_template_bytes,
    format_template_date, format_template_duration_seconds, format_template_float_fold,
    format_template_float_min_max, format_template_float_product, format_template_float_round,
    format_template_float_sum, format_template_float_unary, format_template_integer_binary,
    format_template_integer_min_max, format_template_integer_product, format_template_integer_sum,
    format_template_ordering, format_template_printf, format_template_to_date,
    format_template_to_date_in_zone, indent_template_string, parse_template_float,
    parse_template_integer, substring_template_string, title_template_string,
    truncate_template_string, unix_to_template_timestamp, urldecode_template_string,
    urlencode_template_string, urlquery_template_string,
};

pub(crate) fn evaluate_template_function(
    name: &str,
    args: &[TemplateRuntimeValue],
) -> TemplateRuntimeValue {
    if let Some(value) = evaluate_template_value_function(name, args) {
        return value;
    }

    let args = args
        .iter()
        .map(TemplateRuntimeValue::as_rendered_string)
        .collect::<Vec<_>>();
    let rendered = evaluate_common_string_function(name, &args)
        .or_else(|| evaluate_prometheus_template_function(name, &args))
        .unwrap_or_else(|| match name {
            "add" => format_template_integer_sum(&args),
            "addf" => format_template_float_sum(&args),
            "ceil" => args.first().map_or_else(String::new, |value| {
                format_template_float_unary(value, f64::ceil)
            }),
            "bytes" => {
                let Some(value) = args.first() else {
                    return String::new();
                };
                format_template_bytes(value)
            }
            "date" => format_template_date(&args),
            "duration" | "duration_seconds" => {
                let Some(value) = args.first() else {
                    return String::new();
                };
                format_template_duration_seconds(value)
            }
            "div" => format_template_integer_binary(&args, |left, right| {
                (right != 0).then_some(left / right)
            }),
            "divf" => format_template_float_fold(&args, |left, right| {
                (right != 0.0).then_some(left / right)
            }),
            "eq" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                (args[1] == args[0]).to_string()
            }
            "ne" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                (args[1] != args[0]).to_string()
            }
            "lt" => format_template_ordering(&args, std::cmp::Ordering::is_lt),
            "le" => format_template_ordering(&args, std::cmp::Ordering::is_le),
            "gt" => format_template_ordering(&args, std::cmp::Ordering::is_gt),
            "ge" => format_template_ordering(&args, std::cmp::Ordering::is_ge),
            "float64" => args
                .first()
                .map_or_else(String::new, |value| parse_template_float(value)),
            "floor" => args.first().map_or_else(String::new, |value| {
                format_template_float_unary(value, f64::floor)
            }),
            "hasPrefix" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                args[1].starts_with(&args[0]).to_string()
            }
            "hasSuffix" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                args[1].ends_with(&args[0]).to_string()
            }
            "indent" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(spaces) = args[0].parse::<usize>() else {
                    return String::new();
                };
                indent_template_string(spaces, &args[1])
            }
            "int" => args
                .first()
                .map_or_else(String::new, |value| parse_template_integer(value)),
            "len" => args
                .first()
                .map_or_else(String::new, |value| value.len().to_string()),
            "max" => format_template_integer_min_max(&args, Ord::max),
            "maxf" => format_template_float_min_max(&args, f64::max),
            "min" => format_template_integer_min_max(&args, Ord::min),
            "minf" => format_template_float_min_max(&args, f64::min),
            "mod" => format_template_integer_binary(&args, |left, right| {
                (right != 0).then_some(left % right)
            }),
            "mul" => format_template_integer_product(&args),
            "mulf" => format_template_float_product(&args),
            "repeat" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(count) = args[0].parse::<usize>() else {
                    return String::new();
                };
                args[1].repeat(count)
            }
            "count" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(regex) = Regex::new(&args[0]) else {
                    return String::new();
                };
                regex.find_iter(&args[1]).count().to_string()
            }
            "regexReplaceAll" => {
                if args.len() < 3 {
                    return String::new();
                }
                let Ok(regex) = Regex::new(&args[0]) else {
                    return String::new();
                };
                regex.replace_all(&args[1], args[2].as_str()).into_owned()
            }
            "regexReplaceAllLiteral" => {
                if args.len() < 3 {
                    return String::new();
                }
                let Ok(regex) = Regex::new(&args[0]) else {
                    return String::new();
                };
                regex
                    .replace_all(&args[1], NoExpand(args[2].as_str()))
                    .into_owned()
            }
            "round" => format_template_float_round(&args),
            "trunc" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(count) = args[0].parse::<i64>() else {
                    return String::new();
                };
                truncate_template_string(&args[1], count)
            }
            "substr" => {
                if args.len() < 3 {
                    return String::new();
                }
                let (Ok(start), Ok(end)) = (args[0].parse::<i64>(), args[1].parse::<i64>()) else {
                    return String::new();
                };
                substring_template_string(&args[2], start, end)
            }
            "toDate" => format_template_to_date(&args),
            "toDateInZone" => format_template_to_date_in_zone(&args),
            "trim" => args
                .first()
                .map_or_else(String::new, |value| value.trim().to_string()),
            "trimAll" => {
                if args.len() < 2 {
                    return String::new();
                }
                args[1].trim_matches(|ch| args[0].contains(ch)).to_string()
            }
            "trimPrefix" => {
                if args.len() < 2 {
                    return String::new();
                }
                args[1]
                    .strip_prefix(&args[0])
                    .unwrap_or(&args[1])
                    .to_string()
            }
            "trimSuffix" => {
                if args.len() < 2 {
                    return String::new();
                }
                args[1]
                    .strip_suffix(&args[0])
                    .unwrap_or(&args[1])
                    .to_string()
            }
            "sub" => format_template_integer_binary(&args, |left, right| Some(left - right)),
            "subf" => format_template_float_fold(&args, |left, right| Some(left - right)),
            "unixEpoch" => epoch_template_timestamp(&args, 1_000_000_000),
            "unixEpochMillis" => epoch_template_timestamp(&args, 1_000_000),
            "unixEpochNanos" => epoch_template_timestamp(&args, 1),
            "unixToTime" => args
                .first()
                .map_or_else(String::new, |value| unix_to_template_timestamp(value)),
            "urlquery" => args
                .first()
                .map_or_else(String::new, |value| urlquery_template_string(value)),
            "urlencode" => args
                .first()
                .map_or_else(String::new, |value| urlencode_template_string(value)),
            "urldecode" => args
                .first()
                .map_or_else(String::new, |value| urldecode_template_string(value)),
            _ => String::new(),
        });
    TemplateRuntimeValue::String(rendered)
}

fn evaluate_prometheus_template_function(name: &str, args: &[String]) -> Option<String> {
    let rendered = match name {
        "humanize" => humanize(args),
        "humanize1024" => humanize_1024(args),
        "humanizeDuration" => args
            .first()
            .map_or_else(String::new, |value| humanize_duration(value)),
        "humanizePercentage" => args.first().map_or_else(String::new, |value| {
            value.parse::<f64>().map_or_else(
                |_| String::new(),
                |value| format!("{}%", four_significant_digits(value * 100.0)),
            )
        }),
        "printf" => format_template_printf(args),
        "reReplaceAll" => {
            let [pattern, replacement, input, ..] = args else {
                return Some(String::new());
            };
            let Ok(regex) = Regex::new(pattern) else {
                return Some(String::new());
            };
            regex.replace_all(input, replacement.as_str()).into_owned()
        }
        "title" => args
            .first()
            .map_or_else(String::new, |value| title_template_string(value)),
        _ => return None,
    };
    Some(rendered)
}

fn humanize(args: &[String]) -> String {
    let Some(Ok(value)) = args.first().map(|value| value.parse::<f64>()) else {
        return String::new();
    };
    if !value.is_finite() || value == 0.0 {
        return four_significant_digits(value);
    }
    let prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"];
    let sub_prefixes = ["", "m", "u", "n", "p", "f", "a", "z", "y"];
    let mut scaled = value.abs();
    let mut index = 0;
    if scaled < 1.0 {
        while scaled < 1.0 && index + 1 < sub_prefixes.len() {
            scaled *= 1000.0;
            index += 1;
        }
        return format!(
            "{}{}",
            four_significant_digits(scaled.copysign(value)),
            sub_prefixes[index]
        );
    }
    while scaled >= 1000.0 && index + 1 < prefixes.len() {
        scaled /= 1000.0;
        index += 1;
    }
    format!(
        "{}{}",
        four_significant_digits(scaled.copysign(value)),
        prefixes[index]
    )
}

fn humanize_1024(args: &[String]) -> String {
    let Some(Ok(value)) = args.first().map(|value| value.parse::<f64>()) else {
        return String::new();
    };
    if !value.is_finite() || value.abs() <= 1.0 {
        return four_significant_digits(value);
    }
    let prefixes = ["", "ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"];
    let mut scaled = value.abs();
    let mut index = 0;
    while scaled >= 1024.0 && index + 1 < prefixes.len() {
        scaled /= 1024.0;
        index += 1;
    }
    format!(
        "{}{}",
        four_significant_digits(scaled.copysign(value)),
        prefixes[index]
    )
}

fn humanize_duration(value: &str) -> String {
    let Ok(seconds) = value.parse::<f64>() else {
        return String::new();
    };
    if !seconds.is_finite() {
        return four_significant_digits(seconds);
    }
    let sign = if seconds < 0.0 { "-" } else { "" };
    let mut seconds = seconds.abs();
    if seconds < 1.0 {
        for (scale, suffix) in [
            (1_000.0, "ms"),
            (1_000_000.0, "us"),
            (1_000_000_000.0, "ns"),
        ] {
            let scaled = seconds * scale;
            if scaled >= 1.0 {
                return format!("{sign}{}{suffix}", four_significant_digits(scaled));
            }
        }
        return format!("{sign}{}s", four_significant_digits(seconds));
    }
    let days = (seconds / 86_400.0).floor();
    seconds -= days * 86_400.0;
    let hours = (seconds / 3_600.0).floor();
    seconds -= hours * 3_600.0;
    let minutes = (seconds / 60.0).floor();
    seconds -= minutes * 60.0;
    let mut parts = Vec::new();
    if days > 0.0 {
        parts.push(format!("{days:.0}d"));
    }
    if days > 0.0 || hours > 0.0 {
        parts.push(format!("{hours:.0}h"));
    }
    if days > 0.0 || hours > 0.0 || minutes > 0.0 {
        parts.push(format!("{minutes:.0}m"));
    }
    if days > 0.0 || hours > 0.0 || minutes > 0.0 {
        seconds = seconds.floor();
    }
    parts.push(format!("{}s", four_significant_digits(seconds)));
    format!("{sign}{}", parts.join(" "))
}

fn four_significant_digits(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value == f64::INFINITY {
        return "+Inf".into();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".into();
    }
    if value == 0.0 {
        return "0".into();
    }
    let abs = value.abs();
    let exponent = abs.log10().floor();
    if !(-4.0..4.0).contains(&exponent) {
        let mantissa = value / 10_f64.powf(exponent);
        let mantissa = format!("{mantissa:.3}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string();
        return format!("{mantissa}e{exponent:+03.0}");
    }
    let decimals = if exponent >= 3.0 {
        0
    } else if exponent >= 2.0 {
        1
    } else if exponent >= 1.0 {
        2
    } else if exponent >= 0.0 {
        3
    } else if exponent >= -1.0 {
        4
    } else if exponent >= -2.0 {
        5
    } else if exponent >= -3.0 {
        6
    } else {
        7
    };
    format!("{value:.decimals$}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}
