use super::*;

pub(crate) fn evaluate_template_function(name: &str, args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    if let Some(value) = evaluate_template_value_function(name, args) {
        return value;
    }

    let args = args
        .iter()
        .map(TemplateRuntimeValue::as_rendered_string)
        .collect::<Vec<_>>();
    let rendered = evaluate_common_string_function(name, &args).unwrap_or_else(|| match name {
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
        "divf" => {
            format_template_float_fold(&args, |left, right| (right != 0.0).then_some(left / right))
        }
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
        "printf" => format_template_printf(&args),
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
        "title" => args
            .first()
            .map_or_else(String::new, |value| title_template_string(value)),
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
