use super::*;

pub(crate) fn evaluate_common_string_function(name: &str, args: &[String]) -> Option<String> {
    let rendered = match name {
        "alignLeft" => align_template_string(args, align_left_template_string),
        "alignRight" => align_template_string(args, align_right_template_string),
        "b64enc" => args
            .first()
            .map_or_else(String::new, |value| BASE64_STANDARD.encode(value)),
        "b64dec" => {
            let Some(value) = args.first() else {
                return Some(String::new());
            };
            let Ok(decoded) = BASE64_STANDARD.decode(value) else {
                return Some(String::new());
            };
            String::from_utf8(decoded).unwrap_or_default()
        }
        "lower" => args
            .first()
            .map_or_else(String::new, |value| value.to_lowercase()),
        "upper" => args
            .first()
            .map_or_else(String::new, |value| value.to_uppercase()),
        "replace" => {
            if args.len() < 3 {
                return Some(String::new());
            }
            args[2].replace(&args[0], &args[1])
        }
        "default" => {
            if args.len() < 2 || args[1].is_empty() {
                return Some(args.first().cloned().unwrap_or_default());
            }
            args[1].clone()
        }
        "contains" => {
            if args.len() < 2 {
                return Some("false".to_string());
            }
            args[1].contains(&args[0]).to_string()
        }
        "nindent" => {
            if args.len() < 2 {
                return Some(String::new());
            }
            let Ok(spaces) = args[0].parse::<usize>() else {
                return Some(String::new());
            };
            format!("\n{}", indent_template_string(spaces, &args[1]))
        }
        "now" => current_template_timestamp(),
        _ => return None,
    };
    Some(rendered)
}

fn align_template_string(args: &[String], align: impl FnOnce(usize, &str) -> String) -> String {
    let [width, text, ..] = args else {
        return String::new();
    };
    let Ok(width) = width.parse::<usize>() else {
        return String::new();
    };
    align(width, text)
}
