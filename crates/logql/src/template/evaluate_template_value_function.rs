use super::{
    TemplateRuntimeValue, evaluate_template_index, evaluate_template_slice, format_template_print,
    html_escape_template_string, js_escape_template_string,
};

pub(crate) fn evaluate_template_value_function(
    name: &str,
    args: &[TemplateRuntimeValue],
) -> Option<TemplateRuntimeValue> {
    if name == "fromJson" {
        let Some(value) = args.first() else {
            return Some(TemplateRuntimeValue::String(String::new()));
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&value.as_rendered_string())
        else {
            return Some(TemplateRuntimeValue::String(String::new()));
        };
        return Some(TemplateRuntimeValue::Json(value));
    }

    if name == "index" {
        return Some(evaluate_template_index(args));
    }
    if name == "slice" {
        return Some(evaluate_template_slice(args));
    }
    if name == "print" {
        return Some(TemplateRuntimeValue::String(format_template_print(
            args, false,
        )));
    }
    if name == "println" {
        return Some(TemplateRuntimeValue::String(format_template_print(
            args, true,
        )));
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
    if name == "and" {
        return Some(TemplateRuntimeValue::String(
            args.iter().all(TemplateRuntimeValue::is_truthy).to_string(),
        ));
    }
    if name == "not" {
        return Some(TemplateRuntimeValue::String(
            args.first()
                .is_none_or(|value| !value.is_truthy())
                .to_string(),
        ));
    }
    if name == "or" {
        return Some(TemplateRuntimeValue::String(
            args.iter().any(TemplateRuntimeValue::is_truthy).to_string(),
        ));
    }

    None
}
