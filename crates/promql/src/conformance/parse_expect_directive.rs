use super::{Line, Result, AnnotationExpect, parse_error};

pub(crate) fn parse_expect_directive(directive: &str, line: Line<'_>) -> Result<AnnotationExpect> {
    let directive = directive.trim();
    match directive {
        "no_warn" => return Ok(AnnotationExpect::NoWarn),
        "no_info" => return Ok(AnnotationExpect::NoInfo),
        "warn" => return Ok(AnnotationExpect::AnyWarn),
        "info" => return Ok(AnnotationExpect::AnyInfo),
        _ => {}
    }
    if let Some(message) = directive.strip_prefix("warn msg:") {
        return Ok(AnnotationExpect::WarnMsg(message.trim().to_string()));
    }
    if let Some(message) = directive.strip_prefix("info msg:") {
        return Ok(AnnotationExpect::InfoMsg(message.trim().to_string()));
    }
    if directive == "ordered" {
        return Ok(AnnotationExpect::Ordered);
    }
    Err(parse_error(
        line,
        format!("unsupported expect directive `{directive}`"),
    ))
}
