use super::{AnnotationExpect, ExpectDirective, Line, Result, parse_error};

pub(crate) fn parse_expect_directive(directive: &str, line: Line<'_>) -> Result<ExpectDirective> {
    let directive = directive.trim();
    match directive {
        "no_warn" => {
            return Ok(ExpectDirective::Annotation(AnnotationExpect::NoWarn));
        }
        "no_info" => {
            return Ok(ExpectDirective::Annotation(AnnotationExpect::NoInfo));
        }
        "warn" => {
            return Ok(ExpectDirective::Annotation(AnnotationExpect::AnyWarn));
        }
        "info" => {
            return Ok(ExpectDirective::Annotation(AnnotationExpect::AnyInfo));
        }
        "ordered" => return Ok(ExpectDirective::Ordered),
        _ => {}
    }
    if let Some(message) = directive.strip_prefix("warn msg:") {
        return Ok(ExpectDirective::Annotation(AnnotationExpect::WarnMsg(
            message.trim().to_string(),
        )));
    }
    if let Some(message) = directive.strip_prefix("info msg:") {
        return Ok(ExpectDirective::Annotation(AnnotationExpect::InfoMsg(
            message.trim().to_string(),
        )));
    }
    Err(parse_error(
        line,
        format!("unsupported expect directive `{directive}`"),
    ))
}
