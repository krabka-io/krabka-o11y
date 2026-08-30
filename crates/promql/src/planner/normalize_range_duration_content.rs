use super::{DurationExprContext, DurationExprParser, Result, seconds_to_duration_literal, top_level_colon};

pub(crate) fn normalize_range_duration_content(
    content: &str,
    duration_ctx: DurationExprContext,
) -> Result<String> {
    let Some(colon) = top_level_colon(content)? else {
        return seconds_to_duration_literal(
            DurationExprParser::new(content, duration_ctx).parse()?,
        );
    };
    let range = content[..colon].trim();
    let step = content[colon + 1..].trim();
    let range = seconds_to_duration_literal(DurationExprParser::new(range, duration_ctx).parse()?)?;
    if step.is_empty() {
        Ok(format!("{range}:"))
    } else {
        Ok(format!(
            "{range}:{}",
            seconds_to_duration_literal(DurationExprParser::new(step, duration_ctx).parse()?)?
        ))
    }
}
