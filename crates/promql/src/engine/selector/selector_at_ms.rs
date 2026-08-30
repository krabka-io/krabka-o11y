use super::*;

pub(crate) fn selector_at_ms(
    time_ms: i64,
    at: Option<&AtModifier>,
    bounds: Option<AtModifierBounds>,
) -> Result<i64> {
    let Some(at) = at else {
        return Ok(time_ms);
    };
    match at {
        AtModifier::At(time) => system_time_ms(*time),
        AtModifier::Start => bounds.map(|bounds| bounds.start_ms).ok_or_else(|| {
            PromqlError::Unsupported(
                "@ start()/end() modifiers require range-query bounds".to_string(),
            )
        }),
        AtModifier::End => bounds.map(|bounds| bounds.end_ms).ok_or_else(|| {
            PromqlError::Unsupported(
                "@ start()/end() modifiers require range-query bounds".to_string(),
            )
        }),
    }
}
