use super::{AtModifier, AtModifierBounds, Result, system_time_ms};

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
        AtModifier::Start => Ok(bounds.map_or(time_ms, |bounds| bounds.start_ms)),
        AtModifier::End => Ok(bounds.map_or(time_ms, |bounds| bounds.end_ms)),
    }
}
