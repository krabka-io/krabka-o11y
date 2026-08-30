use super::{
    AtModifier, AtModifierBounds, Offset, Result, apply_offset_delta, selector_at_ms,
    selector_offset,
};

pub(crate) fn apply_selector_time_modifier(
    time_ms: i64,
    at: Option<&AtModifier>,
    offset: Option<&Offset>,
    bounds: Option<AtModifierBounds>,
) -> Result<i64> {
    let base_time_ms = selector_at_ms(time_ms, at, bounds)?;
    apply_offset_delta(base_time_ms, selector_offset(offset)?)
}
