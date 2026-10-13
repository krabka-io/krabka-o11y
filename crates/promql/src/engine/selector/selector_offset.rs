use super::{Offset, Result, Time, TimeExt, selector_duration};

/// The signed extent by which an `offset` modifier shifts a selector's
/// evaluation instant. `offset 5m` looks 5 minutes further back, so it is a
/// negative extent.
pub(crate) fn selector_offset(offset: Option<&Offset>) -> Result<Time> {
    let Some(offset) = offset else {
        return Ok(Time::ZERO);
    };
    match offset {
        Offset::Pos(duration) => Ok(selector_duration(*duration)? * -1.0),
        Offset::Neg(duration) => Ok(selector_duration(*duration)?),
    }
}
