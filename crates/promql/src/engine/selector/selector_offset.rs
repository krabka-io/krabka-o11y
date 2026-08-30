use super::{ Offset, Result, Time, TimeExt,
    selector_duration};

/// The signed extent by which an `offset` modifier shifts a selector's
/// evaluation instant. `offset 5m` looks 5 minutes further back, so it is a
/// negative extent.
pub(crate) fn selector_offset(offset: Option<&Offset>) -> Result<Time> {
    let Some(offset) = offset else {
        return Ok(Time::ZERO);
    };
    let (duration, sign) = match offset {
        Offset::Pos(duration) => (*duration, -1.0),
        Offset::Neg(duration) => (*duration, 1.0),
    };
    Ok(selector_duration(duration)? * sign)
}
