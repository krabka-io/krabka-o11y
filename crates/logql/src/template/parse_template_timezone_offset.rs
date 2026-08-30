use super::*;

pub(crate) fn parse_template_timezone_offset(value: &str, pos: &mut usize) -> Option<i32> {
    let rest = value.get(*pos..)?;
    if rest.starts_with('Z') {
        advance_template_pos(pos, 1)?;
        return Some(0);
    }
    let sign: i32 = if rest.starts_with('+') {
        advance_template_pos(pos, 1)?;
        1
    } else if rest.starts_with('-') {
        advance_template_pos(pos, 1)?;
        -1
    } else {
        return None;
    };
    let hours = i32::try_from(parse_fixed_template_digits(value, pos, 2)?).ok()?;
    match_template_literal(value, pos, ':')?;
    let minutes = i32::try_from(parse_fixed_template_digits(value, pos, 2)?).ok()?;
    let total_minutes = hours.checked_mul(60)?.checked_add(minutes)?;
    sign.checked_mul(total_minutes.checked_mul(60)?)
}
