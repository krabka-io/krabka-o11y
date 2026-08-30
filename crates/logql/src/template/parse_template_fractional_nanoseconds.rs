use super::{advance_template_pos, match_template_literal};

pub(crate) fn parse_template_fractional_nanoseconds(
    value: &str,
    pos: &mut usize,
    max_digits: usize,
) -> Option<u32> {
    match_template_literal(value, pos, '.')?;
    let start = *pos;
    let rest = value.get(start..)?;
    let digits_len = rest
        .bytes()
        .take(max_digits)
        .take_while(u8::is_ascii_digit)
        .count();
    let digits = rest.get(..digits_len).filter(|digits| !digits.is_empty())?;
    advance_template_pos(pos, digits_len)?;
    let mut fraction = digits.parse::<u32>().ok()?;
    for _ in digits_len..9 {
        fraction = fraction.checked_mul(10)?;
    }
    Some(fraction)
}
