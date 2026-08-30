use super::*;

pub(crate) fn parse_variable_template_digits(value: &str, pos: &mut usize, max_count: usize) -> Option<u32> {
    let start = *pos;
    let rest = value.get(start..)?;
    let digits_len = rest
        .bytes()
        .take(max_count)
        .take_while(u8::is_ascii_digit)
        .count();
    let digits = rest.get(..digits_len).filter(|digits| !digits.is_empty())?;
    advance_template_pos(pos, digits_len)?;
    digits.parse::<u32>().ok()
}
