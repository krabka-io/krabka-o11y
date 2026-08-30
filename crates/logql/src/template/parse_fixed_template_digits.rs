use super::advance_template_pos;

pub(crate) fn parse_fixed_template_digits(value: &str, pos: &mut usize, count: usize) -> Option<u32> {
    let start = *pos;
    let digits = value.get(start..)?.get(..count)?;
    digits
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then_some(())?;
    advance_template_pos(pos, count)?;
    digits.parse::<u32>().ok()
}
