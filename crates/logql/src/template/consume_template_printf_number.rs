use super::*;

pub(crate) fn consume_template_printf_number<I>(chars: &mut std::iter::Peekable<I>) -> Option<usize>
where
    I: Iterator<Item = char>,
{
    let mut value = 0usize;
    let mut consumed = false;
    while let Some(ch) = chars.peek().copied() {
        let Some(digit) = ch.to_digit(10) else {
            break;
        };
        chars.next();
        value = value
            .saturating_mul(10)
            .saturating_add(usize::try_from(digit).unwrap_or(0));
        consumed = true;
    }
    consumed.then_some(value)
}
