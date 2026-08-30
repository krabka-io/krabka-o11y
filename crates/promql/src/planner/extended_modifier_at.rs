use super::*;

pub(crate) fn extended_modifier_at(chars: &[char], index: usize) -> Option<(ExtendedSelectorModifier, usize)> {
    for (keyword, modifier) in [
        ("anchored", ExtendedSelectorModifier::Anchored),
        ("smoothed", ExtendedSelectorModifier::Smoothed),
    ] {
        let end = index.checked_add(keyword.len())?;
        if end > chars.len() {
            continue;
        }
        if chars[index..end].iter().collect::<String>() != keyword {
            continue;
        }
        let before_ok = index == 0 || !is_ident_char(chars[index - 1]);
        let after_ok = chars.get(end).is_none_or(|ch| !is_ident_char(*ch));
        if before_ok && after_ok {
            return Some((modifier, end));
        }
    }
    None
}
