use super::{ExtendedSelectorModifier, is_ident_char};

pub(crate) struct ExtendedModifierMatch {
    pub modifier: ExtendedSelectorModifier,
    pub end: usize,
}

struct ModifierKeyword {
    keyword: &'static str,
    modifier: ExtendedSelectorModifier,
}

pub(crate) fn extended_modifier_at(chars: &[char], index: usize) -> Option<ExtendedModifierMatch> {
    for candidate in [
        ModifierKeyword {
            keyword: "anchored",
            modifier: ExtendedSelectorModifier::Anchored,
        },
        ModifierKeyword {
            keyword: "smoothed",
            modifier: ExtendedSelectorModifier::Smoothed,
        },
    ] {
        let end = index.checked_add(candidate.keyword.len())?;
        if end > chars.len() {
            continue;
        }
        if chars[index..end].iter().collect::<String>() != candidate.keyword {
            continue;
        }
        let before_ok = index == 0 || !is_ident_char(chars[index - 1]);
        let after_ok = chars.get(end).is_none_or(|ch| !is_ident_char(*ch));
        if before_ok && after_ok {
            return Some(ExtendedModifierMatch {
                modifier: candidate.modifier,
                end,
            });
        }
    }
    None
}
