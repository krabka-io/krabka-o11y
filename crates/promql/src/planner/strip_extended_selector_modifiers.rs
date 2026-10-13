use super::{ExtendedSelectorModifier, PromqlError, QuotedCopy, Result, extended_modifier_at};

pub(crate) struct StrippedExtendedSelectorModifiers {
    pub query: String,
    pub modifier: Option<ExtendedSelectorModifier>,
}

pub(crate) fn strip_extended_selector_modifiers(
    query: &str,
) -> Result<StrippedExtendedSelectorModifiers> {
    let chars = query.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(query.len());
    let mut index = 0;
    let mut quoted = QuotedCopy::default();
    let mut modifier = None;

    while index < chars.len() {
        if quoted.copy(&chars, &mut index, &mut out) {
            continue;
        }
        let ch = chars[index];

        if let Some(found_match) = extended_modifier_at(&chars, index) {
            let found = found_match.modifier;
            if let Some(previous) = modifier
                && previous != found
            {
                return Err(PromqlError::Parse(
                    "cannot mix anchored and smoothed selector modifiers".to_string(),
                ));
            }
            modifier = Some(found);
            while out.ends_with(char::is_whitespace) {
                out.pop();
            }
            index = found_match.end;
            while chars.get(index).is_some_and(|ch| ch.is_whitespace()) {
                index += 1;
            }
            if chars.get(index).is_some_and(|ch| *ch != ')' && *ch != ',') {
                out.push(' ');
            }
            continue;
        }

        out.push(ch);
        index += 1;
    }

    Ok(StrippedExtendedSelectorModifiers {
        query: out,
        modifier,
    })
}
