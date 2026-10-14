/// A lexical duration component and the unconsumed suffix after it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurationPart<'a> {
    /// The numeric component, such as `1.5`.
    pub number: &'a str,
    /// The unit component, such as `ms`.
    pub unit: &'a str,
    /// The input following this component.
    pub rest: &'a str,
}

/// Splits a duration string at its next numeric and alphabetic component.
///
/// Returns `None` when the input does not begin with a number. An empty unit
/// is retained in the returned value so callers can report errors using their
/// own parser's error type and wording.
#[must_use]
pub fn next_duration_part(input: &str) -> Option<DurationPart<'_>> {
    let number_len = input
        .char_indices()
        .take_while(|entry| entry.1.is_ascii_digit() || entry.1 == '.')
        .map(|entry| entry.0 + entry.1.len_utf8())
        .last()?;
    let number = &input[..number_len];
    let tail = &input[number_len..];
    let unit_len = tail
        .char_indices()
        .take_while(|entry| entry.1.is_ascii_alphabetic() || entry.1 == 'µ')
        .map(|entry| entry.0 + entry.1.len_utf8())
        .last()
        .unwrap_or_default();
    let unit = &tail[..unit_len];

    Some(DurationPart {
        number,
        unit,
        rest: &tail[unit_len..],
    })
}
