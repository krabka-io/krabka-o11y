/// Whether [`escape_quoted`] escapes a tab or leaves it as it is.
#[derive(Clone, Copy)]
pub(crate) enum TabEscape {
    Escaped,
    Literal,
}

/// `value` as the body of a double-quoted string: backslashes, double quotes
/// and newlines escaped, and tabs as `tabs` says.
pub(crate) fn escape_quoted(value: &str, tabs: TabEscape) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\t' if matches!(tabs, TabEscape::Escaped) => "\\t".chars().collect::<Vec<_>>(),
            _ => vec![ch],
        })
        .collect()
}
