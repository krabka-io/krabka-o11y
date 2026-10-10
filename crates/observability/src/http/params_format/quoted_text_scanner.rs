/// Follows a left-to-right scan of `LogQL` text through its `"` and `` ` ``
/// quoted strings, so the scan can skip what they contain.
#[derive(Default)]
pub(crate) struct QuotedTextScanner {
    quote: Option<char>,
    escaped: bool,
}

impl QuotedTextScanner {
    /// Feeds the next character, and reports whether it belongs to a quoted
    /// string, its opening and closing quotes included.
    pub(crate) fn in_quotes(&mut self, ch: char) -> bool {
        if let Some(quote) = self.quote {
            if self.escaped {
                self.escaped = false;
            } else if ch == '\\' {
                self.escaped = true;
            } else if ch == quote {
                self.quote = None;
            }
            return true;
        }
        if matches!(ch, '"' | '`') {
            self.quote = Some(ch);
            return true;
        }
        false
    }
}
