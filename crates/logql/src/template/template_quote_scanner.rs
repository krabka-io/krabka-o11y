/// Tracks whether a template scan is inside a quoted string literal.
///
/// Double- and single-quoted literals honour backslash escapes; raw
/// backtick literals do not.
#[derive(Debug, Default)]
pub(crate) struct TemplateQuoteScanner {
    open_quote: Option<char>,
    escaped: bool,
}

impl TemplateQuoteScanner {
    /// Consumes `ch` when it opens, continues, escapes or closes a quoted
    /// literal, and returns whether it did.
    pub(crate) fn consume_quoted(&mut self, ch: char) -> bool {
        if self.escaped {
            self.escaped = false;
            return true;
        }
        if matches!(self.open_quote, Some('"' | '\'')) && ch == '\\' {
            self.escaped = true;
            return true;
        }
        if let Some(quote_ch) = self.open_quote {
            if ch == quote_ch {
                self.open_quote = None;
            }
            return true;
        }
        if matches!(ch, '"' | '\'' | '`') {
            self.open_quote = Some(ch);
            return true;
        }
        false
    }

    /// Returns whether a quoted literal is still open.
    pub(crate) fn is_inside_quote(&self) -> bool {
        self.open_quote.is_some()
    }
}
