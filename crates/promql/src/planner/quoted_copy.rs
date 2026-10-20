/// Copies a `char` scanner of a `PromQL` query through string literals.
#[derive(Default)]
pub(crate) struct QuotedCopy {
    /// The delimiter of the string literal the scanner is inside, if any.
    quote: Option<char>,
}

impl QuotedCopy {
    /// Returns `true` when the char at `index` opens, continues, or closes a
    /// string literal: the scanner has pushed it (and an escaped char after a
    /// backslash) onto `copied` and advanced `index` past it. Returns `false`,
    /// leaving `index` and `copied` alone, for a char of query code.
    pub(crate) fn copy(&mut self, chars: &[char], index: &mut usize, copied: &mut String) -> bool {
        let ch = chars[*index];
        if let Some(quote_ch) = self.quote {
            copied.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.get(*index + 1) {
                    copied.push(*next);
                    *index += 2;
                    return true;
                }
            } else if ch == quote_ch {
                self.quote = None;
            }
            *index += 1;
            return true;
        }
        if ch == '"' || ch == '\'' || ch == '`' {
            self.quote = Some(ch);
            copied.push(ch);
            *index += 1;
            return true;
        }
        false
    }
}
