use super::decode_quoted_escape;

pub(crate) struct LogfmtParser<'a> {
    pub(crate) input: &'a str,
    pub(crate) pos: usize,
}

impl<'a> LogfmtParser<'a> {
    pub(crate) fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    pub(crate) fn next_pair_with_options(
        &mut self,
        keep_standalone: bool,
        strict: bool,
    ) -> Result<Option<(String, String)>, String> {
        loop {
            let remaining = &self.input[self.pos..];
            let trimmed = remaining.trim_start_matches(char::is_whitespace);
            self.pos = self.input.len().saturating_sub(trimmed.len());
            if self.pos == self.input.len() {
                return Ok(None);
            }

            let token_start = self.pos;
            let key = self.parse_key();
            if key.is_empty() || !self.input[self.pos..].starts_with('=') {
                if keep_standalone && !key.is_empty() {
                    return Ok(Some((key, String::new())));
                }
                if strict && key.is_empty() {
                    return Err(format!("invalid logfmt token at byte {token_start}"));
                }
                let remaining = &self.input[self.pos..];
                let token_end = remaining
                    .find(char::is_whitespace)
                    .map_or(self.input.len(), |offset| self.pos.saturating_add(offset));
                self.pos = token_end;
                continue;
            }
            self.pos = self.pos.saturating_add('='.len_utf8());
            match self.parse_value(strict) {
                Ok(value) => return Ok(Some((key, value))),
                Err(details) if strict => return Err(details),
                Err(_) => {}
            }
        }
    }

    pub(crate) fn parse_key(&mut self) -> String {
        let start = self.pos;
        let remaining = &self.input[self.pos..];
        let key_end = remaining
            .find(|ch: char| ch.is_whitespace() || ch == '=')
            .map_or(self.input.len(), |offset| self.pos.saturating_add(offset));
        self.pos = key_end;
        self.input[start..self.pos].to_string()
    }

    pub(crate) fn parse_value(&mut self, strict: bool) -> Result<String, String> {
        if self.input[self.pos..].starts_with('"') {
            self.pos = self.pos.saturating_add('"'.len_utf8());
            return self.parse_quoted_value().ok_or_else(|| {
                format!(
                    "logfmt syntax error at pos {} : unterminated quoted value",
                    self.pos.saturating_add(1)
                )
            });
        }

        let start = self.pos;
        let remaining = &self.input[self.pos..];
        let value_end = remaining
            .find(char::is_whitespace)
            .map_or(self.input.len(), |offset| self.pos.saturating_add(offset));
        self.pos = value_end;
        let value = &self.input[start..self.pos];
        if strict && value.contains('=') {
            return Err(format!("invalid logfmt value at byte {start}"));
        }
        Ok(value.to_string())
    }

    pub(crate) fn parse_quoted_value(&mut self) -> Option<String> {
        let mut out = String::new();
        let start = self.pos;
        let mut chars = self.input[start..].char_indices();
        while let Some((offset, ch)) = chars.next() {
            let ch_end = start.saturating_add(offset).saturating_add(ch.len_utf8());
            match ch {
                '"' => {
                    self.pos = ch_end;
                    return Some(out);
                }
                '\\' => {
                    if let Some((escaped_offset, escaped)) = chars.next() {
                        self.pos = start
                            .saturating_add(escaped_offset)
                            .saturating_add(escaped.len_utf8());
                        out.push(decode_quoted_escape(escaped));
                    }
                }
                _ => out.push(ch),
            }
        }
        self.pos = self.input.len();
        None
    }
}
