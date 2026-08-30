use super::{
    JsonPath, JsonPathPart, ParseError, decode_quoted_escape, is_json_path_field_name_char,
    template_parse_error,
};

pub(crate) struct JsonPathParser<'a> {
    pub(crate) input: &'a str,
    pub(crate) pos: usize,
}

impl<'a> JsonPathParser<'a> {
    pub(crate) fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    pub(crate) fn parse(&mut self) -> Result<JsonPath, ParseError> {
        let mut parts = Vec::new();
        if self.input.starts_with('.') {
            return Err(template_parse_error(
                "expected json field name before dot separator",
            ));
        }
        if let Some(field) = self.parse_field_name() {
            parts.push(JsonPathPart::Field(field));
        }

        while self.pos < self.input.len() {
            match self.peek() {
                Some('.') => {
                    self.pos = self.pos.saturating_add('.'.len_utf8());
                    let field = self.parse_field_name().ok_or_else(|| {
                        template_parse_error("expected json field name after '.'")
                    })?;
                    parts.push(JsonPathPart::Field(field));
                }
                Some('[') => {
                    self.pos = self.pos.saturating_add('['.len_utf8());
                    parts.push(self.parse_bracket_part()?);
                    if self.peek() != Some(']') {
                        return Err(template_parse_error("expected closing json path bracket"));
                    }
                    self.pos = self.pos.saturating_add(']'.len_utf8());
                }
                _ => return Err(template_parse_error("expected json path component")),
            }
        }

        if parts.is_empty() {
            return Err(template_parse_error("expected json path expression"));
        }
        Ok(JsonPath { parts })
    }

    pub(crate) fn parse_field_name(&mut self) -> Option<String> {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if is_json_path_field_name_char(ch) {
                self.pos = self.pos.saturating_add(ch.len_utf8());
            } else {
                break;
            }
        }
        (self.pos > start).then(|| self.input[start..self.pos].to_string())
    }

    pub(crate) fn parse_bracket_part(&mut self) -> Result<JsonPathPart, ParseError> {
        if self.peek() == Some('"') {
            return self.parse_bracket_string().map(JsonPathPart::Field);
        }

        let start = self.pos;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.pos = self.pos.saturating_add('0'.len_utf8());
        }
        if self.pos == start {
            return Err(template_parse_error("expected json path array index"));
        }
        let index = self.input[start..self.pos]
            .parse::<usize>()
            .map_err(|_| template_parse_error("expected json path array index"))?;
        Ok(JsonPathPart::Index(index))
    }

    pub(crate) fn parse_bracket_string(&mut self) -> Result<String, ParseError> {
        self.pos = self.pos.saturating_add('"'.len_utf8());
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            self.pos = self.pos.saturating_add(ch.len_utf8());
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let Some(escaped) = self.peek() else {
                        return Err(template_parse_error("expected escaped json path character"));
                    };
                    self.pos = self.pos.saturating_add(escaped.len_utf8());
                    out.push(decode_quoted_escape(escaped));
                }
                _ => out.push(ch),
            }
        }
        Err(template_parse_error("expected closing json path string"))
    }

    pub(crate) fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }
}
