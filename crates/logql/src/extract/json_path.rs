use super::{
    ParseError, QuotedBodyError, is_json_path_field_name_char, read_quoted_body,
    template_parse_error,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JsonPathPart {
    Field(String),
    Index(usize),
}

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
        read_quoted_body(self.input, &mut self.pos).map_err(|error| {
            template_parse_error(match error {
                QuotedBodyError::DanglingEscape => "expected escaped json path character",
                QuotedBodyError::Unterminated => "expected closing json path string",
            })
        })
    }

    pub(crate) fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonPath {
    pub(crate) parts: Vec<JsonPathPart>,
}

pub(crate) struct JsonPathMatch<'a> {
    pub(crate) value: Option<&'a serde_json::value::RawValue>,
    // Borrowed values point into the same input line. ArrayEach invokes all
    // requested paths at one element in query order, before scanning siblings.
    pub(crate) position: usize,
}

impl JsonPath {
    pub(crate) fn parse(expression: &str) -> Result<Self, ParseError> {
        let mut parser = JsonPathParser::new(expression);
        parser.parse()
    }

    pub(crate) fn evaluate<'a>(
        &self,
        value: &'a serde_json::value::RawValue,
    ) -> Option<JsonPathMatch<'a>> {
        Self::evaluate_parts(value, &self.parts)
    }

    // EachKey marks a path when its requested array index exists, even if a
    // subsequent field is absent. Object paths instead keep searching duplicate
    // parent keys until the first complete path matches.
    pub(crate) fn evaluate_parts<'a>(
        value: &'a serde_json::value::RawValue,
        parts: &[JsonPathPart],
    ) -> Option<JsonPathMatch<'a>> {
        let Some(first_part) = parts.split_first() else {
            return Some(JsonPathMatch {
                value: Some(value),
                position: value.get().as_ptr() as usize,
            });
        };
        let part = first_part.0;
        let rest = first_part.1;
        match part {
            JsonPathPart::Field(name) => {
                let entries = super::parse_json_object_entries(value.get()).ok()?;
                let mut entries = entries
                    .into_iter()
                    .filter(|entry| entry.key.as_str() == name.as_str());
                entries.find_map(|entry| Self::evaluate_parts(entry.value, rest))
            }
            JsonPathPart::Index(index) => {
                let values: Vec<&serde_json::value::RawValue> =
                    serde_json::from_str(value.get()).ok()?;
                let child = values.get(*index)?;
                Some(JsonPathMatch {
                    value: Self::evaluate_parts(child, rest).and_then(|matched| matched.value),
                    position: child.get().as_ptr() as usize,
                })
            }
        }
    }
}
