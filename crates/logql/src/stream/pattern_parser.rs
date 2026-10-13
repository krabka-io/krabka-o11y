use super::{
    Labels, ParseError, PatternPart, insert_pattern_parser_error, insert_raw_parsed_field,
    parse_pattern_parts,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PatternCapture {
    pub(crate) name: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatternParser {
    pub(crate) pattern: String,
    pub(crate) parts: Vec<PatternPart>,
}

impl PatternParser {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(pattern: impl Into<String>) -> Result<Self, ParseError> {
        let pattern = pattern.into();
        let parts = parse_pattern_parts(&pattern)?;
        Ok(Self { pattern, parts })
    }

    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub(crate) fn apply(&self, line: &str, fields: &mut Labels) {
        let Some(captures) = self.captures(line) else {
            insert_pattern_parser_error(fields);
            return;
        };

        for capture in captures {
            if capture.name != "_" {
                insert_raw_parsed_field(fields, &capture.name, capture.value);
            }
        }
    }

    pub(crate) fn captures(&self, line: &str) -> Option<Vec<PatternCapture>> {
        let mut pos = 0;
        let mut captures = Vec::new();
        let mut index: usize = 0;
        for part in &self.parts {
            match part {
                PatternPart::Literal(literal) => {
                    if !line[pos..].starts_with(literal) {
                        return None;
                    }
                    pos = pos.saturating_add(literal.len());
                }
                PatternPart::Capture(name) => {
                    let next_literal =
                        self.parts
                            .iter()
                            .skip(index.saturating_add(1))
                            .find_map(|part| {
                                if let PatternPart::Literal(literal) = part {
                                    Some(literal.as_str())
                                } else {
                                    None
                                }
                            });
                    let value_end = if let Some(next_literal) = next_literal {
                        let Some(offset) = line[pos..].find(next_literal) else {
                            captures.push(PatternCapture {
                                name: name.clone(),
                                value: line[pos..].to_string(),
                            });
                            return Some(captures);
                        };
                        pos.saturating_add(offset)
                    } else {
                        line.len()
                    };
                    captures.push(PatternCapture {
                        name: name.clone(),
                        value: line[pos..value_end].to_string(),
                    });
                    pos = value_end;
                }
            }
            index += 1;
        }
        Some(captures)
    }
}
