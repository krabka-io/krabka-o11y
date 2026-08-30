use super::{
    Labels, ParseError, PatternPart, insert_extracted_field, insert_pattern_parser_error,
    parse_pattern_parts,
};

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

        for (name, value) in captures {
            if name != "_" {
                insert_extracted_field(fields, &name, value);
            }
        }
    }

    pub(crate) fn captures(&self, line: &str) -> Option<Vec<(String, String)>> {
        let mut pos = 0;
        let mut captures = Vec::new();
        for (index, part) in self.parts.iter().enumerate() {
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
                        pos.saturating_add(line[pos..].find(next_literal)?)
                    } else {
                        line.len()
                    };
                    captures.push((name.clone(), line[pos..value_end].to_string()));
                    pos = value_end;
                }
            }
        }
        Some(captures)
    }
}
