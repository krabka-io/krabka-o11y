use super::{Regex, ParseError, regexp_parse_error, Labels, insert_regexp_parser_error, insert_extracted_field};

#[derive(Clone, Debug)]
pub struct RegexpParser {
    pub(crate) pattern: String,
    pub(crate) regex: Regex,
    pub(crate) capture_names: Vec<String>,
}

impl RegexpParser {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(pattern: impl Into<String>) -> Result<Self, ParseError> {
        let pattern = pattern.into();
        let regex = Regex::new(&pattern).map_err(|error| regexp_parse_error(&error.to_string()))?;
        let capture_names = regex
            .capture_names()
            .flatten()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if capture_names.is_empty() {
            return Err(regexp_parse_error(
                "regexp parser requires at least one named capture",
            ));
        }

        Ok(Self {
            pattern,
            regex,
            capture_names,
        })
    }

    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub(crate) fn apply(&self, line: &str, fields: &mut Labels) {
        let Some(captures) = self.regex.captures(line) else {
            insert_regexp_parser_error(fields);
            return;
        };

        for name in &self.capture_names {
            if let Some(value) = captures.name(name) {
                insert_extracted_field(fields, name, value.as_str().to_string());
            }
        }
    }
}

impl PartialEq for RegexpParser {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.capture_names == other.capture_names
    }
}

impl Eq for RegexpParser {}
