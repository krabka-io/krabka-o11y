use super::{
    ComparisonOp, FieldValue, Labels, ParseError, Regex, insert_extracted_field,
    parse_bytes_literal, parse_prometheus_duration_literal,
};

#[derive(Clone, Debug, PartialEq)]
pub struct FieldFilter {
    pub name: String,
    pub op: ComparisonOp,
    pub value: FieldValue,
}

impl FieldFilter {
    #[must_use]
    pub fn new(name: impl Into<String>, op: ComparisonOp, value: FieldValue) -> Self {
        Self {
            name: name.into(),
            op,
            value,
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn try_new(
        name: impl Into<String>,
        op: ComparisonOp,
        value: FieldValue,
    ) -> Result<Self, ParseError> {
        let filter = Self::new(name, op, value);
        filter.validate()?;
        Ok(filter)
    }

    #[must_use]
    pub fn matches(&self, fields: &Labels) -> bool {
        let mut fields = fields.clone();
        self.apply(&mut fields)
    }

    pub fn apply(&self, fields: &mut Labels) -> bool {
        let candidate = fields
            .get(&self.name)
            .map_or("", String::as_str)
            .to_string();

        match &self.value {
            FieldValue::Number(expected) => {
                if !fields.contains_key(&self.name) {
                    return false;
                }
                if let Ok(candidate) = candidate.parse::<f64>() {
                    self.op.compare_numbers(candidate, *expected)
                } else {
                    insert_extracted_field(fields, "__error__", "LabelFilterErr".to_string());
                    insert_extracted_field(
                        fields,
                        "__error_details__",
                        format!(r#"strconv.ParseFloat: parsing "{candidate}": invalid syntax"#),
                    );
                    true
                }
            }
            FieldValue::Duration(expected) => parse_prometheus_duration_literal(&candidate)
                .is_some_and(|candidate| {
                    num_traits::ToPrimitive::to_f64(&candidate)
                        .zip(num_traits::ToPrimitive::to_f64(expected))
                        .is_some_and(|(candidate, expected)| {
                            self.op.compare_numbers(candidate, expected)
                        })
                }),
            FieldValue::Bytes(expected) => parse_bytes_literal(&candidate)
                .is_some_and(|candidate| self.op.compare_sizes(candidate, *expected)),
            FieldValue::String(expected) => self.op.compare_strings(&candidate, expected),
            FieldValue::Ip(expected) => match self.op {
                ComparisonOp::Equal => expected.matches_ip_text(&candidate),
                ComparisonOp::NotEqual => !expected.matches_ip_text(&candidate),
                _ => false,
            },
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ParseError> {
        if matches!(
            self.op,
            ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual
        ) {
            let FieldValue::String(pattern) = &self.value else {
                return Err(ParseError::Syntax {
                    message: "expected string regex field comparison value".to_string(),
                    position: 0,
                });
            };
            Regex::new(pattern).map_err(|source| ParseError::InvalidRegex {
                pattern: pattern.clone(),
                source,
            })?;
        }
        if matches!(self.value, FieldValue::Ip(_))
            && !matches!(self.op, ComparisonOp::Equal | ComparisonOp::NotEqual)
        {
            return Err(ParseError::Syntax {
                message: "ip field comparisons only support = and !=".to_string(),
                position: 0,
            });
        }
        Ok(())
    }
}
