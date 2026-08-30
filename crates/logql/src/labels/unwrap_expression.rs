use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnwrapExpression {
    pub(crate) label: String,
    pub(crate) conversion: UnwrapConversion,
}

impl UnwrapExpression {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(label: impl Into<String>) -> Result<Self, ParseError> {
        let expression = Self {
            label: label.into(),
            conversion: UnwrapConversion::Raw,
        };
        expression.validate()?;
        Ok(expression)
    }

    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn bytes(label: impl Into<String>) -> Result<Self, ParseError> {
        let expression = Self {
            label: label.into(),
            conversion: UnwrapConversion::Bytes,
        };
        expression.validate()?;
        Ok(expression)
    }

    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn duration(label: impl Into<String>) -> Result<Self, ParseError> {
        let expression = Self {
            label: label.into(),
            conversion: UnwrapConversion::Duration,
        };
        expression.validate()?;
        Ok(expression)
    }

    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    #[must_use]
    pub fn conversion(&self) -> UnwrapConversion {
        self.conversion
    }

    pub(crate) fn apply(&self, fields: &mut Labels) {
        fields.remove(UNWRAP_SAMPLE_VALUE_LABEL);
        let Some(value) = fields.get(&self.label) else {
            fields.insert("__error__".to_string(), "SampleExtractionErr".to_string());
            fields.insert(
                "__error_details__".to_string(),
                format!("unwrap label `{}` is missing", self.label),
            );
            return;
        };
        if let Some(value) = self.convert_sample_value(value) {
            fields.insert(UNWRAP_SAMPLE_VALUE_LABEL.to_string(), value.clone());
        } else {
            fields.insert("__error__".to_string(), "SampleExtractionErr".to_string());
            fields.insert(
                "__error_details__".to_string(),
                format!("unwrap label `{}` cannot be converted", self.label),
            );
        }
    }

    pub(crate) fn convert_sample_value(&self, value: &str) -> Option<String> {
        match self.conversion {
            UnwrapConversion::Raw => parse_raw_sample_literal(value),
            UnwrapConversion::Bytes => {
                let bytes = parse_bytes_literal(value)?.bytes_f64();
                if bytes.fract() == 0.0 {
                    Some(bytes.to_u64()?.to_string())
                } else {
                    None
                }
            }
            UnwrapConversion::Duration => {
                let duration_ns = parse_prometheus_duration_literal(value)?;
                Some(format_decimal_ratio(
                    u128::try_from(duration_ns).ok()?,
                    1_000_000_000,
                ))
            }
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ParseError> {
        if self.label.is_empty() {
            return Err(template_parse_error("expected unwrap label name"));
        }
        Ok(())
    }
}
