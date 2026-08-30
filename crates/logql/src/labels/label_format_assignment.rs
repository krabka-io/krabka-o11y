use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelFormatAssignment {
    pub(crate) destination: String,
    pub(crate) value: LabelFormatValue,
}

impl LabelFormatAssignment {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn rename(
        destination: impl Into<String>,
        source: impl Into<String>,
    ) -> Result<Self, ParseError> {
        Ok(Self {
            destination: destination.into(),
            value: LabelFormatValue::Rename(source.into()),
        })
    }

    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn template(
        destination: impl Into<String>,
        template: impl Into<String>,
    ) -> Result<Self, ParseError> {
        Ok(Self {
            destination: destination.into(),
            value: LabelFormatValue::Template(LineFormat::new(template)?),
        })
    }

    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    #[must_use]
    pub fn value(&self) -> &LabelFormatValue {
        &self.value
    }

    pub(crate) fn apply_with_timestamp(&self, line: &str, fields: &mut Labels, timestamp_ns: Option<i64>) {
        match &self.value {
            LabelFormatValue::Rename(source) => {
                if let Some(value) = fields.remove(source) {
                    fields.insert(self.destination.clone(), value);
                } else {
                    fields.remove(&self.destination);
                }
            }
            LabelFormatValue::Template(template) => {
                fields.insert(
                    self.destination.clone(),
                    template.render_with_timestamp(line, fields, timestamp_ns),
                );
            }
        }
    }
}
