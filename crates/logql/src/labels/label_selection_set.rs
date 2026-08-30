use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelSelectionSet {
    pub(crate) selections: Vec<LabelSelection>,
}

impl LabelSelectionSet {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(selections: Vec<LabelSelection>) -> Result<Self, ParseError> {
        if selections.is_empty() {
            return Err(template_parse_error("expected label selection"));
        }
        Ok(Self { selections })
    }

    #[must_use]
    pub fn selections(&self) -> &[LabelSelection] {
        &self.selections
    }

    pub(crate) fn apply_drop(&self, fields: &mut Labels) {
        for selection in &self.selections {
            if selection.matches(fields) {
                fields.remove(selection.name_str());
            }
        }
    }

    pub(crate) fn apply_keep(&self, fields: &mut Labels) {
        let mut kept = Labels::new();
        for selection in &self.selections {
            if selection.matches(fields)
                && let Some(value) = fields.get(selection.name_str()).cloned()
            {
                kept.insert(selection.name_str().to_string(), value);
            }
        }

        for reserved in ["__error__", "__error_details__"] {
            if let Some(value) = fields.get(reserved).cloned() {
                kept.insert(reserved.to_string(), value);
            }
        }

        *fields = kept;
    }
}
