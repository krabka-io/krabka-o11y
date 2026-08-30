use super::{BTreeSet, LabelFormatAssignment, Labels, ParseError, template_parse_error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelFormat {
    pub(crate) assignments: Vec<LabelFormatAssignment>,
}

impl LabelFormat {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(assignments: Vec<LabelFormatAssignment>) -> Result<Self, ParseError> {
        let mut destinations = BTreeSet::new();
        for assignment in &assignments {
            if !destinations.insert(assignment.destination.clone()) {
                return Err(template_parse_error(
                    "label_format destination appears more than once",
                ));
            }
        }
        Ok(Self { assignments })
    }

    #[must_use]
    pub fn assignments(&self) -> &[LabelFormatAssignment] {
        &self.assignments
    }

    pub(crate) fn apply_with_timestamp(
        &self,
        line: &str,
        fields: &mut Labels,
        timestamp_ns: Option<i64>,
    ) {
        for assignment in &self.assignments {
            assignment.apply_with_timestamp(line, fields, timestamp_ns);
        }
    }
}
