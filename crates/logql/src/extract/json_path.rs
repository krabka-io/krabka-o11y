use super::{JsonPathParser, JsonPathPart, ParseError};

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
        let Some((part, rest)) = parts.split_first() else {
            return Some(JsonPathMatch {
                value: Some(value),
                position: value.get().as_ptr() as usize,
            });
        };
        match part {
            JsonPathPart::Field(name) => {
                let entries = super::parse_json_object_entries(value.get()).ok()?;
                let mut entries = entries.into_iter().filter(|(key, _)| key == name);
                entries.find_map(|(_, child)| Self::evaluate_parts(child, rest))
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
