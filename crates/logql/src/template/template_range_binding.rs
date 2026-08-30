#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplateRangeBinding {
    Dot,
    Value(String),
    IndexValue { index: String, value: String },
}
