#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplateRangeBinding {
    Dot,
    Value(String),
    AssignValue(String),
    IndexValue { index: String, value: String },
    AssignIndexValue { index: String, value: String },
}
