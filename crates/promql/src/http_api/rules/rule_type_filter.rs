#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuleTypeFilter {
    Any,
    Alert,
    Record,
}

impl RuleTypeFilter {
    pub(crate) fn from_param(value: Option<&str>) -> Self {
        match value {
            Some("alert") => Self::Alert,
            Some("record") => Self::Record,
            _ => Self::Any,
        }
    }
}
