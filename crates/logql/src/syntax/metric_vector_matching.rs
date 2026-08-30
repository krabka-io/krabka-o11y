use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricVectorMatching {
    On {
        labels: Vec<String>,
        group: Option<MetricVectorGroupModifier>,
    },
    Ignoring {
        labels: Vec<String>,
        group: Option<MetricVectorGroupModifier>,
    },
}
