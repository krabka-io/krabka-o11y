use super::*;

/// The partial result of one tag-values job.
#[derive(Clone, Debug, Default)]
pub struct TagValuesPartial {
    pub values: Vec<TypedValue>,
    pub metrics: Metrics,
}
