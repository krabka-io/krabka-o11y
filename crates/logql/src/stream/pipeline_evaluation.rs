use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineEvaluation {
    pub fields: Labels,
    pub line: String,
}
