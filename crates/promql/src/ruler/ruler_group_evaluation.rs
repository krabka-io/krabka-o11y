use super::*;

/// Summary of one ruler rule-group evaluation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RulerGroupEvaluation {
    pub recording_records: usize,
    pub alerts_dispatched: usize,
    pub last_eval_ms: i64,
}
