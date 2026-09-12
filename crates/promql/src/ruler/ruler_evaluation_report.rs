use super::RulerGroupEvaluation;

/// Identity and outcome of one rule evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct RulerRuleEvaluationStatus {
    pub tenant: String,
    pub namespace: String,
    pub group: String,
    pub rule_index: usize,
    pub last_error: String,
    pub last_evaluation_ms: i64,
    pub evaluation_time_seconds: f64,
}

/// Identity and outcome of one rule-group evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct RulerGroupEvaluationStatus {
    pub tenant: String,
    pub namespace: String,
    pub group: String,
    pub last_error: String,
    pub last_evaluation_ms: i64,
    pub evaluation_time_seconds: f64,
}

/// Detailed ruler outcome used for status rendering and metrics.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RulerEvaluationReport {
    pub evaluation: RulerGroupEvaluation,
    pub rules: Vec<RulerRuleEvaluationStatus>,
    pub groups: Vec<RulerGroupEvaluationStatus>,
}
