/// Outcome of one ruler rule evaluation, as
/// [`ServiceMetrics::record_ruler_rule`](super::ServiceMetrics::record_ruler_rule)
/// records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleEvaluationOutcome {
    /// The rule evaluated without an error.
    Succeeded,
    /// The rule's evaluation failed: it counts toward
    /// `rule_evaluation_failures`.
    Failed,
}
