use super::{SessionContext, LogicalPlan};

/// The assembled scalar-math operator plan.
///
/// The leaf already drops the metric name, so the code reads the result label set
/// directly from the projected output batch. The plan needs no map from
/// fingerprint to labels.
pub struct ScalarMathPlan {
    /// Session context whose registry holds the scalar-math UDFs. The context
    /// also holds the registered leaf table.
    pub ctx: SessionContext,
    /// The `Projection(labels..., prom_<fn>([bounds...,] value) AS value)` plan.
    pub plan: LogicalPlan,
}
