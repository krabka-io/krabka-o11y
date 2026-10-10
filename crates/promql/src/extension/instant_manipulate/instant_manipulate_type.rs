use std::fmt;

use datafusion::{
    common::Result as DfResult,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
};

use crate::extension::only_logical_input;

/// The step grid and columns an instant-vector selection reads, shared by the
/// logical [`InstantManipulate`] node and its physical `InstantManipulateExec`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct InstantManipulateSettings {
    /// The first grid instant, in milliseconds.
    pub start_ms: i64,
    /// The last grid instant, in milliseconds.
    pub end_ms: i64,
    /// The grid stride, in milliseconds.
    pub step_ms: i64,
    /// How far back from a grid instant a sample may lie, in milliseconds.
    pub lookback_delta_ms: i64,
    /// The `Int64` sample-timestamp column.
    pub time_index: String,
    /// The `Float64` sample-value column.
    pub field_column: String,
}

/// Logical node: instant-vector selection over a step grid.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct InstantManipulate {
    pub settings: InstantManipulateSettings,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for InstantManipulate {
    fn name(&self) -> &'static str {
        "InstantManipulate"
    }

    pass_through_logical_node_plumbing!();

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromInstantManipulate: start_ms={}, end_ms={}, step_ms={}, lookback_delta_ms={}",
            self.settings.start_ms,
            self.settings.end_ms,
            self.settings.step_ms,
            self.settings.lookback_delta_ms
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = only_logical_input(&exprs, inputs, "InstantManipulate")?;
        Ok(Self {
            settings: self.settings.clone(),
            input,
        })
    }
}
