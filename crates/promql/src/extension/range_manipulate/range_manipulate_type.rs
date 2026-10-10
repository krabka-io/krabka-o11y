use super::{
    Arc, DfResult, Expr, LogicalPlan, UserDefinedLogicalNodeCore, build_extended_range_schema, fmt,
};
use crate::extension::only_logical_input;

/// The step grid, window and columns a range-vector materialization reads,
/// shared by the logical [`RangeManipulate`] node and its physical
/// `RangeManipulateExec`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct RangeManipulateSettings {
    /// The first eval step, in milliseconds.
    pub start_ms: i64,
    /// The last eval step, in milliseconds.
    pub end_ms: i64,
    /// The eval-step stride, in milliseconds.
    pub interval_ms: i64,
    /// The width of the window that closes on each eval step, in milliseconds.
    pub range_ms: i64,
    /// The `Int64` sample-timestamp column.
    pub time_index: String,
    /// The `Float64` sample-value column.
    pub field_column: String,
}

/// Logical node: materialize range vectors over a step grid.
///
/// The other fields fully determine `output_schema`. The manual `PartialEq`,
/// `Eq`, `Hash`, and `PartialOrd` impls that `UserDefinedLogicalNodeCore` needs
/// leave `output_schema` out, so node identity depends only on the logical
/// parameters.
#[derive(Debug, Clone)]
pub struct RangeManipulate {
    pub settings: RangeManipulateSettings,
    pub input: LogicalPlan,
    pub(crate) output_schema: datafusion::common::DFSchemaRef,
}

impl RangeManipulate {
    /// Builds the logical node and derives its extended output schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the metric input is malformed.
    /// Returns an error if a limit is exceeded.
    /// Returns an error if the backing WAL, block store, or remote endpoint fails.
    pub fn new(settings: RangeManipulateSettings, input: LogicalPlan) -> DfResult<Self> {
        let extended = build_extended_range_schema(
            input.schema().as_arrow(),
            &settings.time_index,
            &settings.field_column,
        );
        let output_schema = Arc::new(datafusion::common::DFSchema::try_from(
            extended.as_ref().clone(),
        )?);
        Ok(Self {
            settings,
            input,
            output_schema,
        })
    }

    /// The logical parameters that define node identity: every field except the
    /// derived `output_schema`.
    pub(crate) fn identity(&self) -> (&RangeManipulateSettings, &LogicalPlan) {
        (&self.settings, &self.input)
    }
}

impl PartialEq for RangeManipulate {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for RangeManipulate {}

impl std::hash::Hash for RangeManipulate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.identity().hash(state);
    }
}

impl PartialOrd for RangeManipulate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // `LogicalPlan` is not `Ord`; order by the scalar parameters only, which
        // is sufficient for the framework's deterministic-ordering needs.
        self.settings.partial_cmp(&other.settings)
    }
}

impl UserDefinedLogicalNodeCore for RangeManipulate {
    fn name(&self) -> &'static str {
        "RangeManipulate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PromRangeManipulate: start_ms={}, end_ms={}, interval_ms={}, range_ms={}",
            self.settings.start_ms,
            self.settings.end_ms,
            self.settings.interval_ms,
            self.settings.range_ms
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let input = only_logical_input(&exprs, inputs, "RangeManipulate")?;
        Self::new(self.settings.clone(), input)
    }
}
