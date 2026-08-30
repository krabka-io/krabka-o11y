use super::{LogicalPlan, DfResult, build_extended_range_schema, UserDefinedLogicalNodeCore, Arc, Expr, fmt, DataFusionError};

/// Logical node: materialize range vectors over a step grid.
///
/// The other fields fully determine `output_schema`. The manual `PartialEq`,
/// `Eq`, `Hash`, and `PartialOrd` impls that `UserDefinedLogicalNodeCore` needs
/// leave `output_schema` out, so node identity depends only on the logical
/// parameters.
#[derive(Debug, Clone)]
pub struct RangeManipulate {
    pub start_ms: i64,
    pub end_ms: i64,
    pub interval_ms: i64,
    pub range_ms: i64,
    pub time_index: String,
    pub field_column: String,
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
    pub fn new(
        start_ms: i64,
        end_ms: i64,
        interval_ms: i64,
        range_ms: i64,
        time_index: String,
        field_column: String,
        input: LogicalPlan,
    ) -> DfResult<Self> {
        let extended =
            build_extended_range_schema(input.schema().as_arrow(), &time_index, &field_column);
        let output_schema = Arc::new(datafusion::common::DFSchema::try_from(
            extended.as_ref().clone(),
        )?);
        Ok(Self {
            start_ms,
            end_ms,
            interval_ms,
            range_ms,
            time_index,
            field_column,
            input,
            output_schema,
        })
    }

    /// The logical parameters that define node identity: every field except the
    /// derived `output_schema`.
    pub(crate) fn identity(&self) -> (i64, i64, i64, i64, &str, &str, &LogicalPlan) {
        (
            self.start_ms,
            self.end_ms,
            self.interval_ms,
            self.range_ms,
            &self.time_index,
            &self.field_column,
            &self.input,
        )
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
        (
            self.start_ms,
            self.end_ms,
            self.interval_ms,
            self.range_ms,
            &self.time_index,
            &self.field_column,
        )
            .partial_cmp(&(
                other.start_ms,
                other.end_ms,
                other.interval_ms,
                other.range_ms,
                &other.time_index,
                &other.field_column,
            ))
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
            self.start_ms, self.end_ms, self.interval_ms, self.range_ms
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "RangeManipulate expects no expressions and one input".to_string(),
            ));
        }
        Self::new(
            self.start_ms,
            self.end_ms,
            self.interval_ms,
            self.range_ms,
            self.time_index.clone(),
            self.field_column.clone(),
            inputs.swap_remove(0),
        )
    }
}
