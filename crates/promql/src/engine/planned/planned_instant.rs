use super::{OperatorInstant, InstantSample, RangeSeries, SessionContext, LogicalPlan, BTreeMap, SeriesFingerprint, Labels, InstantShape};

/// A planned instant-query result.
///
/// The recursive `PromqlEngine::plan_instant_expr` produces this type, and
/// `PromqlEngine::assemble_planned_instant` consumes it.
///
/// Most shapes lower to a `DataFusion` `LogicalPlan` over the custom operators
/// (`PlannedInstant::Operator`). The label-rewrite and ordering functions
/// `label_replace`/`label_join`/`sort`/`sort_desc` instead transform their
/// already-assembled inner instant vector in pure Rust, so they carry the
/// finished samples directly as `PlannedInstant::Precomputed`. No operator plan
/// runs for them.
pub(crate) enum PlannedInstant {
    /// An executable operator plan plus the metadata its shape's assembler needs.
    /// The box keeps the enum small, because the operator payload carries a
    /// `SessionContext` and a `LogicalPlan`.
    Operator(Box<OperatorInstant>),
    /// A fully-assembled instant vector from a label-rewrite or ordering
    /// transform over a recursively-planned inner vector. The assembler returns
    /// it to the caller verbatim. There is no operator plan to execute.
    Precomputed(Vec<InstantSample>),
    /// A fully-computed scalar result. The scalar-returning utility functions
    /// `time`/`pi`/`scalar` and the argless calendar forms carry this variant, as
    /// does any scalar∘scalar binary fold that the planner resolves in pure Rust.
    /// The assembler turns it into a `QueryResult::Scalar` verbatim, and there is
    /// no operator plan to execute. The `ts_ms`/`value` mirror exactly what the
    /// interpreter returns for the same expression, so the two paths are
    /// parity-exact.
    PrecomputedScalar { ts_ms: i64, value: f64 },
    /// A fully-computed string result. A top-level string literal carries this
    /// variant. The assembler turns it into a `QueryResult::Str` verbatim, and
    /// there is no operator plan to execute. The value mirrors exactly what the
    /// interpreter returns for the same literal.
    PrecomputedString { ts_ms: i64, value: String },
    /// A fully-materialized range vector, also called a range matrix. A top-level
    /// raw matrix selector or subquery carries this variant, and its
    /// `query_instant` result is a `QueryResult::RangeMatrix`. The interpreter's
    /// own `eval_matrix_selector`/`eval_subquery` builds it, so the two paths are
    /// parity-exact by construction.
    PrecomputedMatrix(Vec<RangeSeries>),
}

impl PlannedInstant {
    /// Wraps an executable operator plan and boxes the payload.
    pub(crate) fn operator(
        ctx: SessionContext,
        plan: LogicalPlan,
        labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
        shape: InstantShape,
    ) -> Self {
        Self::Operator(Box::new(OperatorInstant {
            ctx,
            plan,
            labels_by_fp,
            shape,
        }))
    }
}
