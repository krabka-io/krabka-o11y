//! Physical planning for the custom `PromQL` logical operators.
//!
//! `DataFusion` cannot turn the [`SeriesDivide`], [`SeriesNormalize`], and
//! [`InstantManipulate`] logical nodes into [`ExecutionPlan`]s on its own. This
//! module supplies an [`ExtensionPlanner`] that maps each logical node to its
//! `Exec` counterpart. It also supplies the [`prom_session_context`] helper,
//! which builds a [`SessionContext`] that holds that planner, so
//! `execute_logical_plan` can run the operator chain.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    error::Result as DfResult,
    execution::{context::QueryPlanner, session_state::SessionStateBuilder},
    logical_expr::{
        LogicalPlan, UserDefinedLogicalNode, physical_planning_context::PhysicalPlanningContext,
    },
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
    prelude::SessionContext,
};

use super::{
    instant_manipulate::{InstantManipulate, InstantManipulateExec},
    normalize::{SeriesNormalize, SeriesNormalizeExec},
    range_manipulate::{RangeManipulate, RangeManipulateExec},
    series_divide::{SeriesDivide, SeriesDivideExec},
};
use crate::functions::{
    register_aggregate_udafs, register_over_time_udfs, register_rate_udfs,
    register_scalar_math_udfs,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        catalog::MemTable,
        logical_expr::{Extension, LogicalPlan},
    };

    use super::*;
    use crate::extension::{
        instant_manipulate::InstantManipulate, normalize::SeriesNormalize,
        series_divide::SeriesDivide,
    };

    #[tokio::test]
    async fn execute_logical_plan_runs_divide_normalize_instant_chain() {
        // Two series ("a" and "b") with two samples each, intentionally out of
        // timestamp order so SeriesNormalize must sort them.
        let job = StringArray::from(vec!["a", "a", "b", "b"]);
        let ts = Int64Array::from(vec![60_000_i64, 0, 60_000, 0]);
        let value = Float64Array::from(vec![2.0, 1.0, 20.0, 10.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("job", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(job), Arc::new(ts), Arc::new(value)],
        )
        .unwrap();

        let ctx = prom_session_context();
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        ctx.register_table("leaf", Arc::new(table)).unwrap();
        let leaf = ctx
            .table("leaf")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();

        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide {
                tag_columns: vec!["job".to_string()],
                input: leaf,
            }),
        });
        let normalize = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize {
                offset_ms: 0,
                time_index: "timestamp".to_string(),
                need_filter_out_nan: false,
                input: divide,
            }),
        });
        let instant = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate {
                start_ms: 120_000,
                end_ms: 120_000,
                step_ms: 300_000,
                lookback_delta_ms: 300_000,
                time_index: "timestamp".to_string(),
                field_column: "value".to_string(),
                input: normalize,
            }),
        });

        let batches = ctx
            .execute_logical_plan(instant)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut got = Vec::new();
        for batch in &batches {
            let job = batch
                .column_by_name("job")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let value = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push((job.value(row).to_string(), value.value(row)));
            }
        }
        got.sort_by(|left, right| left.0.cmp(&right.0));

        // At grid step 120_000 within a 300_000 lookback, the latest sample for
        // each series (ts=60_000) is selected.
        assert2::assert!(got == vec![("a".to_string(), 2.0), ("b".to_string(), 20.0)]);
    }
}

// === split-modules: generated submodules ===
mod prom_extension_planner;
mod prom_query_planner;
mod prom_session_context;
mod single_input;

pub use prom_extension_planner::PromExtensionPlanner;
use prom_query_planner::PromQueryPlanner;
pub use prom_session_context::prom_session_context;
use single_input::single_input;
