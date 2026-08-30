//! `InstantManipulate`: step-grid instant-vector lookback selection.

use std::{fmt, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, UInt32Array},
    compute::take,
    record_batch::RecordBatch,
};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, Float64Array, Int64Array},
        compute::concat_batches,
        datatypes::{DataType, Field, Schema},
    };
    use assert2::check;
    use datafusion::{
        catalog::MemTable,
        datasource::memory::MemorySourceConfig,
        logical_expr::{Extension, UserDefinedLogicalNodeCore, col},
        physical_plan::{collect, display::DisplayableExecutionPlan},
        prelude::SessionContext,
    };

    use super::*;

    fn batch_from_rows(rows: &[(i64, f64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let timestamps = rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>();
        let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    async fn logical_input() -> LogicalPlan {
        let batch = batch_from_rows(&[(0, 1.0)]);
        let schema = batch.schema();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("leaf", Arc::new(table)).unwrap();
        ctx.table("leaf")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap()
    }

    fn physical_input() -> Arc<dyn ExecutionPlan> {
        let batch = batch_from_rows(&[(0, 1.0)]);
        let schema = batch.schema();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn logical_node_reports_identity_explain_and_rejects_bad_rewrites() {
        let input = logical_input().await;
        let node = InstantManipulate {
            start_ms: 0,
            end_ms: 60_000,
            step_ms: 15_000,
            lookback_delta_ms: 300_000,
            time_index: "timestamp".to_string(),
            field_column: "value".to_string(),
            input: input.clone(),
        };

        check!(UserDefinedLogicalNodeCore::name(&node) == "InstantManipulate");
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        let explain = format!("{plan}");
        check!(explain.starts_with(
            "PromInstantManipulate: start_ms=0, end_ms=60000, step_ms=15000, lookback_delta_ms=300000"
        ));
        check!(explain.contains("TableScan: leaf projection=[timestamp, value]"));

        let node = InstantManipulate {
            start_ms: 0,
            end_ms: 60_000,
            step_ms: 15_000,
            lookback_delta_ms: 300_000,
            time_index: "timestamp".to_string(),
            field_column: "value".to_string(),
            input: input.clone(),
        };
        check!(
            node.with_exprs_and_inputs(vec![col("timestamp")], vec![input.clone()])
                .is_err()
        );
        check!(node.with_exprs_and_inputs(vec![], vec![]).is_err());
        let rewritten = node
            .with_exprs_and_inputs(vec![], vec![input.clone()])
            .expect("valid rewrite");
        assert2::assert!(
            rewritten
                == InstantManipulate {
                    start_ms: 0,
                    end_ms: 60_000,
                    step_ms: 15_000,
                    lookback_delta_ms: 300_000,
                    time_index: "timestamp".to_string(),
                    field_column: "value".to_string(),
                    input,
                }
        );
    }

    #[test]
    fn physical_node_reports_identity_display_ordering_and_rejects_bad_children() {
        let input = physical_input();
        let exec = Arc::new(InstantManipulateExec::new(
            0,
            60_000,
            15_000,
            300_000,
            "timestamp".to_string(),
            "value".to_string(),
            Arc::clone(&input),
        ));

        check!(exec.name() == "InstantManipulateExec");
        let display = format!(
            "{}",
            DisplayableExecutionPlan::new(exec.as_ref()).indent(false)
        );
        check!(display.starts_with(
            "PromInstantManipulateExec: start_ms=0, end_ms=60000, step_ms=15000, lookback_delta_ms=300000"
        ));
        check!(display.contains("DataSourceExec: partitions=1"));
        check!(exec.maintains_input_order() == vec![false]);
        check!(Arc::clone(&exec).with_new_children(vec![]).is_err());
        check!(
            Arc::clone(&exec)
                .with_new_children(vec![input])
                .expect("valid child rewrite")
                .name()
                == "InstantManipulateExec"
        );
    }

    #[tokio::test]
    async fn selects_latest_sample_within_lookback_for_each_grid_step() {
        let ts = Int64Array::from(vec![0_i64, 60_000]);
        let val = Float64Array::from(vec![1.0, 2.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(val)]).unwrap();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let exec = InstantManipulateExec::new(
            0,
            120_000,
            60_000,
            300_000,
            "timestamp".into(),
            "value".into(),
            mem,
        );
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        let merged = concat_batches(&out[0].schema(), &out).unwrap();
        let ts = merged
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let val = merged
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert2::assert!(
            (0..ts.len())
                .map(|index| ts.value(index))
                .collect::<Vec<_>>()
                == vec![0, 60_000, 120_000]
        );
        assert2::assert!(
            (0..val.len())
                .map(|index| val.value(index))
                .collect::<Vec<_>>()
                == vec![1.0, 2.0, 2.0]
        );
    }

    #[tokio::test]
    async fn excludes_sample_at_exact_lookback_delta() {
        let batch = batch_from_rows(&[(0, 1.0)]);
        let schema = batch.schema();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let exec = InstantManipulateExec::new(
            300_000,
            300_000,
            60_000,
            300_000,
            "timestamp".into(),
            "value".into(),
            mem,
        );
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        let rows = out.iter().map(RecordBatch::num_rows).sum::<usize>();
        assert2::assert!(rows == 0);
    }

    #[tokio::test]
    async fn keeps_genuine_nan_and_drops_stale_nan_marker() {
        // Two series: one whose latest in-window sample is a genuine NaN, and
        // one whose latest in-window sample is Prometheus' stale-NaN marker.
        // The genuine NaN must survive selection as a NaN value; the stale
        // marker must suppress its grid step entirely.
        let stale = f64::from_bits(super::super::STALE_NAN_BITS);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        // Two single-row batches so each series is normalized independently.
        let genuine = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0_i64])),
                Arc::new(Float64Array::from(vec![f64::NAN])),
            ],
        )
        .unwrap();
        let staled = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0_i64])),
                Arc::new(Float64Array::from(vec![stale])),
            ],
        )
        .unwrap();
        let mem =
            MemorySourceConfig::try_new_exec(&[vec![genuine], vec![staled]], schema, None).unwrap();

        let exec = InstantManipulateExec::new(
            0,
            0,
            60_000,
            300_000,
            "timestamp".into(),
            "value".into(),
            mem,
        );
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        let merged = concat_batches(&out[0].schema(), &out).unwrap();
        let val = merged
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Exactly one row survives: the genuine NaN. The stale marker is dropped.
        check!(val.len() == 1);
        check!(val.value(0).is_nan());
        check!(!super::super::is_stale_nan(val.value(0)));
    }
}

// === split-modules: generated submodules ===
mod instant_manipulate;
mod instant_manipulate_exec;

pub use instant_manipulate::InstantManipulate;
pub use instant_manipulate_exec::InstantManipulateExec;
