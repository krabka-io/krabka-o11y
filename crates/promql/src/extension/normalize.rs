//! `SeriesNormalize`: applies the offset, sorts by timestamp, and drops stale values.

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

    async fn logical_input() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100_i64])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100_i64])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn logical_node_reports_identity_explain_and_rejects_bad_rewrites() {
        let input = logical_input().await;
        let node = SeriesNormalize {
            offset_ms: 123,
            time_index: "timestamp".to_string(),
            need_filter_out_nan: true,
            input: input.clone(),
        };

        check!(UserDefinedLogicalNodeCore::name(&node) == "SeriesNormalize");
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        let explain = format!("{plan}");
        check!(
            explain
                .starts_with("PromSeriesNormalize: time=timestamp, offset_ms=123, filter_nan=true")
        );
        check!(explain.contains("TableScan: leaf projection=[timestamp, value]"));

        let node = SeriesNormalize {
            offset_ms: 123,
            time_index: "timestamp".to_string(),
            need_filter_out_nan: true,
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
                == SeriesNormalize {
                    offset_ms: 123,
                    time_index: "timestamp".to_string(),
                    need_filter_out_nan: true,
                    input,
                }
        );
    }

    #[test]
    fn physical_node_reports_identity_display_ordering_and_rejects_bad_children() {
        let input = physical_input();
        let exec = Arc::new(SeriesNormalizeExec::new(
            123,
            "timestamp".to_string(),
            true,
            Arc::clone(&input),
        ));

        check!(exec.name() == "SeriesNormalizeExec");
        check!(
            format!(
                "{}",
                DisplayableExecutionPlan::new(exec.as_ref()).indent(false)
            ) == "PromSeriesNormalizeExec: time=timestamp, offset_ms=123, filter_nan=true\n  DataSourceExec: partitions=1, partition_sizes=[1]\n"
        );
        check!(exec.maintains_input_order() == vec![false]);
        check!(Arc::clone(&exec).with_new_children(vec![]).is_err());
        check!(
            Arc::clone(&exec)
                .with_new_children(vec![input])
                .expect("valid child rewrite")
                .name()
                == "SeriesNormalizeExec"
        );
    }

    #[tokio::test]
    async fn sorts_by_time_and_drops_nan() {
        let ts = Int64Array::from(vec![300_i64, 100, 200]);
        let val = Float64Array::from(vec![3.0, f64::NAN, 2.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(val)]).unwrap();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let exec = SeriesNormalizeExec::new(0, "timestamp".into(), true, mem);
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        let merged = concat_batches(&out[0].schema(), &out).unwrap();
        let ts = merged
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert2::assert!(
            (0..ts.len())
                .map(|index| ts.value(index))
                .collect::<Vec<_>>()
                == vec![200, 300]
        );
    }
}

mod series_normalize;
mod series_normalize_exec;

pub use series_normalize::SeriesNormalize;
pub use series_normalize_exec::SeriesNormalizeExec;
