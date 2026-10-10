//! `SeriesNormalize`: applies the offset, sorts by timestamp, and drops stale values.

use std::{fmt, sync::Arc};

use arrow::{array::Float64Array, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    },
};

#[cfg(test)]
mod tests {
    use assert2::check;
    use datafusion::{
        logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore, col},
        physical_plan::display::DisplayableExecutionPlan,
    };

    use super::*;
    use crate::extension::test_support::{
        check_single_child_exec, checked_rewrite, collect_concat, int64_values, logical_leaf,
        physical_leaf, time_value_batch,
    };

    #[tokio::test]
    async fn logical_node_reports_identity_explain_and_rejects_bad_rewrites() {
        let input = logical_leaf(time_value_batch(vec![100], vec![1.0])).await;
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
        let rewritten = checked_rewrite(&node, &input, col("timestamp"));
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
        let input = physical_leaf(vec![time_value_batch(vec![100], vec![1.0])]);
        let exec: Arc<dyn ExecutionPlan> = Arc::new(SeriesNormalizeExec::new(
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
        check_single_child_exec(&exec, input, "SeriesNormalizeExec");
    }

    #[tokio::test]
    async fn sorts_by_time_and_drops_nan() {
        let mem = physical_leaf(vec![time_value_batch(
            vec![300, 100, 200],
            vec![3.0, f64::NAN, 2.0],
        )]);

        let exec = SeriesNormalizeExec::new(0, "timestamp".into(), true, mem);
        let merged = collect_concat(Arc::new(exec)).await;
        assert2::assert!(int64_values(&merged, "timestamp") == vec![200, 300]);
    }
}

mod series_normalize;
mod series_normalize_exec;

pub use series_normalize::SeriesNormalize;
pub use series_normalize_exec::SeriesNormalizeExec;
