//! `SeriesDivide`: split sorted input into contiguous single-series batches.

use std::{fmt, sync::Arc};

use arrow::{record_batch::RecordBatch, util::display::array_value_to_string};
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
        array::{Array, Int64Array, StringArray},
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

    fn input_batch() -> RecordBatch {
        let job = StringArray::from(vec!["a", "a", "b"]);
        let ts = Int64Array::from(vec![1_i64, 2, 1]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("job", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(job), Arc::new(ts)]).unwrap()
    }

    async fn logical_input() -> LogicalPlan {
        let batch = input_batch();
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
        let batch = input_batch();
        let schema = batch.schema();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn logical_node_reports_identity_explain_and_rejects_bad_rewrites() {
        let input = logical_input().await;
        let node = SeriesDivide {
            tag_columns: vec!["job".to_string()],
            input: input.clone(),
        };

        check!(UserDefinedLogicalNodeCore::name(&node) == "SeriesDivide");
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        let explain = format!("{plan}");
        check!(explain.starts_with("PromSeriesDivide: tags=[\"job\"]"));
        check!(explain.contains("TableScan: leaf projection=[job, timestamp]"));

        let node = SeriesDivide {
            tag_columns: vec!["job".to_string()],
            input: input.clone(),
        };
        check!(
            node.with_exprs_and_inputs(vec![col("job")], vec![input.clone()])
                .is_err()
        );
        check!(node.with_exprs_and_inputs(vec![], vec![]).is_err());
        let rewritten = node
            .with_exprs_and_inputs(vec![], vec![input.clone()])
            .expect("valid rewrite");
        assert2::assert!(
            rewritten
                == SeriesDivide {
                    tag_columns: vec!["job".to_string()],
                    input,
                }
        );
    }

    #[test]
    fn physical_node_reports_identity_display_ordering_and_rejects_bad_children() {
        let input = physical_input();
        let exec = Arc::new(SeriesDivideExec::new(
            vec!["job".to_string()],
            Arc::clone(&input),
        ));

        check!(exec.name() == "SeriesDivideExec");
        let display = format!(
            "{}",
            DisplayableExecutionPlan::new(exec.as_ref()).indent(false)
        );
        check!(display.starts_with("PromSeriesDivideExec: tags=[\"job\"]"));
        check!(display.contains("DataSourceExec: partitions=1"));
        check!(exec.maintains_input_order() == vec![true]);
        check!(Arc::clone(&exec).with_new_children(vec![]).is_err());
        check!(
            Arc::clone(&exec)
                .with_new_children(vec![input])
                .expect("valid child rewrite")
                .name()
                == "SeriesDivideExec"
        );
    }

    #[tokio::test]
    async fn divides_into_single_series_batches() {
        let batch = input_batch();
        let schema = batch.schema();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let exec = SeriesDivideExec::new(vec!["job".to_string()], mem);

        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        for batch in &out {
            let job = batch
                .column_by_name("job")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let first = job.value(0);
            assert2::assert!((0..job.len()).all(|index| job.value(index) == first));
        }
        let total = out.iter().map(RecordBatch::num_rows).sum::<usize>();
        assert2::assert!(total == 3);
    }
}

mod series_divide_exec;
mod series_divide_type;

pub use series_divide_exec::SeriesDivideExec;
pub use series_divide_type::SeriesDivide;
