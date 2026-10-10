//! `InstantManipulate`: step-grid instant-vector lookback selection.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use assert2::check;
    use datafusion::{
        datasource::memory::MemorySourceConfig,
        logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore, col},
        physical_plan::{ExecutionPlan, collect, display::DisplayableExecutionPlan},
        prelude::SessionContext,
    };

    use super::*;
    use crate::extension::test_support::{
        check_single_child_exec, checked_rewrite, collect_concat, float64_values, int64_values,
        logical_leaf, physical_leaf, time_value_batch,
    };

    /// A minute-long grid of 15s steps with a five-minute lookback, over the
    /// `timestamp` and `value` columns.
    fn explain_settings() -> InstantManipulateSettings {
        InstantManipulateSettings {
            start_ms: 0,
            end_ms: 60_000,
            step_ms: 15_000,
            lookback_delta_ms: 300_000,
            time_index: "timestamp".to_string(),
            field_column: "value".to_string(),
        }
    }

    #[tokio::test]
    async fn logical_node_reports_identity_explain_and_rejects_bad_rewrites() {
        let input = logical_leaf(time_value_batch(vec![0], vec![1.0])).await;
        let node = InstantManipulate {
            settings: explain_settings(),
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
            settings: explain_settings(),
            input: input.clone(),
        };
        let rewritten = checked_rewrite(&node, &input, col("timestamp"));
        assert2::assert!(
            rewritten
                == InstantManipulate {
                    settings: explain_settings(),
                    input,
                }
        );
    }

    #[test]
    fn physical_node_reports_identity_display_ordering_and_rejects_bad_children() {
        let input = physical_leaf(vec![time_value_batch(vec![0], vec![1.0])]);
        let exec: Arc<dyn ExecutionPlan> = Arc::new(InstantManipulateExec::new(
            explain_settings(),
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
        check_single_child_exec(&exec, input, "InstantManipulateExec");
    }

    #[tokio::test]
    async fn selects_latest_sample_within_lookback_for_each_grid_step() {
        let mem = physical_leaf(vec![time_value_batch(vec![0, 60_000], vec![1.0, 2.0])]);

        let exec = InstantManipulateExec::new(
            InstantManipulateSettings {
                start_ms: 0,
                end_ms: 120_000,
                step_ms: 60_000,
                lookback_delta_ms: 300_000,
                ..explain_settings()
            },
            mem,
        );
        let merged = collect_concat(Arc::new(exec)).await;
        assert2::assert!(int64_values(&merged, "timestamp") == vec![0, 60_000, 120_000]);
        assert2::assert!(float64_values(&merged, "value") == vec![1.0, 2.0, 2.0]);
    }

    #[tokio::test]
    async fn excludes_sample_at_exact_lookback_delta() {
        let mem = physical_leaf(vec![time_value_batch(vec![0], vec![1.0])]);

        let exec = InstantManipulateExec::new(
            InstantManipulateSettings {
                start_ms: 300_000,
                end_ms: 300_000,
                step_ms: 60_000,
                lookback_delta_ms: 300_000,
                ..explain_settings()
            },
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
        // Two single-row batches so each series is normalized independently.
        let genuine = time_value_batch(vec![0], vec![f64::NAN]);
        let staled = time_value_batch(vec![0], vec![stale]);
        let schema = genuine.schema();
        let mem =
            MemorySourceConfig::try_new_exec(&[vec![genuine], vec![staled]], schema, None).unwrap();

        let exec = InstantManipulateExec::new(
            InstantManipulateSettings {
                start_ms: 0,
                end_ms: 0,
                step_ms: 60_000,
                lookback_delta_ms: 300_000,
                ..explain_settings()
            },
            mem,
        );
        let val = float64_values(&collect_concat(Arc::new(exec)).await, "value");
        // Exactly one row survives: the genuine NaN. The stale marker is dropped.
        check!(val.len() == 1);
        check!(val[0].is_nan());
        check!(!super::super::is_stale_nan(val[0]));
    }
}

mod instant_manipulate_exec;
mod instant_manipulate_type;

pub use instant_manipulate_exec::InstantManipulateExec;
pub use instant_manipulate_type::{InstantManipulate, InstantManipulateSettings};
