//! Inputs and contract checks shared by the extension operators' unit tests.

use std::sync::Arc;

use arrow::{
    array::{Array, Float64Array, Int64Array},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assert2::check;
use datafusion::{
    catalog::MemTable,
    common::{plan_err, tree_node::TreeNodeRecursion},
    datasource::memory::MemorySourceConfig,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
    physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions, collect},
    prelude::SessionContext,
};

/// One batch with an `Int64` `timestamp` column and a `Float64` `value` column.
pub(crate) fn time_value_batch(timestamps: Vec<i64>, sample_values: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(sample_values)),
        ],
    )
    .unwrap()
}

/// An optimized logical scan of `batch`, registered as the table `leaf`.
pub(crate) async fn logical_leaf(batch: RecordBatch) -> LogicalPlan {
    let schema = batch.schema();
    let session = SessionContext::new();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    session.register_table("leaf", Arc::new(table)).unwrap();
    session
        .table("leaf")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap()
}

/// A one-partition in-memory physical scan of `batches`.
pub(crate) fn physical_leaf(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    let schema = batches[0].schema();
    MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap()
}

/// Checks a logical node's rewrite contract: it takes no expressions and
/// exactly one input. Returns the valid rewrite over `input`.
pub(crate) fn checked_rewrite<N: UserDefinedLogicalNodeCore>(
    node: &N,
    input: &LogicalPlan,
    expr: Expr,
) -> N {
    check!(
        node.with_exprs_and_inputs(vec![expr], vec![input.clone()])
            .is_err()
    );
    check!(node.with_exprs_and_inputs(vec![], vec![]).is_err());
    node.with_exprs_and_inputs(vec![], vec![input.clone()])
        .expect("valid rewrite")
}

/// Checks a physical node's child contract: it owns no expressions, rejects
/// a rebuild without its one child, and rebuilds over `input` as `name`.
pub(crate) fn check_single_child_exec(
    exec: &Arc<dyn ExecutionPlan>,
    input: Arc<dyn ExecutionPlan>,
    name: &str,
) {
    // The plan owns no expression, so a visitor that fails must never run.
    let walk = exec.apply_expressions(&mut |_| plan_err!("visited an expression"));
    check!(let Ok(TreeNodeRecursion::Continue) = walk);
    check!(
        Arc::clone(exec)
            .replace_children(
                vec![],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute)
            )
            .is_err()
    );
    check!(
        Arc::clone(exec)
            .replace_children(
                vec![input],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute)
            )
            .expect("valid child rewrite")
            .name()
            == name
    );
}

/// Runs `exec` and concatenates its output batches.
pub(crate) async fn collect_concat(exec: Arc<dyn ExecutionPlan>) -> RecordBatch {
    let session = SessionContext::new();
    let batches = collect(exec, session.task_ctx()).await.unwrap();
    concat_batches(&batches[0].schema(), &batches).unwrap()
}

/// The values of `batch`'s `Int64` column `name`.
pub(crate) fn int64_values(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let column = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..column.len()).map(|index| column.value(index)).collect()
}

/// The values of `batch`'s `Float64` column `name`.
pub(crate) fn float64_values(batch: &RecordBatch, name: &str) -> Vec<f64> {
    let column = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..column.len()).map(|index| column.value(index)).collect()
}
