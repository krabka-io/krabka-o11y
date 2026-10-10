//! Custom `DataFusion` operators used to model `PromQL` vectors.
//!
//! These nodes carry window widths: a step, a lookback delta, a range, and a
//! grid interval. The widths are extents, but they stay raw `i64` milliseconds
//! here instead of [`Time`](krabka_units::Time) quantities.
//! `UserDefinedLogicalNodeCore` needs `Eq` and `Hash` so that the `DataFusion`
//! planner can key on nodes and deduplicate them, and a quantity stores `f64`,
//! so a quantity can be neither. The paired `*Exec` nodes hold the same raw
//! integers, which also keeps the per-row timestamp arithmetic in integer
//! space. The seam is the planner in [`crate::planner`], which converts a `Time`
//! into milliseconds exactly once as it builds the node.

/// Implements the [`ExecutionPlan`](datafusion::physical_plan::ExecutionPlan)
/// methods every single-input `Prom*Exec` node shares.
///
/// The node exposes its cached `properties` field, its one `input` field as its
/// only child, and no physical expressions. Trait methods cannot come from a
/// helper function, so a macro writes them into each `impl ExecutionPlan`.
macro_rules! single_input_exec_plumbing {
    () => {
        fn properties(&self) -> &::std::sync::Arc<::datafusion::physical_plan::PlanProperties> {
            &self.properties
        }

        fn children(
            &self,
        ) -> Vec<&::std::sync::Arc<dyn ::datafusion::physical_plan::ExecutionPlan>> {
            vec![&self.input]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &::std::sync::Arc<dyn ::datafusion::physical_expr::PhysicalExpr>,
            ) -> ::datafusion::common::Result<
                ::datafusion::common::tree_node::TreeNodeRecursion,
            >,
        ) -> ::datafusion::common::Result<::datafusion::common::tree_node::TreeNodeRecursion> {
            Ok(::datafusion::common::tree_node::TreeNodeRecursion::Continue)
        }
    };
}

/// Implements the [`ExecutionPlan`](datafusion::physical_plan::ExecutionPlan)
/// methods every `Prom*Exec` node built from a `settings` field shares.
///
/// The node keeps no input order, rebuilds itself from its `settings` over a
/// new child, and maps each input batch through `self.$transform_batch`. Like
/// [`single_input_exec_plumbing`], it is a macro because it writes trait
/// methods.
macro_rules! settings_batch_exec_methods {
    ($transform_batch:ident) => {
        fn maintains_input_order(&self) -> Vec<bool> {
            vec![false]
        }

        fn with_new_children(
            self: ::std::sync::Arc<Self>,
            children: Vec<::std::sync::Arc<dyn ::datafusion::physical_plan::ExecutionPlan>>,
        ) -> ::datafusion::common::Result<
            ::std::sync::Arc<dyn ::datafusion::physical_plan::ExecutionPlan>,
        > {
            let input = $crate::extension::only_child(children, self.name())?;
            Ok(::std::sync::Arc::new(Self::new(
                self.settings.clone(),
                input,
            )))
        }

        fn execute(
            &self,
            partition: usize,
            context: ::std::sync::Arc<::datafusion::execution::TaskContext>,
        ) -> ::datafusion::common::Result<::datafusion::physical_plan::SendableRecordBatchStream> {
            let input = self.input.execute(partition, context)?;
            let schema = self.schema();
            let this = self.clone();
            Ok($crate::extension::map_batches(
                input,
                schema,
                move |batch| this.$transform_batch(batch),
            ))
        }
    };
}

pub mod instant_manipulate;
pub mod normalize;
pub mod planner;
pub mod range_manipulate;
pub mod series_divide;

mod batch_rows;
mod is_stale_nan;
mod only_child;
mod stale_nan_bits;
#[cfg(test)]
mod test_support;

pub(crate) use batch_rows::{RowSelection, TimeColumn, map_batches, take_rows_with_timestamps};
pub(crate) use is_stale_nan::is_stale_nan;
pub(crate) use only_child::only_child;
pub(crate) use stale_nan_bits::STALE_NAN_BITS;
