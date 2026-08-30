//! `TraceQL` abstract syntax tree.

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn ast_constructs_selector_and_pipeline() {
        let q = Query {
            root: SpansetExpr::Selector(Box::new(FieldExpr::Comparison {
                lhs: Field {
                    scope: Scope::Both,
                    key: "foo".into(),
                },
                op: ComparisonOp::Eq,
                rhs: Value::Int(1),
            })),
            pipeline: vec![Pipeline::Aggregate(Aggregate::Count)],
            hints: QueryHints::default(),
        };
        assert!(matches!(q.root, SpansetExpr::Selector(_)));
        assert!(q.pipeline == vec![Pipeline::Aggregate(Aggregate::Count)]);
    }
}

// === split-modules: generated submodules ===
mod aggregate;
mod comparison_op;
mod field;
mod field_expr;
mod intrinsic;
mod pipeline;
mod query;
mod query_hints;
mod scope;
mod spanset_expr;
mod structural_op;
mod value;
mod with_binding;

pub use aggregate::Aggregate;
pub use comparison_op::ComparisonOp;
pub use field::Field;
pub use field_expr::FieldExpr;
pub use intrinsic::Intrinsic;
pub use pipeline::Pipeline;
pub use query::Query;
pub use query_hints::QueryHints;
pub use scope::Scope;
pub use spanset_expr::SpansetExpr;
pub use structural_op::StructuralOp;
pub use value::Value;
pub use with_binding::WithBinding;
