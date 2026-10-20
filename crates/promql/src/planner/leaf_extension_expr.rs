use std::{any::Any, fmt, sync::Arc};

use promql_parser::parser::{Expr, ast::ExtensionExpr, value::ValueType};

/// The payload of an extension node that has no children.
pub(crate) trait LeafExtension: Clone + fmt::Debug + Send + Sync + 'static {
    /// The node's extension name.
    const NAME: &'static str;

    /// The type of value the node evaluates to.
    fn leaf_value_type(&self) -> ValueType;
}

/// The [`ExtensionExpr`] node of a [`LeafExtension`] payload.
///
/// `as_any` and `Debug` both expose the payload itself, so callers downcast
/// to the payload type, and node equality compares the payloads.
#[derive(Clone)]
pub(crate) struct LeafExtensionExpr<T>(pub(crate) T);

impl<T: fmt::Debug> fmt::Debug for LeafExtensionExpr<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<T: LeafExtension> ExtensionExpr for LeafExtensionExpr<T> {
    fn as_any(&self) -> &dyn Any {
        &self.0
    }
    fn name(&self) -> &'static str {
        T::NAME
    }
    fn value_type(&self) -> ValueType {
        self.0.leaf_value_type()
    }
    fn children(&self) -> &[Expr] {
        &[]
    }
    fn with_new_children(&self, children: Vec<Expr>) -> Arc<dyn ExtensionExpr> {
        assert2::assert!(children.is_empty());
        Arc::new(self.clone())
    }
}
