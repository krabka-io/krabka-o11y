use super::{ExtendedSelectorModifier, Expr, ExtensionExpr, Any, ValueType, Arc};

#[derive(Debug, Clone)]
pub struct ExtendedSelectorExpr {
    pub(crate) modifier: ExtendedSelectorModifier,
    pub(crate) children: Vec<Expr>,
}

impl ExtendedSelectorExpr {
    #[must_use]
    pub fn modifier(&self) -> ExtendedSelectorModifier {
        self.modifier
    }

    #[must_use]
    pub fn child(&self) -> Option<&Expr> {
        self.children.first()
    }
}

impl ExtensionExpr for ExtendedSelectorExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.modifier.keyword()
    }

    fn value_type(&self) -> ValueType {
        self.child().map_or(ValueType::Vector, Expr::value_type)
    }

    fn children(&self) -> &[Expr] {
        &self.children
    }

    fn with_new_children(&self, children: Vec<Expr>) -> Arc<dyn ExtensionExpr> {
        Arc::new(Self {
            modifier: self.modifier,
            children,
        })
    }
}
