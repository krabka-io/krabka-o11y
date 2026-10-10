use std::sync::Arc;

use promql_parser::parser::{Expr, Extension, value::ValueType};

use super::leaf_extension_expr::{LeafExtension, LeafExtensionExpr};
use crate::PromqlString;

#[derive(Clone, Debug)]
pub(crate) struct ByteStringExpr(pub(crate) PromqlString);

impl LeafExtension for ByteStringExpr {
    const NAME: &'static str = "byteString";

    fn leaf_value_type(&self) -> ValueType {
        ValueType::String
    }
}

pub(crate) fn string_expr_value(expr: &Expr) -> Option<PromqlString> {
    match expr {
        Expr::StringLiteral(literal) => Some(literal.val.clone().into()),
        Expr::Paren(paren) => string_expr_value(&paren.expr),
        Expr::Extension(extension) => extension
            .expr
            .as_any()
            .downcast_ref::<ByteStringExpr>()
            .map(|literal| literal.0.clone()),
        _ => None,
    }
}

pub(crate) fn restore_byte_strings(
    expr: &mut Expr,
    values: &std::collections::BTreeMap<String, PromqlString>,
) {
    super::byte_selector_expr::restore_byte_selector(expr, values);
    match expr {
        Expr::StringLiteral(literal) => {
            if let Some(value) = values.get(&literal.val) {
                *expr = Expr::Extension(Extension {
                    expr: Arc::new(LeafExtensionExpr(ByteStringExpr(value.clone()))),
                });
            }
        }
        Expr::Binary(binary) => {
            restore_byte_strings(&mut binary.lhs, values);
            restore_byte_strings(&mut binary.rhs, values);
        }
        Expr::Aggregate(aggregate) => {
            if let Some(parameter) = &mut aggregate.param {
                restore_byte_strings(parameter, values);
            }
            restore_byte_strings(&mut aggregate.expr, values);
        }
        Expr::Call(call) => {
            for argument in &mut call.args.args {
                restore_byte_strings(argument, values);
            }
        }
        Expr::Paren(paren) => restore_byte_strings(&mut paren.expr, values),
        Expr::Unary(unary) => restore_byte_strings(&mut unary.expr, values),
        Expr::Subquery(subquery) => restore_byte_strings(&mut subquery.expr, values),
        _ => {}
    }
}
