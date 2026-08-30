use super::*;

pub(crate) fn collect_nested_selectors(expr: &SpansetExpr, out: &mut Vec<FieldExpr>) {
    match expr {
        SpansetExpr::Selector(fe) if selector::has_nested_scope(fe) => {
            if !out.iter().any(|existing| existing == fe.as_ref()) {
                out.push((**fe).clone());
            }
        }
        SpansetExpr::Selector(_) => {}
        SpansetExpr::And(lhs, rhs)
        | SpansetExpr::Or(lhs, rhs)
        | SpansetExpr::Structural { lhs, rhs, .. } => {
            collect_nested_selectors(lhs, out);
            collect_nested_selectors(rhs, out);
        }
    }
}
