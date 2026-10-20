use super::{CompareRegexCache, ComparisonOp, FieldExpr, SpansetExpr, Value};

// Collect Field Expr Regexes
pub(crate) fn collect_field_expr_regexes(fe: &FieldExpr, cache: &mut CompareRegexCache) {
    match fe {
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            collect_field_expr_regexes(a, cache);
            collect_field_expr_regexes(b, cache);
        }
        FieldExpr::Not(inner) => collect_field_expr_regexes(inner, cache),
        FieldExpr::Comparison {
            op: ComparisonOp::Re | ComparisonOp::Nre,
            rhs: Value::Str(pattern),
            ..
        } => {
            if !cache.contains_key(pattern)
                && let Ok(re) = regex::Regex::new(&format!("^(?:{pattern})$"))
            {
                cache.insert(pattern.clone(), re);
            }
        }
        FieldExpr::ExpressionComparison { .. }
        | FieldExpr::FieldComparison { .. }
        | FieldExpr::Comparison { .. }
        | FieldExpr::Field(_)
        | FieldExpr::Const(_) => {}
    }
}

// Collect Selection Regexes
/// Collects every `=~`/`!~` literal pattern in the selection.
///
/// This function compiles each pattern once into `cache`. It skips a pattern
/// that does not compile.
pub(crate) fn collect_selection_regexes(selection: &SpansetExpr, cache: &mut CompareRegexCache) {
    match selection {
        SpansetExpr::Selector(fe) => collect_field_expr_regexes(fe, cache),
        SpansetExpr::And(lhs, rhs) | SpansetExpr::Or(lhs, rhs) => {
            collect_selection_regexes(lhs, cache);
            collect_selection_regexes(rhs, cache);
        }
        SpansetExpr::Structural { .. } => {}
    }
}
