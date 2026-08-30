use super::*;

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
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) | FieldExpr::Const(_) => {}
    }
}
