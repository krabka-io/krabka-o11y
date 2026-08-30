use super::*;

pub(crate) fn exact_tag_value_filter(query: &str, tag: &str) -> Result<Option<TypedValue>, TraceqlError> {
    let query = krabka_traceql::parse(query)?;
    if !query.pipeline.is_empty() {
        return Ok(None);
    }
    let SpansetExpr::Selector(expr) = query.root else {
        return Ok(None);
    };
    let FieldExpr::Comparison { lhs, op, rhs } = *expr else {
        return Ok(None);
    };
    if op != ComparisonOp::Eq || !field_matches_tag(&lhs, tag) {
        return Ok(None);
    }
    Ok(typed_traceql_value(&rhs))
}
