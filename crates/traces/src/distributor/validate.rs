use super::*;

/// Validate decoded spans against per-tenant structural limits.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn validate(spans: &[Span], limits: &TenantLimits) -> Result<(), TracesError> {
    if spans.len() > limits.max_spans_per_request {
        return Err(TracesError::Limit(format!(
            "span count {} exceeds limit {}",
            spans.len(),
            limits.max_spans_per_request
        )));
    }
    let mut spans_per_trace = BTreeMap::new();
    for span in spans {
        let count = spans_per_trace
            .entry(span.trace_id)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        if *count > limits.max_spans_per_trace {
            return Err(TracesError::Limit(format!(
                "trace span count {} exceeds limit {}",
                count, limits.max_spans_per_trace
            )));
        }
        validate_attrs(&span.resource_attrs, limits)?;
        validate_attrs(&span.span_attrs, limits)?;
        for event in &span.events {
            validate_attrs(&event.attrs, limits)?;
        }
        for link in &span.links {
            validate_attrs(&link.attrs, limits)?;
        }
    }
    Ok(())
}
