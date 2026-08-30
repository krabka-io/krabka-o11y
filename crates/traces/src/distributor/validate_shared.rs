use super::{Span, Limits, TracesError, BTreeMap, IngestEnforcer, limit_error_to_traces_error, check_shared_attrs};

pub(crate) fn validate_shared(spans: &[Span], limits: &Limits) -> Result<(), TracesError> {
    let mut spans_per_trace = BTreeMap::new();
    for span in spans {
        let count = spans_per_trace
            .entry(span.trace_id)
            .and_modify(|count| *count += 1_u64)
            .or_insert(1_u64);
        IngestEnforcer::check_trace_size(limits, *count)
            .map_err(|err| limit_error_to_traces_error(&err))?;
        check_shared_attrs(limits, &span.resource_attrs)?;
        check_shared_attrs(limits, &span.span_attrs)?;
        for event in &span.events {
            check_shared_attrs(limits, &event.attrs)?;
        }
        for link in &span.links {
            check_shared_attrs(limits, &link.attrs)?;
        }
    }
    Ok(())
}
