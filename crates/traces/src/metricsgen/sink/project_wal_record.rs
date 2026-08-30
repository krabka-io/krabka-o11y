use super::*;

#[must_use]
pub fn project_wal_record(record: wal::SpanRecord, size: ByteSize) -> SpanRecord {
    let service_name = service_name(&record.span.resource_attrs);
    let attributes = record
        .span
        .span_attrs
        .iter()
        .chain(record.span.resource_attrs.iter())
        .filter(|kv| kv.key != "service.name")
        .map(|kv| (kv.key.clone(), attr_value_to_string(&kv.value)))
        .collect();

    SpanRecord {
        tenant: record.tenant,
        trace_id: record.span.trace_id,
        span_id: record.span.span_id,
        parent_span_id: record.span.parent_span_id.unwrap_or([0; 8]),
        name: record.span.name,
        kind: record.span.kind,
        start_ns: record.span.start_ns,
        duration_ns: record.span.duration_ns,
        status: record.span.status,
        status_message: record.span.status_message,
        service_name,
        attributes,
        size,
    }
}
