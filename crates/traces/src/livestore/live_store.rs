use krabka_traceql::{TagCatalog, TagNameGroups, TagNameSets};

use super::{
    BTreeMap, BTreeSet, EVENT_TAGS, INTRINSIC_TAGS, LINK_TAGS, LiveResult, LiveSource, MemTable,
    RecordBatch, Span, SpanRecord, TracesError, UnixNano, collect_event_values,
    collect_link_values, collect_span_intrinsic_value, collect_trace_intrinsic_values,
    in_time_range, order_spans, span_rows_for_window, trace_spans, typed_value_parts,
};

const SPAN_BATCH_ROWS: usize = 8_192;

// Arrival slots preserve duplicate spans and stable query ordering, while allowing
// expiration to remove an individual span without scanning or shifting a trace.
type TraceSpans = BTreeMap<usize, Span>;
type Expiration = (i64, String, [u8; 16], usize);

/// In-memory recent span store keyed by tenant and trace id.
#[derive(Debug)]
pub struct LiveStore {
    pub(crate) retention_ns: i64,
    pub(crate) max_start_ns: i64,
    expirations: BTreeSet<Expiration>,
    pub(crate) by_tenant: BTreeMap<String, BTreeMap<[u8; 16], TraceSpans>>,
}

impl LiveStore {
    /// Create a live store that retains spans within `retention_ns` of the
    /// newest ingested span timestamp.
    #[must_use]
    pub fn new(retention_ns: i64) -> Self {
        Self {
            retention_ns,
            max_start_ns: i64::MIN,
            by_tenant: BTreeMap::new(),
            expirations: BTreeSet::new(),
        }
    }

    /// Append a WAL span record within the latest retention window.
    pub fn ingest(&mut self, rec: SpanRecord) {
        if rec.span.start_ns > self.max_start_ns {
            self.max_start_ns = rec.span.start_ns;
            self.evict_old();
        }
        if self.retention_ns != i64::MAX
            && self.max_start_ns != i64::MIN
            && rec.span.start_ns < self.max_start_ns.saturating_sub(self.retention_ns)
        {
            return;
        }
        let spans = self
            .by_tenant
            .entry(rec.tenant.clone())
            .or_default()
            .entry(rec.span.trace_id)
            .or_default();
        let slot = spans.last_key_value().map_or(0, |(slot, _)| slot + 1);
        if self.retention_ns != i64::MAX {
            self.expirations
                .insert((rec.span.start_ns, rec.tenant, rec.span.trace_id, slot));
        }
        spans.insert(slot, rec.span);
    }

    /// Return recent spans for one trace, ordered by start time and span id.
    #[must_use]
    pub fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Vec<Span> {
        let mut spans = self
            .by_tenant
            .get(tenant)
            .and_then(|traces| traces.get(trace_id))
            .map_or_else(Vec::new, |spans| spans.values().cloned().collect());
        order_spans(&mut spans);
        spans
    }

    /// Expose a tenant's recent spans as a `DataFusion` `MemTable`.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn mem_table(&self, tenant: &str) -> Result<MemTable, TracesError> {
        let schema = krabka_blockstore::span_block_schema();
        let batches = self.batches_in_window(tenant, i64::MIN, i64::MAX)?;
        MemTable::try_new(schema, vec![batches]).map_err(|err| TracesError::Block(err.to_string()))
    }

    fn batches_in_window(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>, TracesError> {
        let mut batches = Vec::new();
        let mut rows = Vec::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for spans in traces.values() {
                let mut in_range = spans
                    .values()
                    .filter(|span| in_time_range(span, UnixNano(start_ns), UnixNano(end_ns)))
                    .collect::<Vec<_>>();
                if in_range.is_empty() {
                    continue;
                }
                order_spans(&mut in_range);
                // Nested sets describe this trace's selected rows; root columns
                // describe its complete span set, even when the window clips it.
                for row in span_rows_for_window(&in_range, spans.values()) {
                    rows.push(row);
                    if rows.len() == SPAN_BATCH_ROWS {
                        batches.push(
                            krabka_blockstore::encode_span_rows(&rows)
                                .map_err(|err| TracesError::Block(err.to_string()))?,
                        );
                        rows.clear();
                    }
                }
            }
        }
        if !rows.is_empty() {
            batches.push(
                krabka_blockstore::encode_span_rows(&rows)
                    .map_err(|err| TracesError::Block(err.to_string()))?,
            );
        }
        Ok(batches)
    }

    pub(crate) fn evict_old(&mut self) {
        if self.retention_ns == i64::MAX || self.max_start_ns == i64::MIN {
            return;
        }
        let cutoff = self.max_start_ns.saturating_sub(self.retention_ns);
        // Only visit expired entries. A frontier advance with no expiration
        // checks the first key, regardless of the number of retained spans.
        while self
            .expirations
            .first()
            .is_some_and(|entry| entry.0 < cutoff)
        {
            let (_, tenant, trace_id, slot) = self.expirations.pop_first().unwrap();
            let traces = self.by_tenant.get_mut(&tenant).expect("indexed tenant");
            let spans = traces.get_mut(&trace_id).expect("indexed trace");
            spans.remove(&slot);
            if spans.is_empty() {
                traces.remove(&trace_id);
            }
            if traces.is_empty() {
                self.by_tenant.remove(&tenant);
            }
        }
    }
}

#[async_trait::async_trait]
impl LiveSource for LiveStore {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> LiveResult<Vec<RecordBatch>> {
        self.batches_in_window(tenant, start_ns, end_ns)
            .map_err(|err| krabka_traceql::TraceqlError::Store(err.to_string()))
    }

    async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> LiveResult<Option<krabka_traceql::TraceSpans>> {
        let spans = self.trace_by_id(tenant, trace_id);
        Ok((!spans.is_empty()).then(|| trace_spans(trace_id, &spans)))
    }

    fn block_builder_frontier_ns(&self, _tenant: &str) -> i64 {
        if self.max_start_ns == i64::MIN {
            0
        } else {
            self.max_start_ns
        }
    }
}

#[async_trait::async_trait]
impl TagCatalog for LiveStore {
    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<krabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> LiveResult<Vec<krabka_traceql::ScopedTag>> {
        let mut tags = TagNameSets::default();
        let mut has_spans = false;
        if let Some(traces) = self.by_tenant.get(tenant) {
            for item in traces
                .values()
                .flat_map(|spans| spans.values())
                .filter(|item| in_time_range(item, UnixNano(start_ns), UnixNano(end_ns)))
            {
                has_spans = true;
                tags.resource
                    .extend(item.resource_attrs.iter().map(|attr| attr.key.clone()));
                tags.span
                    .extend(item.span_attrs.iter().map(|attr| attr.key.clone()));
                for event_record in &item.events {
                    tags.event
                        .extend(EVENT_TAGS.iter().map(|tag| (*tag).to_string()));
                    tags.event
                        .extend(event_record.attrs.iter().map(|attr| attr.key.clone()));
                }
                for link_record in &item.links {
                    tags.link
                        .extend(LINK_TAGS.iter().map(|tag| (*tag).to_string()));
                    tags.link
                        .extend(link_record.attrs.iter().map(|attr| attr.key.clone()));
                }
                tags.add_instrumentation(
                    &item.instrumentation_scope,
                    &item.instrumentation_version,
                );
            }
        }

        Ok(TagNameGroups {
            resource: tags.resource,
            span: tags.span,
            event: tags.event,
            link: tags.link,
            instrumentation: tags.instrumentation,
        }
        .into_scoped_tags(scope, has_spans, INTRINSIC_TAGS))
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> LiveResult<Vec<krabka_traceql::TypedValue>> {
        let parsed_tag = krabka_traceql::TagScope::parse_attribute_tag(tag);
        let tag = parsed_tag.tag;
        let attr_tag = parsed_tag.attribute.tag;
        let attr_scope = parsed_tag.attribute.scope;
        let mut values = BTreeSet::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for spans in traces.values() {
                let in_range = spans
                    .values()
                    .filter(|span| in_time_range(span, UnixNano(start_ns), UnixNano(end_ns)))
                    .collect::<Vec<_>>();
                collect_trace_intrinsic_values(&in_range, tag, &mut values);
            }
            for span in traces
                .values()
                .flat_map(|spans| spans.values())
                .filter(|item| in_time_range(item, UnixNano(start_ns), UnixNano(end_ns)))
            {
                if matches!(attr_scope, None | Some(krabka_traceql::TagScope::Resource)) {
                    values.extend(
                        span.resource_attrs
                            .iter()
                            .filter(|attr| attr.key == attr_tag)
                            .flat_map(|attr| typed_value_parts(&attr.value)),
                    );
                }
                if matches!(attr_scope, None | Some(krabka_traceql::TagScope::Span)) {
                    values.extend(
                        span.span_attrs
                            .iter()
                            .filter(|attr| attr.key == attr_tag)
                            .flat_map(|attr| typed_value_parts(&attr.value)),
                    );
                }
                collect_span_intrinsic_value(span, tag, &mut values);
                collect_event_values(span, tag, &mut values);
                collect_link_values(span, tag, &mut values);
                if tag == "instrumentation:name" && !span.instrumentation_scope.is_empty() {
                    values.insert(krabka_traceql::TypedValue::new(
                        "string",
                        span.instrumentation_scope.clone(),
                    ));
                }
                if tag == "instrumentation:version" && !span.instrumentation_version.is_empty() {
                    values.insert(krabka_traceql::TypedValue::new(
                        "string",
                        span.instrumentation_version.clone(),
                    ));
                }
            }
        }
        Ok(values.into_iter().collect())
    }
}

#[cfg(test)]
mod retention_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn expiration_index_is_bounded_and_empty_tenants_are_pruned() {
        let mut store = LiveStore::new(10);
        for time in 0..1_000 {
            store.ingest(SpanRecord {
                tenant: format!("tenant-{time}"),
                span: Span {
                    start_ns: time,
                    ..Span::default()
                },
            });
            assert!(store.expirations.len() == store.by_tenant.len());
            assert!(store.expirations.len() <= 11);
        }
        store.ingest(SpanRecord {
            tenant: "last".into(),
            span: Span {
                start_ns: i64::MAX,
                ..Span::default()
            },
        });
        assert!(store.by_tenant.len() == 1);
        assert!(store.expirations.len() == 1);
        assert!(store.by_tenant.contains_key("last"));
    }

    #[test]
    fn unbounded_retention_does_not_build_an_expiration_index() {
        let mut store = LiveStore::new(i64::MAX);
        for time in [i64::MIN, 0, i64::MAX, -1] {
            store.ingest(SpanRecord {
                tenant: "t".into(),
                span: Span {
                    start_ns: time,
                    ..Span::default()
                },
            });
        }
        assert!(store.expirations.is_empty());
        assert!(store.trace_by_id("t", &[0; 16]).len() == 4);
    }
}
