use super::*;

/// In-memory recent span store keyed by tenant and trace id.
#[derive(Debug)]
pub struct LiveStore {
    pub(crate) retention_ns: i64,
    pub(crate) max_start_ns: i64,
    pub(crate) by_tenant: BTreeMap<String, BTreeMap<[u8; 16], Vec<Span>>>,
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
        }
    }

    /// Append a WAL span record, then evict spans older than the retention
    /// window.
    pub fn ingest(&mut self, rec: SpanRecord) {
        self.max_start_ns = self.max_start_ns.max(rec.span.start_ns);
        self.by_tenant
            .entry(rec.tenant)
            .or_default()
            .entry(rec.span.trace_id)
            .or_default()
            .push(rec.span);
        self.evict_old();
    }

    /// Return recent spans for one trace, ordered by start time and span id.
    #[must_use]
    pub fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Vec<Span> {
        let mut spans = self
            .by_tenant
            .get(tenant)
            .and_then(|traces| traces.get(trace_id))
            .cloned()
            .unwrap_or_default();
        order_spans(&mut spans);
        spans
    }

    /// Expose a tenant's recent spans as a `DataFusion` `MemTable`.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn mem_table(&self, tenant: &str) -> Result<MemTable, TracesError> {
        let schema = krabka_blockstore::span_block_schema();
        let mut batches = Vec::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for spans in traces.values() {
                let mut ordered = spans.clone();
                order_spans(&mut ordered);
                batches.push(span_batch(&ordered)?);
            }
        }
        MemTable::try_new(schema, vec![batches]).map_err(|err| TracesError::Block(err.to_string()))
    }

    pub(crate) fn evict_old(&mut self) {
        if self.retention_ns == i64::MAX || self.max_start_ns == i64::MIN {
            return;
        }
        let cutoff = self.max_start_ns.saturating_sub(self.retention_ns);
        self.by_tenant.retain(|_, traces| {
            traces.retain(|_, spans| {
                spans.retain(|span| span.start_ns >= cutoff);
                !spans.is_empty()
            });
            !traces.is_empty()
        });
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
        let mut batches = Vec::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for spans in traces.values() {
                let mut in_range = spans
                    .iter()
                    .filter(|span| in_time_range(span, UnixNano(start_ns), UnixNano(end_ns)))
                    .cloned()
                    .collect::<Vec<_>>();
                if !in_range.is_empty() {
                    order_spans(&mut in_range);
                    // Rows come from the in-window subset, but trace-level
                    // columns (root service/name, start, duration) must reflect
                    // the FULL trace so a window that clips the trace does not
                    // skew them. `spans` is the complete per-trace span set.
                    batches.push(
                        span_batch_for_window(&in_range, spans, &[])
                            .map_err(|err| krabka_traceql::TraceqlError::Store(err.to_string()))?,
                    );
                }
            }
        }
        Ok(batches)
    }

    async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> LiveResult<Option<krabka_traceql::TraceSpans>> {
        let spans = self.trace_by_id(tenant, trace_id);
        Ok((!spans.is_empty()).then(|| trace_spans(trace_id, &spans)))
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<krabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> LiveResult<Vec<krabka_traceql::ScopedTag>> {
        let mut resource = BTreeSet::new();
        let mut span = BTreeSet::new();
        let mut event = BTreeSet::new();
        let mut link = BTreeSet::new();
        let mut instrumentation = BTreeSet::new();
        let mut has_spans = false;
        if let Some(traces) = self.by_tenant.get(tenant) {
            for item in traces
                .values()
                .flatten()
                .filter(|item| in_time_range(item, UnixNano(start_ns), UnixNano(end_ns)))
            {
                has_spans = true;
                resource.extend(item.resource_attrs.iter().map(|attr| attr.key.clone()));
                span.extend(item.span_attrs.iter().map(|attr| attr.key.clone()));
                for event_record in &item.events {
                    event.extend(EVENT_TAGS.iter().map(|tag| (*tag).to_string()));
                    event.extend(event_record.attrs.iter().map(|attr| attr.key.clone()));
                }
                for link_record in &item.links {
                    link.extend(LINK_TAGS.iter().map(|tag| (*tag).to_string()));
                    link.extend(link_record.attrs.iter().map(|attr| attr.key.clone()));
                }
                if !item.instrumentation_scope.is_empty() {
                    instrumentation.insert("instrumentation:name".to_string());
                }
                if !item.instrumentation_version.is_empty() {
                    instrumentation.insert("instrumentation:version".to_string());
                }
            }
        }

        let mut out = Vec::new();
        if matches!(scope, None | Some(krabka_traceql::TagScope::Resource)) && !resource.is_empty()
        {
            out.push(krabka_traceql::ScopedTag {
                scope: krabka_traceql::TagScope::Resource,
                tags: resource.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(krabka_traceql::TagScope::Span)) && !span.is_empty() {
            out.push(krabka_traceql::ScopedTag {
                scope: krabka_traceql::TagScope::Span,
                tags: span.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(krabka_traceql::TagScope::Event)) && !event.is_empty() {
            out.push(krabka_traceql::ScopedTag {
                scope: krabka_traceql::TagScope::Event,
                tags: event.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(krabka_traceql::TagScope::Link)) && !link.is_empty() {
            out.push(krabka_traceql::ScopedTag {
                scope: krabka_traceql::TagScope::Link,
                tags: link.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(krabka_traceql::TagScope::Intrinsic)) && has_spans {
            out.push(krabka_traceql::ScopedTag {
                scope: krabka_traceql::TagScope::Intrinsic,
                tags: INTRINSIC_TAGS
                    .iter()
                    .map(|tag| (*tag).to_string())
                    .collect(),
            });
        }
        if matches!(
            scope,
            None | Some(krabka_traceql::TagScope::Instrumentation)
        ) && !instrumentation.is_empty()
        {
            out.push(krabka_traceql::ScopedTag {
                scope: krabka_traceql::TagScope::Instrumentation,
                tags: instrumentation.into_iter().collect(),
            });
        }
        Ok(out)
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> LiveResult<Vec<krabka_traceql::TypedValue>> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        let (attr_tag, attr_scope) = scoped_attribute_tag(tag);
        let mut values = BTreeSet::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for spans in traces.values() {
                let in_range = spans
                    .iter()
                    .filter(|span| in_time_range(span, UnixNano(start_ns), UnixNano(end_ns)))
                    .collect::<Vec<_>>();
                collect_trace_intrinsic_values(&in_range, tag, &mut values);
            }
            for span in traces
                .values()
                .flatten()
                .filter(|item| in_time_range(item, UnixNano(start_ns), UnixNano(end_ns)))
            {
                if matches!(attr_scope, None | Some(krabka_traceql::TagScope::Resource)) {
                    values.extend(
                        span.resource_attrs
                            .iter()
                            .filter(|attr| attr.key == attr_tag)
                            .map(|attr| typed_value_parts(&attr.value)),
                    );
                }
                if matches!(attr_scope, None | Some(krabka_traceql::TagScope::Span)) {
                    values.extend(
                        span.span_attrs
                            .iter()
                            .filter(|attr| attr.key == attr_tag)
                            .map(|attr| typed_value_parts(&attr.value)),
                    );
                }
                collect_span_intrinsic_value(span, tag, &mut values);
                collect_event_values(span, tag, &mut values);
                collect_link_values(span, tag, &mut values);
                if tag == "instrumentation:name" && !span.instrumentation_scope.is_empty() {
                    values.insert(("string".into(), span.instrumentation_scope.clone()));
                }
                if tag == "instrumentation:version" && !span.instrumentation_version.is_empty() {
                    values.insert(("string".into(), span.instrumentation_version.clone()));
                }
            }
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| krabka_traceql::TypedValue { type_, value })
            .collect())
    }

    fn block_builder_frontier_ns(&self, _tenant: &str) -> i64 {
        if self.max_start_ns == i64::MIN {
            0
        } else {
            self.max_start_ns
        }
    }
}
