//! Recent trace hot tier for the traces backend.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use krabka_client_consumer::Consumer;
use krabka_units::{Time, convert::TimeExt as _};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::{
    error::TracesError,
    ids::UnixNano,
    querier::live::{LiveSource, Result as LiveResult},
    span::{
        AttrValue, EventRecord, KeyValue, LinkRecord, Span,
        batch::{span_batch, span_batch_for_window},
        nested_set,
    },
    wal::SpanRecord,
};

const INTRINSIC_TAGS: &[&str] = &[
    "span:childCount",
    "span:duration",
    "span:id",
    "span:kind",
    "span:name",
    "span:Parent",
    "span:parentID",
    "span:nestedSetLeft",
    "span:nestedSetParent",
    "span:nestedSetRight",
    "span:status",
    "span:statusMessage",
    "trace:duration",
    "trace:id",
    "trace:rootName",
    "trace:rootService",
];
const EVENT_TAGS: &[&str] = &["event:name", "event:timeSinceStart"];
const LINK_TAGS: &[&str] = &["link:traceID", "link:spanID"];

/// In-memory recent span store keyed by tenant and trace id.
#[derive(Debug)]
pub struct LiveStore {
    retention_ns: i64,
    max_start_ns: i64,
    by_tenant: BTreeMap<String, BTreeMap<[u8; 16], Vec<Span>>>,
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

    fn evict_old(&mut self) {
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

/// Decode WAL payloads and ingest them into the live store.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn ingest_wal_payloads<'a, I>(store: &mut LiveStore, payloads: I) -> Result<usize, TracesError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut count = 0;
    for payload in payloads {
        store.ingest(SpanRecord::decode(payload)?);
        count += 1;
    }
    Ok(count)
}

/// Consume traces WAL records and rebuild the in-memory hot tier.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn run(
    mut consumer: Consumer,
    store: Arc<RwLock<LiveStore>>,
    shutdown: CancellationToken,
) -> Result<(), TracesError> {
    while !shutdown.is_cancelled() {
        let records = consumer
            .poll(krabka_units::millis(500))
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))?;
        if records.is_empty() {
            continue;
        }

        {
            let payloads = records
                .iter()
                .filter_map(|record| record.value.as_deref())
                .collect::<Vec<_>>();
            let mut guard = store.write().await;
            ingest_wal_payloads(&mut guard, payloads)?;
        }

        if let Err(err) = consumer.commit_sync().await {
            tracing::warn!(error = %err, "live-store offset commit failed");
        }
    }
    Ok(())
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

fn scoped_attribute_tag(tag: &str) -> (&str, Option<krabka_traceql::TagScope>) {
    if let Some(tag) = tag.strip_prefix("resource.") {
        (tag, Some(krabka_traceql::TagScope::Resource))
    } else if let Some(tag) = tag.strip_prefix("span.") {
        (tag, Some(krabka_traceql::TagScope::Span))
    } else {
        (tag, None)
    }
}

fn order_spans(spans: &mut [Span]) {
    spans.sort_by_key(|span| (span.start_ns, span.span_id));
}

fn in_time_range(span: &Span, start_ns: UnixNano, end_ns: UnixNano) -> bool {
    start_ns.0 <= span.start_ns && span.start_ns <= end_ns.0
}

fn trace_spans(trace_id: &[u8; 16], spans: &[Span]) -> krabka_traceql::TraceSpans {
    let root = spans
        .iter()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().min_by_key(|span| span.start_ns));
    krabka_traceql::TraceSpans {
        trace_id: *trace_id,
        root_service_name: root
            .and_then(|span| attr_string(&span.resource_attrs, "service.name"))
            .unwrap_or_default(),
        root_trace_name: root.map(|span| span.name.clone()).unwrap_or_default(),
        resource_attributes: root
            .map(|span| {
                span.resource_attrs
                    .iter()
                    .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
                    .collect()
            })
            .unwrap_or_default(),
        spans: spans
            .iter()
            .zip(nested_set::assign_nested_set(spans))
            .map(|(span, nested)| span_ref(span, nested))
            .collect(),
    }
}

fn span_ref(span: &Span, nested: nested_set::NestedSet) -> krabka_traceql::SpanRef {
    krabka_traceql::SpanRef {
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name.clone(),
        kind: span.kind.as_i32(),
        nested_set_left: nested.left,
        nested_set_right: nested.right,
        nested_set_parent: nested.parent_id,
        start_time_unix_nano: non_negative_u64(span.start_ns),
        duration: Time::from_nanos(span.duration_ns),
        status_code: span.status.as_i32(),
        status_message: span.status_message.clone(),
        instrumentation_name: span.instrumentation_scope.clone(),
        instrumentation_version: span.instrumentation_version.clone(),
        resource_attributes: span
            .resource_attrs
            .iter()
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
        attributes: span
            .span_attrs
            .iter()
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
        events: span
            .events
            .iter()
            .map(|event| event_ref(span, event))
            .collect(),
        links: span.links.iter().map(link_ref).collect(),
    }
}

fn event_ref(span: &Span, event: &EventRecord) -> krabka_traceql::EventRef {
    krabka_traceql::EventRef {
        time_since_start: Time::from_nanos(event.time_unix_nano.saturating_sub(span.start_ns)),
        name: event.name.clone(),
        attributes: event
            .attrs
            .iter()
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
    }
}

fn link_ref(link: &LinkRecord) -> krabka_traceql::LinkRef {
    krabka_traceql::LinkRef {
        trace_id: link.trace_id,
        span_id: link.span_id,
        attributes: link
            .attrs
            .iter()
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
    }
}

fn attr_string(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs.iter().find_map(|attr| {
        (attr.key == key).then(|| match &attr.value {
            AttrValue::Str(value) => Some(value.clone()),
            _ => None,
        })?
    })
}

fn traceql_attr(attr: &KeyValue) -> Option<krabka_traceql::AttrValue> {
    Some(match &attr.value {
        AttrValue::Str(value) => krabka_traceql::AttrValue::Str(value.clone()),
        AttrValue::Int(value) => krabka_traceql::AttrValue::Int(*value),
        AttrValue::Double(value) => krabka_traceql::AttrValue::Float(*value),
        AttrValue::Bool(value) => krabka_traceql::AttrValue::Bool(*value),
        AttrValue::Bytes(_) => return None,
    })
}

fn typed_value_parts(value: &AttrValue) -> (String, String) {
    match value {
        AttrValue::Str(value) => ("string".into(), value.clone()),
        AttrValue::Int(value) => ("int".into(), value.to_string()),
        AttrValue::Double(value) => ("float".into(), value.to_string()),
        AttrValue::Bool(value) => ("bool".into(), value.to_string()),
        AttrValue::Bytes(value) => ("string".into(), hex::encode(value)),
    }
}

fn collect_span_intrinsic_value(span: &Span, tag: &str, values: &mut BTreeSet<(String, String)>) {
    match tag {
        "span:duration" => {
            values.insert(("duration".into(), span.duration_ns.to_string()));
        }
        "span:id" => {
            values.insert(("string".into(), bytes_to_hex(&span.span_id)));
        }
        "span:kind" => {
            values.insert(("int".into(), span.kind.as_i32().to_string()));
        }
        "span:name" => {
            values.insert(("string".into(), span.name.clone()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(("string".into(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            values.insert(("int".into(), span.status.as_i32().to_string()));
        }
        "span:statusMessage" => {
            if !span.status_message.is_empty() {
                values.insert(("string".into(), span.status_message.clone()));
            }
        }
        "trace:id" => {
            values.insert(("string".into(), bytes_to_hex(&span.trace_id)));
        }
        _ => {}
    }
}

fn collect_trace_intrinsic_values(
    spans: &[&Span],
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    if spans.is_empty() {
        return;
    }
    match tag {
        "trace:duration" => {
            let start = spans.iter().map(|span| span.start_ns).min().unwrap_or(0);
            let end = spans
                .iter()
                .map(|span| span.start_ns.saturating_add(span.duration_ns))
                .max()
                .unwrap_or(start);
            values.insert(("duration".into(), end.saturating_sub(start).to_string()));
        }
        "trace:rootName" => {
            if let Some(root) = root_span(spans) {
                values.insert(("string".into(), root.name.clone()));
            }
        }
        "trace:rootService" => {
            if let Some(root) = root_span(spans)
                && let Some(service) = attr_string(&root.resource_attrs, "service.name")
            {
                values.insert(("string".into(), service));
            }
        }
        _ => {}
    }
}

fn collect_event_values(span: &Span, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for event in &span.events {
        match tag {
            "event:name" => {
                values.insert(("string".into(), event.name.clone()));
            }
            "event:timeSinceStart" => {
                values.insert((
                    "duration".into(),
                    event
                        .time_unix_nano
                        .saturating_sub(span.start_ns)
                        .to_string(),
                ));
            }
            _ => {
                values.extend(
                    event
                        .attrs
                        .iter()
                        .filter(|attr| attr.key == tag)
                        .map(|attr| typed_value_parts(&attr.value)),
                );
            }
        }
    }
}

fn collect_link_values(span: &Span, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for link in &span.links {
        match tag {
            "link:traceID" => {
                values.insert(("string".into(), bytes_to_hex(&link.trace_id)));
            }
            "link:spanID" => {
                values.insert(("string".into(), bytes_to_hex(&link.span_id)));
            }
            _ => {
                values.extend(
                    link.attrs
                        .iter()
                        .filter(|attr| attr.key == tag)
                        .map(|attr| typed_value_parts(&attr.value)),
                );
            }
        }
    }
}

fn root_span<'a>(spans: &'a [&'a Span]) -> Option<&'a Span> {
    spans
        .iter()
        .copied()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().copied().min_by_key(|span| span.start_ns))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn non_negative_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_traceql::TagScope;

    use super::{LiveSource as _, LiveStore};
    use crate::{
        span::{AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode},
        wal::SpanRecord,
    };

    fn span_with_everything() -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /users".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: vec![KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("GET".into()),
            }],
            events: vec![EventRecord {
                time_unix_nano: 1_100,
                name: "exception".into(),
                attrs: vec![KeyValue {
                    key: "exception.type".into(),
                    value: AttrValue::Str("timeout".into()),
                }],
            }],
            links: vec![LinkRecord {
                trace_id: [9; 16],
                span_id: [8; 8],
                attrs: vec![KeyValue {
                    key: "link.kind".into(),
                    value: AttrValue::Str("retry".into()),
                }],
            }],
            instrumentation_scope: "otel-rust".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    fn store_with_one_span() -> LiveStore {
        let mut store = LiveStore::new(1_000_000);
        store.ingest(SpanRecord {
            tenant: "t".into(),
            span: span_with_everything(),
        });
        store
    }

    fn tags_for(store: &LiveStore, scope: Option<TagScope>) -> Vec<(TagScope, Vec<String>)> {
        futures::executor::block_on(store.tag_names("t", scope, 0, 10_000))
            .expect("tag names are readable")
            .into_iter()
            .map(|scoped| (scoped.scope, scoped.tags))
            .collect()
    }

    /// `tag_values` reads one tag's values across the live spans. Three of its
    /// filters survived the sweep, and all three are only visible when the
    /// whole result set is pinned rather than checked for membership: each
    /// mutant *adds* a value belonging to a different tag, so an assertion
    /// that merely finds the right value still passes.
    #[test]
    fn live_tag_values_return_only_the_asked_for_tag() {
        let mut span = span_with_everything();
        // A second attribute in each scope, so a mutant that inverts a key
        // filter selects the neighbour rather than nothing -- a wrong answer
        // rather than an empty one.
        span.resource_attrs.push(KeyValue {
            key: "service.version".into(),
            value: AttrValue::Str("2.0".into()),
        });
        span.span_attrs.push(KeyValue {
            key: "http.status_code".into(),
            value: AttrValue::Str("200".into()),
        });
        let mut store = LiveStore::new(1_000_000);
        store.ingest(SpanRecord {
            tenant: "t".into(),
            span,
        });

        let values = |tag: &str| {
            futures::executor::block_on(store.tag_values("t", tag, 0, 10_000))
                .expect("tag values are readable")
                .into_iter()
                .map(|value| (value.type_, value.value))
                .collect::<Vec<_>>()
        };
        let pair = |type_: &str, value: &str| (type_.to_string(), value.to_string());

        // Each attribute reads its own value, not its neighbour's.
        check!(values("service.name") == vec![pair("string", "api")]);
        check!(values("service.version") == vec![pair("string", "2.0")]);
        check!(values("http.method") == vec![pair("string", "GET")]);
        check!(values("http.status_code") == vec![pair("string", "200")]);

        // The instrumentation tags are guarded by a name test AND a non-empty
        // test. Pinning the whole result is what catches loosening that pair
        // to `||`: the scope would then be appended to every tag's values.
        check!(values("instrumentation:name") == vec![pair("string", "otel-rust")]);
        check!(values("instrumentation:version") == vec![pair("string", "1.2.3")]);

        // A `resource.` or `span.` prefix restricts which half is searched.
        // Without a scoped tag both guards are trivially true and a mutant
        // that removes either one is invisible.
        check!(values("resource.service.name") == vec![pair("string", "api")]);
        check!(values("span.http.method") == vec![pair("string", "GET")]);
        check!(
            values("span.service.name").is_empty(),
            "a resource attribute is not reachable under the span scope"
        );
        check!(
            values("resource.http.method").is_empty(),
            "nor a span attribute under the resource scope"
        );

        // TraceQL writes an unscoped attribute as `.name`, and the leading
        // dot is stripped before the lookup. Without this the strip can be
        // deleted outright and every other case still passes.
        check!(values(".service.name") == vec![pair("string", "api")]);
        check!(values(".http.method") == vec![pair("string", "GET")]);

        // An unknown tag has no values at all -- in particular it does not
        // pick up the instrumentation scope or version.
        check!(values("nonsense").is_empty());
    }

    /// `collect_span_intrinsic_value` reports a tag's value with the type a
    /// client reads it as. Each tag is checked against a neighbouring field as
    /// well as its own, since the fields are same-typed and a swap produces a
    /// well-formed pair.
    #[test]
    fn collecting_a_live_span_intrinsic_reports_value_and_type() {
        let span = span_with_everything();
        let collect = |tag: &str| {
            let mut values = std::collections::BTreeSet::new();
            super::collect_span_intrinsic_value(&span, tag, &mut values);
            values.into_iter().collect::<Vec<_>>()
        };
        let pair = |type_: &str, value: &str| (type_.to_string(), value.to_string());

        // A duration carries its own type name rather than "int".
        check!(collect("span:duration") == vec![pair("duration", "500")]);
        check!(
            collect("span:kind") == vec![pair("int", "2")],
            "server is kind 2"
        );
        check!(
            collect("span:status") == vec![pair("int", "1")],
            "ok is status 1"
        );
        check!(collect("span:name") == vec![pair("string", "GET /users")]);
        check!(collect("span:id") == vec![pair("string", "0202020202020202")]);

        // A root span contributes no parent id at all, rather than an empty
        // string or a zeroed one.
        check!(
            collect("span:parentID") == vec![],
            "this span has no parent"
        );

        // An unknown tag collects nothing and is not an error.
        check!(collect("span:nonsense") == vec![]);
        check!(collect("") == vec![]);
    }

    /// `tag_names` groups the tags it finds by scope, and each scope's entry
    /// appears only when that scope has something in it. Every scope is
    /// requested individually as well as all together, since a filter that
    /// ignored its argument would still return the right tags for the
    /// unfiltered case.
    #[test]
    fn tag_names_group_by_scope_and_omit_the_empty_ones() {
        let store = store_with_one_span();

        // Unfiltered: every scope the span populates, and nothing else.
        let all = tags_for(&store, None);
        let scopes: Vec<TagScope> = all.iter().map(|(scope, _)| *scope).collect();
        check!(
            scopes
                == vec![
                    TagScope::Resource,
                    TagScope::Span,
                    TagScope::Event,
                    TagScope::Link,
                    TagScope::Intrinsic,
                    TagScope::Instrumentation,
                ],
            "got {scopes:?}"
        );

        // Each scope alone returns only itself.
        for scope in [
            TagScope::Resource,
            TagScope::Span,
            TagScope::Event,
            TagScope::Link,
            TagScope::Instrumentation,
        ] {
            let one = tags_for(&store, Some(scope));
            check!(one.len() == 1, "{scope:?} returned {} groups", one.len());
            check!(one[0].0 == scope, "{scope:?} returned {:?}", one[0].0);
        }

        // The attribute keys reach the scope they were recorded under, and not
        // the neighbouring one.
        let resource = tags_for(&store, Some(TagScope::Resource));
        check!(resource[0].1 == vec!["service.name".to_string()]);
        let span_tags = tags_for(&store, Some(TagScope::Span));
        check!(span_tags[0].1 == vec!["http.method".to_string()]);

        // Event and link scopes carry their fixed intrinsics as well as the
        // attributes found on the records themselves.
        let event = tags_for(&store, Some(TagScope::Event));
        check!(event[0].1.contains(&"event:name".to_string()));
        check!(event[0].1.contains(&"event:timeSinceStart".to_string()));
        check!(event[0].1.contains(&"exception.type".to_string()));
        let link = tags_for(&store, Some(TagScope::Link));
        check!(link[0].1.contains(&"link:traceID".to_string()));
        check!(link[0].1.contains(&"link.kind".to_string()));

        // A tenant with nothing in it has no scopes at all, rather than a set
        // of empty ones.
        let empty = LiveStore::new(1_000_000);
        check!(
            futures::executor::block_on(empty.tag_names("t", None, 0, 10_000))
                .expect("readable")
                .is_empty()
        );

        // A window that excludes the span excludes its tags too.
        check!(
            futures::executor::block_on(store.tag_names("t", None, 5_000, 6_000))
                .expect("readable")
                .is_empty(),
            "outside the time range"
        );
    }
}
