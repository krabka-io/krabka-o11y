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
    metrics::ServiceMetrics,
    querier::live::{LiveSource, Result as LiveResult},
    span::{
        AttrValue, EventRecord, KeyValue, LinkRecord, Span, batch::span_rows_for_window, nested_set,
    },
    wal::SpanRecord,
};

fn traceql_attributes(attrs: &[KeyValue]) -> Vec<krabka_traceql::Attribute> {
    attrs
        .iter()
        .filter_map(|attr| {
            traceql_attr(attr).map(|value| krabka_traceql::Attribute::new(attr.key.clone(), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    mod span_batch_packing;

    use assert2::check;
    use krabka_traceql::{TagCatalog as _, TagScope};

    use super::{LiveSource as _, LiveStore};
    use crate::{
        span::{
            AttrValue, KeyValue, Span,
            test_span::{server_span_with_event_and_link, string_attr},
        },
        wal::SpanRecord,
    };

    fn span_with_everything() -> Span {
        server_span_with_event_and_link(
            "GET /users",
            500,
            vec![string_attr("http.method", "GET")],
            1_100,
            vec![string_attr("exception.type", "timeout")],
        )
    }

    fn store_with_one_span() -> LiveStore {
        let mut store = LiveStore::new(1_000_000);
        store.ingest(SpanRecord {
            tenant: "t".into(),
            span: span_with_everything(),
        });
        store
    }

    fn tags_for(store: &LiveStore, scope: Option<TagScope>) -> Vec<krabka_traceql::ScopedTag> {
        futures::executor::block_on(store.tag_names("t", scope, 0, 10_000))
            .expect("tag names are readable")
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
                .collect::<Vec<_>>()
        };
        let pair = |type_: &str, value: &str| krabka_traceql::TypedValue::new(type_, value);

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
        let pair = |type_: &str, value: &str| krabka_traceql::TypedValue::new(type_, value);

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
        let scopes: Vec<TagScope> = all.iter().map(|scoped| scoped.scope).collect();
        check!(
            scopes
                == vec![
                    TagScope::Resource,
                    TagScope::Span,
                    TagScope::Intrinsic,
                    TagScope::Event,
                    TagScope::Link,
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
            check!(
                one[0].scope == scope,
                "{scope:?} returned {:?}",
                one[0].scope
            );
        }

        // The attribute keys reach the scope they were recorded under, and not
        // the neighbouring one.
        let resource = tags_for(&store, Some(TagScope::Resource));
        check!(resource[0].tags == vec!["service.name".to_string()]);
        let span_tags = tags_for(&store, Some(TagScope::Span));
        check!(span_tags[0].tags == vec!["http.method".to_string()]);

        // Event and link scopes carry their fixed intrinsics as well as the
        // attributes found on the records themselves.
        let event = tags_for(&store, Some(TagScope::Event));
        check!(event[0].tags.contains(&"event:name".to_string()));
        check!(event[0].tags.contains(&"event:timeSinceStart".to_string()));
        check!(event[0].tags.contains(&"exception.type".to_string()));
        let link = tags_for(&store, Some(TagScope::Link));
        check!(link[0].tags.contains(&"link:traceID".to_string()));
        check!(link[0].tags.contains(&"link.kind".to_string()));

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
mod live_store;
mod run;
mod values;
use krabka_traceql::bytes_to_hex;
pub use live_store::LiveStore;
pub use run::run;
use values::{
    INTRINSIC_TAGS, collect_event_values, collect_link_values, collect_span_intrinsic_value,
    collect_trace_intrinsic_values, order_spans, span_ref,
};

use crate::span::typed_value_parts::typed_value_parts;

pub(crate) const EVENT_TAGS: &[&str] = &["event:name", "event:timeSinceStart"];

pub(crate) const LINK_TAGS: &[&str] = &["link:traceID", "link:spanID"];

pub(crate) fn attr_string(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs.iter().find_map(|attr| {
        (attr.key == key).then(|| match &attr.value {
            AttrValue::Str(value) => Some(value.clone()),
            _ => None,
        })?
    })
}

pub(crate) fn in_time_range(span: &Span, start_ns: UnixNano, end_ns: UnixNano) -> bool {
    start_ns.0 <= span.start_ns && span.start_ns <= end_ns.0
}

pub(crate) fn link_ref(link: &LinkRecord) -> krabka_traceql::LinkRef {
    krabka_traceql::LinkRef {
        trace_id: link.trace_id,
        span_id: link.span_id,
        attributes: traceql_attributes(&link.attrs),
    }
}

pub(crate) fn non_negative_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

pub(crate) fn root_span<'a>(spans: &'a [&'a Span]) -> Option<&'a Span> {
    spans
        .iter()
        .copied()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().copied().min_by_key(|span| span.start_ns))
}

pub(crate) fn traceql_attr(attr: &KeyValue) -> Option<krabka_traceql::AttrValue> {
    attr.value.traceql_value()
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

pub(crate) fn trace_spans(trace_id: &[u8; 16], spans: &[Span]) -> krabka_traceql::TraceSpans {
    let root = spans
        .iter()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().min_by_key(|span| span.start_ns));
    krabka_traceql::TraceSpans {
        trace_id: *trace_id,
        root_service_name: root
            .and_then(|span| attr_string(&span.resource_attrs, "service.name"))
            .unwrap_or_default(),
        root_trace_name: root.map_or_else(Default::default, |span| span.name.clone()),
        resource_attributes: root.map_or_else(Default::default, |span| {
            span.resource_attrs
                .iter()
                .filter_map(|attr| {
                    traceql_attr(attr)
                        .map(|value| krabka_traceql::Attribute::new(attr.key.clone(), value))
                })
                .collect()
        }),
        spans: spans
            .iter()
            .zip(nested_set::assign_nested_set(spans))
            .map(|entry| span_ref(entry.0, entry.1))
            .collect(),
    }
}
