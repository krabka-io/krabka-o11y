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

mod attr_string;
mod bytes_to_hex;
mod collect_event_values;
mod collect_link_values;
mod collect_span_intrinsic_value;
mod collect_trace_intrinsic_values;
mod event_ref;
mod event_tags;
mod in_time_range;
mod ingest_wal_payloads;
mod intrinsic_tags;
mod link_ref;
mod link_tags;
mod live_store;
mod non_negative_u64;
mod order_spans;
mod root_span;
mod run;
mod scoped_attribute_tag;
mod span_ref;
mod trace_spans;
mod traceql_attr;
mod typed_value_parts;

use attr_string::attr_string;
use bytes_to_hex::bytes_to_hex;
use collect_event_values::collect_event_values;
use collect_link_values::collect_link_values;
use collect_span_intrinsic_value::collect_span_intrinsic_value;
use collect_trace_intrinsic_values::collect_trace_intrinsic_values;
use event_ref::event_ref;
use event_tags::EVENT_TAGS;
use in_time_range::in_time_range;
pub use ingest_wal_payloads::ingest_wal_payloads;
use intrinsic_tags::INTRINSIC_TAGS;
use link_ref::link_ref;
use link_tags::LINK_TAGS;
pub use live_store::LiveStore;
use non_negative_u64::non_negative_u64;
use order_spans::order_spans;
use root_span::root_span;
pub use run::run;
use scoped_attribute_tag::scoped_attribute_tag;
use span_ref::span_ref;
use trace_spans::trace_spans;
use traceql_attr::traceql_attr;
use typed_value_parts::typed_value_parts;
