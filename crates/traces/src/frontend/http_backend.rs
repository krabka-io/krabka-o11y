//! The real querier fan-out backend.
//!
//! It is a reqwest client over a configurable set of querier addresses. It
//! speaks the Tempo HTTP API at the per-job grain, one HTTP call per planned
//! shard.
//!
//! The shard restriction uses the querier's real `scan_options` contract in
//! `querier/http::scan_options_param`. A cold block with a row-group range is
//! `block=<object_key>&rowGroupStart=<n>&rowGroupEnd=<m>`. The live shard sends
//! no scan params, which gives the querier's hot/cold union scan. There is no
//! `shard=live` param.
//!
//! By-id has no block scoping. It targets one querier by index and unions
//! across the pool. `start` and `end` are epoch **seconds** on every endpoint.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;

use crate::frontend::{
    backend::{
        BackendError, MetricsJobRequest, MetricsPartial, QuerierBackend, SearchJobRequest,
        SearchPartial, TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial,
        TraceByIdJobRequest, TracePartial,
    },
    config::FrontendConfig,
    job::{JobShard, TraceIndexCatalog},
    metrics_merge::MetricsResponseJson,
    wire::{SearchResponseJson, TraceByIdResponseJson},
};

// --- Tag body parsing (the querier's v2 tag shapes) -------------------------

#[cfg(test)]
mod tests {

    /// `scope_param` is the inverse of `parse_scope`: it names a scope for a
    /// query string. The six names are asserted to be distinct, so a scope
    /// borrowed from a neighbouring arm cannot pass unnoticed.
    #[test]
    fn every_tag_scope_has_its_own_query_parameter_name() {
        use krabka_traceql::TagScope;
        let name = super::scope_param;

        check!(name(TagScope::Resource) == "resource");
        check!(name(TagScope::Span) == "span");
        check!(name(TagScope::Intrinsic) == "intrinsic");
        check!(name(TagScope::Event) == "event");
        check!(name(TagScope::Link) == "link");
        check!(name(TagScope::Instrumentation) == "instrumentation");

        let mut names = vec![
            name(TagScope::Resource),
            name(TagScope::Span),
            name(TagScope::Intrinsic),
            name(TagScope::Event),
            name(TagScope::Link),
            name(TagScope::Instrumentation),
        ];
        names.sort_unstable();
        names.dedup();
        check!(names.len() == 6, "the six names must all differ: {names:?}");
    }

    /// `parse_scope` defaults to the span scope rather than refusing, so
    /// "span" and an unknown name reach the same answer by different routes.
    /// Every named scope is checked so none of them can quietly fall through
    /// to that default instead of being recognised.
    #[test]
    fn a_scope_name_defaults_to_span_only_when_unrecognised() {
        use krabka_traceql::TagScope;
        let scope = super::parse_scope;

        check!(scope("resource") == TagScope::Resource);
        check!(scope("intrinsic") == TagScope::Intrinsic);
        check!(scope("event") == TagScope::Event);
        check!(scope("link") == TagScope::Link);
        check!(scope("instrumentation") == TagScope::Instrumentation);

        // Both routes to Span: named, and by falling through.
        check!(scope("span") == TagScope::Span, "named explicitly");
        check!(scope("") == TagScope::Span, "the default");
        check!(scope("unknown") == TagScope::Span);
        check!(
            scope("Resource") == TagScope::Span,
            "case-sensitive, so this defaults"
        );
    }
    use assert2::check;

    use super::*;

    #[test]
    fn ns_to_seconds_round_trips_whole_and_fractional() {
        for (ns, want) in [
            (0, "0"),
            (1_000_000_000, "1"),
            (1_400_000_000, "1.4"),
            (-500_000_000, "-0.5"),
        ] {
            check!(ns_to_seconds(ns) == want);
        }
    }

    #[test]
    fn tag_values_body_projects_typed_values() {
        let body = TagValuesBody {
            tag_values: vec![TypedValueJson {
                type_: "string".into(),
                value: "GET".into(),
            }],
            metrics: crate::frontend::wire::Metrics::default(),
        };
        let values = body.into_typed_values();
        assert2::assert!(values.len() == 1);
        assert2::assert!(values[0].value.as_str() == "GET");
    }
}

// === split-modules: generated submodules ===
mod build_url;
mod error_for_status;
mod http_querier;
mod ns_to_seconds;
mod parse_scope;
mod push_shard_params;
mod run_query_frontend;
mod scope_param;
mod scope_tags_json;
mod tag_values_body;
mod tags_body;
mod tenant_header;
mod typed_value_json;

use build_url::build_url;
use error_for_status::error_for_status;
pub use http_querier::HttpQuerier;
use ns_to_seconds::ns_to_seconds;
use parse_scope::parse_scope;
use push_shard_params::push_shard_params;
pub use run_query_frontend::run_query_frontend;
use scope_param::scope_param;
use scope_tags_json::ScopeTagsJson;
use tag_values_body::TagValuesBody;
use tags_body::TagsBody;
use tenant_header::TENANT_HEADER;
use typed_value_json::TypedValueJson;
