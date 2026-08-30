//! Merge per-job search, by-id and tag partials back into one Tempo response.
//!
//! The merge honors `limit`, the max number of traces, and `spss`, the max
//! number of spans per spanSet. It accumulates the job-accounting `metrics{}`
//! block.
//!
//! The search merge currency is the **typed serde edge model** in
//! [`crate::frontend::wire`], not raw `serde_json::Value`. Reunion keys on
//! `traceID`, so a trace split across blocks or across hot and cold reassembles.
//! Span-level dedup on `spanID` covers the late-span overlap case, and the
//! merge accumulates the `matched` count across blocks. All of this runs over
//! typed structs.

use std::collections::BTreeSet;

use krabka_traceql::{ScopedTag, TagScope, TypedValue};
use krabka_units::ByteSize;

// Re-export the metric-series merge helpers (separate module for clarity).
pub use crate::frontend::metrics_merge::{
    MetricSample, MetricSeries, limit_exemplars, merge_metric_series,
};
use crate::frontend::{
    backend::{SearchPartial, TagNamesPartial, TagValuesPartial, TracePartial},
    wire::{Metrics, SearchResponseJson, SpanSetJson, TraceByIdResponseJson, TraceJson},
};

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::{bytes, millis};

    use super::*;
    use crate::frontend::wire::{
        OtlpSpanJson, ResourceSpansJson, ScopeSpansJson, SpanJson, TraceEnvelopeJson,
    };

    fn span(id: &str, start: u64, dur: u64) -> SpanJson {
        SpanJson {
            span_id: id.to_string(),
            start_time_unix_nano: start.to_string(),
            duration_nanos: dur.to_string(),
            attributes: vec![],
        }
    }

    /// Zero means "no limit" for both knobs, and that is the single input
    /// separating `> 0` from `>= 0`: at zero the two disagree, and the
    /// loosened form truncates the results to nothing. A limit that does fire
    /// must fire *after* the sort, so it keeps the newest traces rather than
    /// whichever happened to arrive first.
    #[test]
    fn a_zero_search_limit_means_no_limit() {
        let ids = |traces: &[TraceJson]| {
            traces
                .iter()
                .map(|trace| trace.trace_id.clone())
                .collect::<Vec<_>>()
        };
        let mut traces = vec![
            trace("01", "a", 10, vec![span("01", 10, 5), span("01", 11, 5)]),
            trace("02", "b", 30, vec![span("02", 30, 5)]),
            trace("03", "c", 20, vec![span("03", 20, 5)]),
        ];

        // Zero on both knobs keeps every trace and every span, but still sorts.
        super::apply_search_limits(&mut traces, 0, 0);
        check!(ids(&traces) == vec!["02", "03", "01"], "newest first");
        check!(
            traces[2].span_sets[0].spans.len() == 2,
            "a zero span limit leaves the spans alone"
        );

        // A real limit keeps the newest, which only holds if it is applied
        // after the sort rather than before it.
        super::apply_search_limits(&mut traces, 2, 1);
        check!(ids(&traces) == vec!["02", "03"]);
        check!(
            traces
                .iter()
                .all(|trace| trace.span_sets.iter().all(|set| set.spans.len() == 1))
        );
    }

    /// `merge_trace` folds a trace into a list, combining it with any entry
    /// that shares its id. Each of the four rules picks a different winner, so
    /// the fixture makes each one decisive on its own: the incoming trace is
    /// earlier but shorter, and one of the two names is blank on each side.
    #[test]
    fn merging_a_trace_takes_the_earliest_start_and_longest_duration() {
        let mut merged = vec![TraceJson {
            trace_id: "abc".to_string(),
            root_service_name: "api".to_string(),
            root_trace_name: String::new(),
            start_time_unix_nano: "2000".to_string(),
            duration: millis(5),
            span_sets: Vec::new(),
        }];

        // An unrelated id is appended rather than merged.
        super::merge_trace(
            &mut merged,
            TraceJson {
                trace_id: "other".to_string(),
                root_service_name: "elsewhere".to_string(),
                root_trace_name: "POST /x".to_string(),
                start_time_unix_nano: "1000".to_string(),
                duration: millis(9),
                span_sets: Vec::new(),
            },
        );
        check!(merged.len() == 2, "a different trace is its own entry");
        check!(merged[1].trace_id == "other");
        check!(
            merged[0].start_time_unix_nano == "2000",
            "and leaves the first alone"
        );

        // The same id merges: earlier start wins, longer duration wins, and a
        // blank name is filled from the incoming trace while a set one is not.
        super::merge_trace(
            &mut merged,
            TraceJson {
                trace_id: "abc".to_string(),
                root_service_name: "ignored".to_string(),
                root_trace_name: "GET /orders".to_string(),
                start_time_unix_nano: "1500".to_string(),
                duration: millis(3),
                span_sets: Vec::new(),
            },
        );
        check!(merged.len() == 2, "merged, not appended");
        check!(
            merged[0].start_time_unix_nano == "1500",
            "the earlier start wins"
        );
        check!(
            merged[0].duration == millis(5),
            "the longer duration wins, not the newer"
        );
        check!(
            merged[0].root_service_name == "api",
            "a name already set is kept, not overwritten"
        );
        check!(
            merged[0].root_trace_name == "GET /orders",
            "a blank name is filled from the incoming trace"
        );

        // A later start does not move the mark back.
        super::merge_trace(
            &mut merged,
            TraceJson {
                trace_id: "abc".to_string(),
                root_service_name: String::new(),
                root_trace_name: String::new(),
                start_time_unix_nano: "9000".to_string(),
                duration: millis(1),
                span_sets: Vec::new(),
            },
        );
        check!(
            merged[0].start_time_unix_nano == "1500",
            "a later start is ignored"
        );
        check!(
            merged[0].duration == millis(5),
            "and a shorter duration too"
        );
        check!(
            merged[0].root_trace_name == "GET /orders",
            "a blank incoming name clears nothing"
        );
    }

    fn trace(tid: &str, svc: &str, start: u64, spans: Vec<SpanJson>) -> TraceJson {
        let matched = u32::try_from(spans.len()).unwrap();
        TraceJson {
            trace_id: tid.to_string(),
            root_service_name: svc.to_string(),
            root_trace_name: "GET /".to_string(),
            start_time_unix_nano: start.to_string(),
            duration: millis(1),
            span_sets: vec![SpanSetJson { spans, matched }],
        }
    }

    fn partial(traces: Vec<TraceJson>, bytes: u64) -> SearchPartial {
        SearchPartial {
            traces,
            metrics: Metrics {
                total_jobs: 1,
                completed_jobs: 1,
                inspected_bytes: bytes,
                inspected_traces: 1,
                ..Metrics::default()
            },
        }
    }

    #[test]
    fn same_trace_across_blocks_reunions_spans() {
        let p0 = partial(
            vec![trace("01", "checkout", 10, vec![span("01", 10, 5)])],
            100,
        );
        let p1 = partial(
            vec![trace("01", "checkout", 8, vec![span("02", 8, 9)])],
            200,
        );
        let resp = merge_search(vec![p0, p1], 20, 10);
        assert2::assert!(
            resp == SearchResponseJson {
                traces: vec![TraceJson {
                    trace_id: "01".to_string(),
                    root_service_name: "checkout".to_string(),
                    root_trace_name: "GET /".to_string(),
                    start_time_unix_nano: "8".to_string(),
                    duration: millis(1),
                    span_sets: vec![SpanSetJson {
                        spans: vec![span("01", 10, 5), span("02", 8, 9)],
                        matched: 2,
                    }],
                }],
                metrics: Metrics {
                    total_jobs: 2,
                    completed_jobs: 2,
                    total_blocks: 0,
                    inspected_traces: 2,
                    inspected_bytes: 300,
                    inspected_spans: 0,
                },
            }
        );
    }

    #[test]
    fn duplicate_span_across_blocks_is_deduped() {
        let p0 = partial(vec![trace("01", "s", 10, vec![span("07", 10, 5)])], 50);
        let p1 = partial(vec![trace("01", "s", 10, vec![span("07", 10, 5)])], 50);
        let resp = merge_search(vec![p0, p1], 20, 10);
        let total_spans: usize = resp.traces[0]
            .span_sets
            .iter()
            .map(|ss| ss.spans.len())
            .sum();
        assert2::assert!(total_spans == 1);
    }

    #[test]
    fn partial_overlap_does_not_double_count_matched() {
        // Shard 0: spans 01,02 (matched 2). Shard 1: spans 02(dup),03 (matched 2).
        // Merged distinct spans = 01,02,03; matched = 2 + (2 - 1 dup) = 3, not 4.
        let p0 = partial(
            vec![trace(
                "01",
                "s",
                10,
                vec![span("01", 10, 5), span("02", 11, 5)],
            )],
            50,
        );
        let p1 = partial(
            vec![trace(
                "01",
                "s",
                10,
                vec![span("02", 11, 5), span("03", 12, 5)],
            )],
            50,
        );
        let resp = merge_search(vec![p0, p1], 20, 10);
        let total_spans: usize = resp.traces[0]
            .span_sets
            .iter()
            .map(|ss| ss.spans.len())
            .sum();
        assert2::assert!(total_spans == 3);
        assert2::assert!(resp.traces[0].span_sets[0].matched == 3);
    }

    #[test]
    fn truncated_overlap_subset_still_folds_its_matched_count() {
        // Per-shard spss truncation: shard 0 returned only spans 01,02 but
        // matched 5; shard 1 returned only span 02 (a subset that happens to
        // overlap shard 0's returned spans) but matched 3. Shard 1 is NOT a pure
        // duplicate — its returned span is truncated, so its non-returned matches
        // are still new. Merged matched = 5 + (3 - 1 returned dup) = 7, not 5.
        let mut p0 = trace("01", "s", 10, vec![span("01", 10, 5), span("02", 11, 5)]);
        p0.span_sets[0].matched = 5;
        let mut p1 = trace("01", "s", 10, vec![span("02", 11, 5)]);
        p1.span_sets[0].matched = 3;
        let resp = merge_search(vec![partial(vec![p0], 50), partial(vec![p1], 50)], 20, 10);
        assert2::assert!(resp.traces.len() == 1);
        // Distinct returned spans are still 01,02 (02 deduped).
        let total_spans: usize = resp.traces[0]
            .span_sets
            .iter()
            .map(|ss| ss.spans.len())
            .sum();
        assert2::assert!(total_spans == 2);
        assert2::assert!(resp.traces[0].span_sets[0].matched == 7);
    }

    #[test]
    fn cross_block_matched_count_accumulates() {
        // Two shards each contribute a distinct span; the merged spanSet's
        // matched is the sum (legacy semantics).
        let p0 = partial(vec![trace("01", "s", 10, vec![span("01", 10, 5)])], 50);
        let p1 = partial(vec![trace("01", "s", 10, vec![span("02", 10, 5)])], 50);
        let resp = merge_search(vec![p0, p1], 20, 10);
        assert2::assert!(resp.traces[0].span_sets[0].matched == 2);
    }

    #[test]
    fn limit_caps_trace_count_newest_first() {
        let p = partial(
            vec![
                trace("01", "a", 100, vec![span("01", 100, 1)]),
                trace("02", "b", 300, vec![span("02", 300, 1)]),
                trace("03", "c", 200, vec![span("03", 200, 1)]),
            ],
            10,
        );
        let resp = merge_search(vec![p], 2, 10);
        assert2::assert!(
            resp.traces
                == vec![
                    trace("02", "b", 300, vec![span("02", 300, 1)]),
                    trace("03", "c", 200, vec![span("03", 200, 1)]),
                ]
        );
    }

    #[test]
    fn spss_caps_spans_but_matched_is_true_count() {
        let spans = vec![
            span("01", 1, 1),
            span("02", 2, 1),
            span("03", 3, 1),
            span("04", 4, 1),
        ];
        let p = partial(vec![trace("01", "a", 1, spans)], 10);
        let resp = merge_search(vec![p], 20, 2);
        assert2::assert!(resp.traces[0].span_sets[0].spans.len() == 2);
        assert2::assert!(resp.traces[0].span_sets[0].matched == 4);
    }

    fn otlp_span(id: &str) -> OtlpSpanJson {
        OtlpSpanJson {
            span_id: id.to_string(),
            rest: serde_json::Map::new(),
        }
    }

    fn by_id_body(span_ids: &[&str], status: &str) -> TraceByIdResponseJson {
        TraceByIdResponseJson {
            trace: TraceEnvelopeJson {
                resource_spans: vec![ResourceSpansJson {
                    resource: serde_json::Value::Null,
                    scope_spans: vec![ScopeSpansJson {
                        scope: serde_json::Value::Null,
                        spans: span_ids.iter().map(|id| otlp_span(id)).collect(),
                    }],
                }],
            },
            status: status.to_string(),
            message: String::new(),
        }
    }

    fn by_id_partial(body: TraceByIdResponseJson, bytes: u64) -> TracePartial {
        TracePartial {
            trace: body,
            metrics: Metrics {
                completed_jobs: 1,
                inspected_bytes: bytes,
                ..Metrics::default()
            },
        }
    }

    #[test]
    fn assemble_returns_none_when_no_querier_has_it() {
        let p0 = by_id_partial(TraceByIdResponseJson::default(), 5);
        let p1 = by_id_partial(TraceByIdResponseJson::default(), 5);
        assert2::assert!(
            assemble_trace(vec![p0, p1], bytes(1_000_000))
                == (
                    None,
                    Metrics {
                        total_jobs: 0,
                        completed_jobs: 2,
                        total_blocks: 0,
                        inspected_traces: 0,
                        inspected_bytes: 10,
                        inspected_spans: 0,
                    },
                    TraceStatus::Complete,
                )
        );
    }

    #[test]
    fn assemble_unions_spans_across_queriers_and_dedupes() {
        // querier A holds spans 1,2; querier B holds spans 2,3 (2 overlaps).
        let p0 = by_id_partial(by_id_body(&["01", "02"], "COMPLETE"), 100);
        let p1 = by_id_partial(by_id_body(&["02", "03"], "COMPLETE"), 100);
        let (trace, metrics, status) = assemble_trace(vec![p0, p1], bytes(1_000_000));
        let trace = trace.unwrap();
        check!(assembled_span_count(&trace) == 3);
        check!(
            metrics
                == Metrics {
                    total_jobs: 0,
                    completed_jobs: 2,
                    total_blocks: 0,
                    inspected_traces: 0,
                    inspected_bytes: 200,
                    inspected_spans: 0,
                }
        );
        check!(status == TraceStatus::Complete);
    }

    #[test]
    fn assemble_flags_partial_over_byte_budget() {
        let p0 = by_id_partial(by_id_body(&["01", "02", "03"], "COMPLETE"), 100);
        let (trace, _m, status) = assemble_trace(vec![p0], bytes(1));
        assert2::assert!(trace.is_some());
        assert2::assert!(matches!(status, TraceStatus::Partial));
    }

    #[test]
    fn assemble_propagates_querier_partial_status() {
        let p0 = by_id_partial(by_id_body(&["01"], "PARTIAL"), 100);
        let (_t, _m, status) = assemble_trace(vec![p0], bytes(1_000_000));
        assert2::assert!(matches!(status, TraceStatus::Partial));
    }

    fn tag_metrics(bytes: u64) -> Metrics {
        Metrics {
            total_jobs: 1,
            completed_jobs: 1,
            inspected_bytes: bytes,
            ..Metrics::default()
        }
    }

    #[test]
    fn tag_names_union_dedupes_per_scope() {
        let a = TagNamesPartial {
            tags: vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["http.method".to_string()],
            }],
            metrics: tag_metrics(10),
        };
        let b = TagNamesPartial {
            tags: vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["http.method".to_string(), "http.status_code".to_string()],
            }],
            metrics: tag_metrics(20),
        };
        assert2::assert!(
            merge_tag_names(vec![a, b])
                == (
                    vec![ScopedTag {
                        scope: TagScope::Span,
                        tags: vec!["http.method".to_string(), "http.status_code".to_string()],
                    }],
                    Metrics {
                        total_jobs: 2,
                        completed_jobs: 2,
                        total_blocks: 0,
                        inspected_traces: 0,
                        inspected_bytes: 30,
                        inspected_spans: 0,
                    },
                )
        );
    }

    #[test]
    fn tag_values_union_dedupes_pairs() {
        let a = TagValuesPartial {
            values: vec![TypedValue {
                type_: "string".to_string(),
                value: "GET".to_string(),
            }],
            metrics: tag_metrics(1),
        };
        let b = TagValuesPartial {
            values: vec![
                TypedValue {
                    type_: "string".to_string(),
                    value: "GET".to_string(),
                },
                TypedValue {
                    type_: "string".to_string(),
                    value: "POST".to_string(),
                },
            ],
            metrics: tag_metrics(1),
        };
        let (merged, _) = merge_tag_values(vec![a, b]);
        // sorted by (type, value).
        assert2::assert!(
            merged
                == vec![
                    TypedValue {
                        type_: "string".to_string(),
                        value: "GET".to_string(),
                    },
                    TypedValue {
                        type_: "string".to_string(),
                        value: "POST".to_string(),
                    },
                ]
        );
    }
}

// === split-modules: generated submodules ===
mod apply_search_limits;
mod assemble_trace;
mod assembled_span_count;
mod merge_scope_spans;
mod merge_search;
mod merge_span_sets;
mod merge_tag_names;
mod merge_tag_values;
mod merge_trace;
mod parse_nanos;
mod scope_key;
mod seed_seen;
mod trace_status;
mod union_trace_bodies;

use apply_search_limits::apply_search_limits;
pub use assemble_trace::assemble_trace;
pub use assembled_span_count::assembled_span_count;
use merge_scope_spans::merge_scope_spans;
pub use merge_search::merge_search;
use merge_span_sets::merge_span_sets;
pub use merge_tag_names::merge_tag_names;
pub use merge_tag_values::merge_tag_values;
use merge_trace::merge_trace;
use parse_nanos::parse_nanos;
use scope_key::scope_key;
use seed_seen::seed_seen;
pub use trace_status::TraceStatus;
use union_trace_bodies::union_trace_bodies;
