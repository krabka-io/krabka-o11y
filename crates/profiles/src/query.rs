//! Querier role: Pyroscope `querier.v1` Connect API and legacy flamebearer endpoints.

use std::{collections::BTreeMap, fmt::Write as _, future::Future, net::SocketAddr, sync::Arc};

use arrow::{
    array::{Array, AsArray},
    datatypes::{Int64Type, UInt64Type},
};
use axum::{
    Extension, Json, Router,
    extract::{Query, RawQuery},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use connectrpc_axum::message::{Code, ConnectError, ConnectRequest, ConnectResponse};
use krabka_blockstore::{LABEL_PROFILE_TYPE, LabelMatcher, MatchOp};
use krabka_pprof::{
    COL_FINGERPRINT, COL_TIMESTAMP, EngineOpts, FlameEngine, FlameGraph, InMemoryProfileStore,
    LabeledHeatmap, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE,
    PCOL_VALUE, ProfileError, ProfileStats, ProfileStore, ProfileType, Series, SeriesAgg,
    bin_heatmap, parse_label_selector, step_bucket_ms, step_from_secs,
};
use krabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt},
    days, hours, millis, minutes, secs,
};
use num_traits::ToPrimitive as _;
use prost::Message;
use serde::{Deserialize, Deserializer};
use serde_json::json;
use tokio::net::TcpListener;

use crate::{
    ids::{DefaultMs, EndMs, MaxValue, MinValue, NowMs, StartMs},
    limits::{Limits, OverridesProvider},
    metrics::ServiceMetrics,
    query_frontend::{FrontendConfig, split_inclusive_range},
    wire::pb,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use base64::Engine;
    use krabka_pprof::{FunctionRec, LineRec, LocationRec};
    use krabka_units::secs;

    /// Converting a heatmap to its wire form derives a step from the time
    /// span and stamps each slot with its own *end*. The span is `end - start`
    /// and every existing fixture starts at zero, where subtracting and adding
    /// agree -- so the heatmap here starts at 1000ms, making the two differ.
    ///
    /// The whole series is compared rather than the step alone, because the
    /// step is not a field: it only shows through the slot timestamps.
    #[test]
    fn a_heatmap_stamps_each_slot_with_the_end_of_its_own_bucket() {
        let series = pb::querier::v1::HeatmapSeries::from(krabka_pprof::Heatmap {
            start_ms: 1_000,
            end_ms: 5_000,
            time_buckets: 4,
            value_buckets: 2,
            min_value: 0,
            max_value: 100,
            counts: vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]],
        });

        // A 4000ms span over 4 buckets is a 1000ms step, so the slots end at
        // 2000..5000. Adding instead of subtracting would give a 1500ms step
        // and run the last slot out to 7000, past the heatmap's own end.
        let slot = |timestamp, counts: Vec<i32>| pb::querier::v1::HeatmapSlot {
            timestamp,
            y_min: vec![0.0, 50.0],
            counts,
            exemplars: Vec::new(),
        };
        check!(
            series
                == pb::querier::v1::HeatmapSeries {
                    labels: Vec::new(),
                    slots: vec![
                        slot(2_000, vec![1, 2]),
                        slot(3_000, vec![3, 4]),
                        slot(4_000, vec![5, 6]),
                        slot(5_000, vec![7, 8]),
                    ],
                }
        );

        // No time buckets means no step to derive and no slots to stamp.
        let empty = pb::querier::v1::HeatmapSeries::from(krabka_pprof::Heatmap {
            start_ms: 1_000,
            end_ms: 5_000,
            time_buckets: 0,
            value_buckets: 2,
            min_value: 0,
            max_value: 100,
            counts: Vec::new(),
        });
        check!(empty.slots.is_empty());
    }

    /// `heatmap_slot_timestamp` places a sample in one of `time_buckets` slots
    /// and returns the slot's *end*. Everything here is boundary work: the
    /// guard is four clauses joined by `||`, so each has to reject on its own,
    /// and the arithmetic is a multiply-then-divide whose operators all look
    /// alike from the middle of a range.
    ///
    /// With start 0, end 100 and 4 buckets the step is 25, so a timestamp maps
    /// to 25, 50, 75 or 100 and nothing else.
    #[test]
    fn heatmap_slots_are_bounded_and_end_labelled() {
        let slot = |ts| super::heatmap_slot_timestamp(0, 100, 4, ts);

        // Each guard clause, alone: before the range, on the exclusive end,
        // past it, an inverted range, and no buckets.
        check!(slot(-1) == None, "before start");
        check!(slot(100) == None, "end is exclusive");
        check!(slot(101) == None, "past end");
        check!(
            super::heatmap_slot_timestamp(100, 0, 4, 50) == None,
            "inverted"
        );
        check!(
            super::heatmap_slot_timestamp(0, 100, 0, 50) == None,
            "no buckets"
        );

        // Inside: the first instant, each bucket edge, and the last instant.
        check!(slot(0) == Some(25), "start of the first bucket");
        check!(slot(24) == Some(25), "last ms of the first bucket");
        check!(slot(25) == Some(50), "first ms of the second");
        check!(slot(50) == Some(75), "first ms of the third");
        check!(slot(75) == Some(100), "first ms of the last");
        check!(slot(99) == Some(100), "last ms in range");

        // Offset from zero: with start_ms 0 a sign error in the span is
        // invisible, so repeat over 1000..1400 (step 100).
        let offset = |ts| super::heatmap_slot_timestamp(1000, 1400, 4, ts);
        check!(offset(999) == None, "before an offset start");
        check!(
            offset(1000) == Some(1100),
            "first instant of an offset range"
        );
        check!(offset(1150) == Some(1200), "mid offset range");
        check!(offset(1399) == Some(1400), "last ms of an offset range");
        check!(offset(1400) == None, "offset end is exclusive");
    }

    /// `query_param_i64` reads the first parameter matching `name`. The lookup
    /// compares keys for equality, so a flipped comparison would return some
    /// *other* parameter's value rather than nothing — the cases below use
    /// distinct values so that swap is visible.
    #[test]
    fn query_params_are_read_by_name_and_must_parse() {
        let params = [
            ("limit".to_string(), "10".to_string()),
            ("offset".to_string(), "20".to_string()),
            ("limit".to_string(), "30".to_string()),
            ("depth".to_string(), "-4".to_string()),
            ("junk".to_string(), "not a number".to_string()),
        ];
        let get = |name| super::query_param_i64(&params, name);

        check!(
            get("limit") == Some(10),
            "first match wins, not the later one"
        );
        check!(
            get("offset") == Some(20),
            "a distinct key gets its own value"
        );
        check!(get("depth") == Some(-4), "negatives parse");
        check!(get("junk") == None, "an unparseable value is not a match");
        check!(get("absent") == None, "a missing key has no value");
    }

    /// `label_matcher_value_escape` protects the four characters that would
    /// otherwise terminate or corrupt a quoted matcher value.
    #[test]
    fn label_matcher_values_escape_exactly_four_characters() {
        let escape = super::label_matcher_value_escape;

        check!(escape(r"a\b") == r"a\\b", "a backslash doubles");
        check!(escape(r#"a"b"#) == r#"a\"b"#, "a quote is escaped");
        check!(
            escape("a\nb") == r"a\nb",
            "a newline becomes an escape pair"
        );
        check!(escape("a\tb") == r"a\tb", "a tab becomes an escape pair");
        check!(escape("plain") == "plain", "ordinary text is untouched");
        check!(escape("") == "", "an empty value stays empty");
        check!(
            escape("\\\"\n\t") == r#"\\\"\n\t"#,
            "all four together, in order"
        );
    }

    /// `is_internal_label` gates the one reserved label name.
    #[test]
    fn only_the_profile_id_label_is_internal() {
        check!(super::is_internal_label(super::PROFILE_ID_LABEL));
        check!(!super::is_internal_label("service_name"));
        check!(
            !super::is_internal_label(""),
            "the empty name is not reserved"
        );
    }

    /// `dot_escape` guards a DOT string literal. It is a near-twin of
    /// `label_matcher_value_escape` but deliberately does *not* escape tabs,
    /// which DOT accepts literally inside quotes.
    #[test]
    fn dot_values_escape_three_characters_and_leave_tabs_alone() {
        let escape = super::dot_escape;

        check!(escape(r"a\b") == r"a\\b", "a backslash doubles");
        check!(escape("a\"b") == r#"a\"b"#, "a quote is escaped");
        check!(
            escape("a\nb") == r"a\nb",
            "a newline becomes an escape pair"
        );
        check!(escape("a\tb") == "a\tb", "a tab is left literal");
        check!(escape("plain") == "plain", "ordinary text is untouched");
    }

    /// `merge_profile_id_selector` folds a profile-id filter into a label
    /// selector. The id count picks the matcher form (exact vs alternation)
    /// and the selector's existing shape picks how the two are joined, so the
    /// cases below cross both.
    #[test]
    fn profile_ids_merge_into_every_selector_shape() {
        let merge = |sel: &str, ids: &[&str]| {
            let ids: Vec<String> = ids.iter().map(|s| (*s).to_string()).collect();
            super::merge_profile_id_selector(sel, &ids)
        };

        // No ids: the selector is handed back untouched, brackets and all.
        check!(merge(r#"{service="api"}"#, &[]).unwrap() == r#"{service="api"}"#);
        check!(
            merge("", &[]).unwrap() == "",
            "an empty selector stays empty"
        );

        // One id uses an exact match; more than one uses an anchored
        // alternation. The boundary between the two forms is at exactly 1.
        check!(merge("", &["abc"]).unwrap() == r#"{__profile_id__="abc"}"#);
        check!(merge("", &["abc", "def"]).unwrap() == r#"{__profile_id__=~"^(?:abc|def)$"}"#);

        // The four selector shapes an empty-vs-braced-vs-populated input takes.
        check!(merge("{}", &["abc"]).unwrap() == r#"{__profile_id__="abc"}"#);
        check!(
            merge("  ", &["abc"]).unwrap() == r#"{__profile_id__="abc"}"#,
            "blank trims to empty"
        );
        check!(
            merge(r#"{service="api"}"#, &["abc"]).unwrap()
                == r#"{service="api",__profile_id__="abc"}"#
        );

        // A selector that opens a brace but never closes it is rejected rather
        // than silently repaired.
        check!(merge(r#"{service="api""#, &["abc"]).is_err());
    }

    /// A render offset is a count and a one-letter unit. The units differ only
    /// in scale, so each is checked against the millisecond total it stands
    /// for rather than merely for being accepted.
    #[test]
    fn render_offsets_scale_by_their_unit() {
        let parse = |value| super::parse_render_offset(value).map(Time::millis_i64);

        check!(parse("1s").unwrap() == 1_000);
        check!(parse("1m").unwrap() == 60_000);
        check!(parse("1h").unwrap() == 3_600_000);
        check!(parse("1d").unwrap() == 86_400_000);
        check!(
            parse("90m").unwrap() == 5_400_000,
            "counts above one scale too"
        );
        check!(parse("0s").unwrap() == 0);
        check!(
            parse("-30m").unwrap() == -1_800_000,
            "an offset may look forward"
        );

        // The unit is the last character and the count is everything before it.
        let err = parse("1w").unwrap_err().to_string();
        check!(err.contains("duration unit \"w\""), "got: {err}");
        let err = parse("s").unwrap_err().to_string();
        check!(
            err.contains("invalid render relative duration"),
            "got: {err}"
        );
        let err = parse("").unwrap_err().to_string();
        check!(
            err.contains("invalid render relative duration"),
            "got: {err}"
        );
        let err = parse("1.5h").unwrap_err().to_string();
        check!(
            err.contains("invalid render relative duration"),
            "got: {err}"
        );

        // An offset too large to express in milliseconds is an error rather
        // than a silently different lookback.
        let err = parse("9223372036854775807d").unwrap_err().to_string();
        check!(err.contains("overflows"), "got: {err}");
    }

    /// `types_label_pairs` is a straight rename across a protobuf boundary.
    /// It has one way to go wrong, and it is worth ruling out.
    #[test]
    fn label_pairs_keep_their_names_with_their_values() {
        let pairs = super::types_label_pairs(vec![
            ("service".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        check!(
            pairs
                == vec![
                    pb::types::v1::LabelPair {
                        name: "service".to_string(),
                        value: "api".to_string()
                    },
                    pb::types::v1::LabelPair {
                        name: "env".to_string(),
                        value: "prod".to_string()
                    },
                ]
        );
        check!(super::types_label_pairs(vec![]).is_empty());
    }

    /// `flamegraph_dot` walks the flamegraph level by level, laying bars out
    /// left to right. Each bar's x position is the running sum of the bars
    /// before it plus its own offset, and a bar is wired to the bar on the
    /// level above whose span contains its left edge.
    ///
    /// The graph below is a root of width 10 over two children of width 4 and
    /// 6, plus a third level under the second child, so parent selection has
    /// to discriminate between two candidates rather than always pick the
    /// first. One name needs escaping and one index is out of range.
    #[test]
    fn flamegraph_dot_lays_out_bars_and_wires_them_to_their_parents() {
        let graph = krabka_pprof::FlameGraph {
            names: vec!["root".to_string(), "a\"quoted".to_string(), "b".to_string()],
            levels: vec![
                krabka_pprof::Level {
                    values: vec![0, 10, 2, 0],
                },
                krabka_pprof::Level {
                    values: vec![0, 4, 4, 1, 0, 6, 6, 2],
                },
                // Offset 4 from a running end of 0 puts this under "b", not "a".
                krabka_pprof::Level {
                    values: vec![4, 6, 6, 9],
                },
                // A negative name index cannot convert at all, which is a
                // different failure from index 9 above: that one converts and
                // then misses. Both fall back to a placeholder rather than
                // naming some unrelated frame.
                krabka_pprof::Level {
                    values: vec![0, 6, 6, -1],
                },
            ],
            total: 10,
            max_self: 6,
        };

        let expected = concat!(
            "digraph flamegraph {\n",
            "  node [shape=box];\n",
            "  n0 [label=\"root\\ntotal=10 self=2\"];\n",
            "  n1 [label=\"a\\\"quoted\\ntotal=4 self=4\"];\n",
            "  n0 -> n1;\n",
            "  n2 [label=\"b\\ntotal=6 self=6\"];\n",
            "  n0 -> n2;\n",
            "  n3 [label=\"unknown:9\\ntotal=6 self=6\"];\n",
            "  n2 -> n3;\n",
            "  n4 [label=\"unknown:18446744073709551615\\ntotal=6 self=6\"];\n",
            "}\n",
        );
        check!(super::flamegraph_dot(&graph) == expected);
    }

    use super::*;
    use crate::{Limits, OverridesProvider};

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    #[test]
    fn metadata_range_expands_omitted_request_without_validation() {
        let state = QuerierState::new_with_limits(
            Arc::new(InMemoryProfileStore::new()),
            Limits {
                max_query_length: secs(1),
                ..Limits::default()
            },
        );

        let range = MetadataRange::from_request(0, 0)
            .validate(&state, "tenant-a")
            .unwrap();

        assert!(range.start_ms == 0);
        assert!(range.end_ms == i64::MAX);
        assert!(range.omitted);
    }

    #[test]
    fn metadata_range_validates_explicit_request() {
        let state = QuerierState::new_with_limits(
            Arc::new(InMemoryProfileStore::new()),
            Limits {
                max_query_length: secs(1),
                ..Limits::default()
            },
        );

        let range = MetadataRange::from_request(0, 1_000)
            .validate(&state, "tenant-a")
            .unwrap();
        assert!(range.start_ms == 0);
        assert!(range.end_ms == 1_000);
        assert!(!range.omitted);

        let Err(err) = MetadataRange::from_request(0, 2_000).validate(&state, "tenant-a") else {
            panic!("explicit over-limit metadata range should be rejected");
        };
        assert!(err.to_string().contains("query length exceeded"), "{err}");
    }

    fn store_with_frame(name: &str) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string(name);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        store.push_sample(
            ("tenant-a", PT),
            vec![("service_name".to_string(), "api".to_string())],
            (0, stacktrace),
            7,
            10,
        );
        store
    }

    /// A store whose single series carries multiple labels in an order that is
    /// NOT sorted by name, with `service_name` before `__profile_type__`. This
    /// store exercises the sort-by-name path of the `Series` response.
    fn store_with_unsorted_labels() -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string("main.work");
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        store.push_sample(
            ("tenant-a", PT),
            vec![
                ("service_name".to_string(), "api".to_string()),
                ("__name__".to_string(), "process_cpu".to_string()),
                ("env".to_string(), "pprofdiff".to_string()),
                ("__profile_type__".to_string(), PT.to_string()),
            ],
            (0, stacktrace),
            7,
            10,
        );
        store
    }

    fn store_with_two_profile_types() -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string("main.work");
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        for profile_type in [PT, "memory:alloc_space:bytes:space:bytes"] {
            store.push_sample(
                ("tenant-a", profile_type),
                vec![
                    ("service_name".to_string(), "api".to_string()),
                    ("__profile_type__".to_string(), profile_type.to_string()),
                ],
                (0, stacktrace),
                7,
                10,
            );
        }
        store
    }

    fn store_with_span_frame(name: &str, span_id: u64) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string(name);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        store.push_sample_with_total_and_span(
            ("tenant-a", PT),
            vec![("service_name".to_string(), "api".to_string())],
            (0, stacktrace),
            (7, 7),
            10,
            span_id,
        );
        store
    }

    fn store_with_span_leaf_frames(frames: &[(&str, u64, i64)]) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        for (name, span_id, value) in frames {
            let name_ref = store.symbols_mut().intern_string(name);
            let function_id = store.symbols_mut().intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: 0,
                start_line: 0,
            });
            let location_id = store.symbols_mut().intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec {
                    function_id,
                    line: 1,
                }],
            });
            let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
            store.push_sample_with_total_and_span(
                ("tenant-a", PT),
                vec![("service_name".to_string(), "api".to_string())],
                (0, stacktrace),
                (*value, *value),
                10,
                *span_id,
            );
        }
        store
    }

    fn store_with_frame_samples(name: &str, samples: &[(i64, i64)]) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string(name);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        for (timestamp, value) in samples {
            store.push_sample(
                ("tenant-a", PT),
                vec![("service_name".to_string(), "api".to_string())],
                (0, stacktrace),
                *value,
                *timestamp,
            );
        }
        store
    }

    fn store_with_services(samples: &[(&str, &str, i64)]) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string("main.work");
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        for (service, env, value) in samples {
            store.push_sample(
                ("tenant-a", PT),
                vec![
                    ("service_name".to_string(), (*service).to_string()),
                    ("env".to_string(), (*env).to_string()),
                ],
                (0, stacktrace),
                *value,
                10,
            );
        }
        store
    }

    fn store_with_leaf_frames(frames: &[(&str, i64)]) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        for (name, value) in frames {
            let name_ref = store.symbols_mut().intern_string(name);
            let function_id = store.symbols_mut().intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: 0,
                start_line: 0,
            });
            let location_id = store.symbols_mut().intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec {
                    function_id,
                    line: 1,
                }],
            });
            let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
            store.push_sample(
                ("tenant-a", PT),
                vec![("service_name".to_string(), "api".to_string())],
                (0, stacktrace),
                *value,
                10,
            );
        }
        store
    }

    fn store_with_profile_ids() -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string("main.work");
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        for (profile_id, value) in [("profile-a", 5), ("profile-b", 7)] {
            store.push_sample(
                ("tenant-a", PT),
                vec![
                    ("service_name".to_string(), "api".to_string()),
                    ("__profile_id__".to_string(), profile_id.to_string()),
                ],
                (0, stacktrace),
                value,
                10,
            );
        }
        store
    }

    fn store_with_profile_ids_and_leaf_frames(
        frames: &[(&str, &str, i64)],
    ) -> InMemoryProfileStore {
        let mut store = InMemoryProfileStore::new();
        for (profile_id, name, value) in frames {
            let name_ref = store.symbols_mut().intern_string(name);
            let function_id = store.symbols_mut().intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: 0,
                start_line: 0,
            });
            let location_id = store.symbols_mut().intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec {
                    function_id,
                    line: 1,
                }],
            });
            let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
            store.push_sample(
                ("tenant-a", PT),
                vec![
                    ("service_name".to_string(), "api".to_string()),
                    ("__profile_id__".to_string(), (*profile_id).to_string()),
                ],
                (0, stacktrace),
                *value,
                10,
            );
        }
        store
    }

    fn json_i64(value: &serde_json::Value) -> Option<i64> {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    }

    #[tokio::test]
    async fn get_profile_stats_is_global_not_time_scoped() {
        // A sample ingested at a non-zero timestamp must be reported by
        // GetProfileStats even though Grafana's Profiles Drilldown sends an empty
        // request (start = end = 0). Time-scoping to [0, 0] hides it and wedges
        // the Drilldown onto its onboarding screen.
        let mut store = InMemoryProfileStore::new();
        let name_ref = store.symbols_mut().intern_string("main.work");
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        store.push_sample(
            ("tenant-a", PT),
            vec![("service_name".to_string(), "api".to_string())],
            (0, stacktrace),
            7,
            5_000, // non-zero ingest timestamp
        );
        let store = Arc::new(store);

        // Control: the old [0, 0]-scoped behavior misses the sample entirely.
        let scoped = store.stats("tenant-a", 0, 0).await.unwrap();
        assert!(!scoped.data_ingested);

        // The handler path queries globally and reports the sample.
        let state = QuerierState::new(Arc::clone(&store));
        let profile_stats = state.global_profile_stats("tenant-a").await.unwrap();
        assert!(
            profile_stats
                == ProfileStats {
                    data_ingested: true,
                    oldest_profile_time: Some(5_000),
                    newest_profile_time: Some(5_000),
                }
        );
    }

    #[tokio::test]
    async fn select_series_rejects_ranges_above_configured_limit() {
        let state = QuerierState::new_with_limits(
            Arc::new(store_with_frame("main.work")),
            Limits {
                max_query_length: secs(1),
                ..Limits::default()
            },
        );

        let err = state
            .select_series(
                ("tenant-a", PT, r#"{service_name="api"}"#),
                &[],
                secs(1),
                SeriesAgg::Sum,
                (0, 2_000),
                &[],
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("query length exceeded"), "{err}");
    }

    #[tokio::test]
    async fn select_series_uses_tenant_specific_query_overrides() {
        let state = QuerierState::new_with_overrides(
            Arc::new(store_with_frame("main.work")),
            OverridesProvider::from_yaml(
                r"
overrides:
  tenant-a:
    max_query_length_secs: 1
",
            )
            .unwrap(),
        );

        let tenant_a_err = state
            .select_series(
                ("tenant-a", PT, r#"{service_name="api"}"#),
                &[],
                secs(1),
                SeriesAgg::Sum,
                (0, 2_000),
                &[],
            )
            .await
            .unwrap_err();
        let tenant_b_series = state
            .select_series(
                ("tenant-b", PT, r#"{service_name="api"}"#),
                &[],
                secs(1),
                SeriesAgg::Sum,
                (0, 2_000),
                &[],
            )
            .await
            .unwrap();

        assert!(
            tenant_a_err.to_string().contains("query length exceeded"),
            "{tenant_a_err}"
        );
        assert!(tenant_b_series.is_empty());
    }

    #[tokio::test]
    async fn select_merge_stacktraces_clamps_requested_nodes_to_configured_max() {
        let state = QuerierState::new_with_limits(
            Arc::new(store_with_leaf_frames(&[
                ("hot.path", 10),
                ("warm.path", 8),
                ("cold.path", 6),
            ])),
            Limits {
                max_flamegraph_nodes_default: 2048,
                max_flamegraph_nodes_max: 2,
                ..Limits::default()
            },
        );

        let flamegraph = state
            .select_merge_stacktraces("tenant-a", PT, r#"{service_name="api"}"#, 0, 100, 10_000)
            .await
            .unwrap();

        for (name, want) in [("other", true), ("warm.path", false), ("cold.path", false)] {
            check!(flamegraph.names.iter().any(|frame| frame == name) == want);
        }
    }

    #[tokio::test]
    async fn render_format_dot_returns_dot_graph() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("from", "0")
            .append_pair("until", "100")
            .append_pair("format", "dot")
            .finish();
        let body = reqwest::Client::new()
            .get(format!("http://{bound}/pyroscope/render?{query}"))
            .header("x-scope-orgid", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(body.starts_with("digraph flamegraph"));
        assert!(body.contains("main.work"), "{body}");
    }

    #[tokio::test]
    async fn settings_service_get_returns_empty_and_set_echoes() {
        // Regression: the Grafana Profiles Drilldown app calls
        // `settings.v1.SettingsService/Get` during init. A 404 aborts init — the
        // app never issues the per-panel SelectSeries queries and the landing
        // grid renders empty. The querier must answer 200 with an (empty)
        // settings set; `Set` must echo the value back.
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("http://{bound}/settings.v1.SettingsService/Get"))
            .header("content-type", "application/json")
            .header("connect-protocol-version", "1")
            .header("x-scope-orgid", "tenant-a")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert!(
            resp.status() == reqwest::StatusCode::OK,
            "Get must succeed (Grafana init calls this), got {}",
            resp.status()
        );
        let json: serde_json::Value = resp.json().await.unwrap();
        // Connect JSON omits empty repeated fields, so `settings` is absent or [].
        let empty = json
            .get("settings")
            .and_then(|v| v.as_array())
            .is_none_or(std::vec::Vec::is_empty);
        assert!(empty, "expected empty settings, got {json}");

        let resp = client
            .post(format!("http://{bound}/settings.v1.SettingsService/Set"))
            .header("content-type", "application/json")
            .header("connect-protocol-version", "1")
            .header("x-scope-orgid", "tenant-a")
            .body(r#"{"setting":{"name":"flamegraph.collapsed","value":"true"}}"#)
            .send()
            .await
            .unwrap();
        assert!(
            resp.status() == reqwest::StatusCode::OK,
            "Set must succeed, got {}",
            resp.status()
        );
        let json: serde_json::Value = resp.json().await.unwrap();
        assert!(
            json.pointer("/setting/name").and_then(|v| v.as_str()) == Some("flamegraph.collapsed"),
            "Set must echo the setting, got {json}"
        );
    }

    #[tokio::test]
    async fn render_group_by_adds_group_frames_to_flamebearer() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_services(&[
            ("api", "prod", 5),
            ("worker", "prod", 7),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", &format!(r#"{PT}{{env="prod"}}"#))
            .append_pair("from", "0")
            .append_pair("until", "100")
            .append_pair("groupBy", "service_name")
            .finish();
        let body: serde_json::Value = reqwest::Client::new()
            .get(format!("http://{bound}/pyroscope/render?{query}"))
            .header("x-scope-orgid", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let names = body
            .pointer("/flamebearer/names")
            .and_then(serde_json::Value::as_array)
            .unwrap();

        for service in ["api", "worker"] {
            check!(
                names.iter().any(|name| name.as_str() == Some(service)),
                "{body}"
            );
        }
        check!(
            body.pointer("/flamebearer/numTicks")
                .and_then(serde_json::Value::as_i64)
                == Some(12),
            "{body}"
        );
    }

    #[tokio::test]
    async fn render_diff_flamebearer_includes_legacy_ticks_and_max_self() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("leftQuery", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("rightQuery", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("from", "0")
            .append_pair("until", "100")
            .finish();
        let body: serde_json::Value = reqwest::Client::new()
            .get(format!("http://{bound}/pyroscope/render-diff?{query}"))
            .header("x-scope-orgid", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        check!(
            body.pointer("/metadata/format")
                .and_then(serde_json::Value::as_str)
                == Some("double"),
            "{body}"
        );
        check!(
            body.pointer("/flamebearer/numTicks")
                .and_then(serde_json::Value::as_i64)
                == Some(14),
            "{body}"
        );
        check!(
            body.pointer("/flamebearer/maxSelf")
                .and_then(serde_json::Value::as_i64)
                == Some(7),
            "{body}"
        );
    }

    #[tokio::test]
    async fn render_diff_uses_side_specific_windows() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame_samples(
            "main.work",
            &[(1_700_000_010_000, 5), (1_700_000_090_000, 7)],
        ))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("leftQuery", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("leftFrom", "1700000000000")
            .append_pair("leftUntil", "1700000060000")
            .append_pair("rightQuery", &format!(r#"{PT}{{service_name="api"}}"#))
            .append_pair("rightFrom", "1700000060000")
            .append_pair("rightUntil", "1700000120000")
            .finish();
        let body: serde_json::Value = reqwest::Client::new()
            .get(format!("http://{bound}/pyroscope/render-diff?{query}"))
            .header("x-scope-orgid", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            body.pointer("/flamebearer/leftTicks")
                .and_then(serde_json::Value::as_i64)
                == Some(5),
            "{body}"
        );
        assert!(
            body.pointer("/flamebearer/rightTicks")
                .and_then(serde_json::Value::as_i64)
                == Some(7),
            "{body}"
        );
    }

    #[tokio::test]
    async fn select_merge_stacktraces_dot_format_returns_dot_only() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeStacktraces"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "format": "PROFILE_FORMAT_DOT",
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        check!(response.get("flamegraph").is_none(), "{response}");
        check!(
            response
                .get("dot")
                .and_then(serde_json::Value::as_str)
                .is_some_and(
                    |dot| dot.starts_with("digraph flamegraph") && dot.contains("main.work")
                ),
            "{response}"
        );
        check!(
            response
                .get("tree")
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty),
            "{response}"
        );
    }

    #[tokio::test]
    async fn select_merge_stacktraces_tree_format_returns_pyroscope_tree_bytes() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeStacktraces"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "format": "PROFILE_FORMAT_TREE",
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(response.get("flamegraph").is_none(), "{response}");
        assert!(
            response
                .get("dot")
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty),
            "{response}"
        );
        let tree = response
            .get("tree")
            .and_then(serde_json::Value::as_str)
            .and_then(|tree| base64::engine::general_purpose::STANDARD.decode(tree).ok())
            .unwrap();

        assert!(tree == b"\x00\x00\x01\x09main.work\x07\x00", "{response}");
    }

    #[tokio::test]
    async fn select_merge_span_profile_tree_format_returns_pyroscope_tree_bytes() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_span_frame(
            "main.work",
            111,
        ))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeSpanProfile"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "spanSelector": ["111"],
                "start": 0,
                "end": 100,
                "format": "PROFILE_FORMAT_TREE",
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(response.get("flamegraph").is_none(), "{response}");
        let tree = response
            .get("tree")
            .and_then(serde_json::Value::as_str)
            .and_then(|tree| base64::engine::general_purpose::STANDARD.decode(tree).ok())
            .unwrap();

        assert!(tree == b"\x00\x00\x01\x09main.work\x07\x00", "{response}");
    }

    #[tokio::test]
    async fn select_merge_stacktraces_profile_id_selector_filters_profiles() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_profile_ids())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeStacktraces"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "profileIdSelector": ["profile-a"],
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let total = response
            .get("flamegraph")
            .and_then(|flamegraph| flamegraph.get("total"))
            .and_then(json_i64);
        assert!(total == Some(5), "{response}");
    }

    #[tokio::test]
    async fn select_merge_profile_profile_id_selector_filters_profiles() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_profile_ids())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeProfile"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "profileIdSelector": ["profile-a"],
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response.get("profile").is_none(), "{response}");
        let total: i64 = response
            .get("sample")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .flat_map(|sample| {
                sample
                    .get("value")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter_map(json_i64)
            .sum();

        assert!(total == 5, "{response}");
    }

    #[tokio::test]
    async fn select_merge_profile_stack_trace_selector_filters_call_sites() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("hot.path", 7),
            ("cold.path", 10),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeProfile"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "stackTraceSelector": {
                    "callSite": [{ "name": "hot.path" }]
                },
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response.get("profile").is_none(), "{response}");
        let total: i64 = response
            .get("sample")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .flat_map(|sample| {
                sample
                    .get("value")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter_map(json_i64)
            .sum();

        assert!(total == 7, "{response}");
    }

    #[tokio::test]
    async fn select_merge_profile_max_nodes_truncates_to_other() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("leaf0", 1),
            ("leaf1", 1),
            ("leaf2", 1),
            ("leaf3", 1),
            ("leaf4", 1),
            ("leaf5", 1),
            ("leaf6", 1),
            ("leaf7", 1),
            ("leaf8", 1),
            ("leaf9", 1),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeProfile"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "maxNodes": 4,
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let samples = response
            .get("sample")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        let total: i64 = samples
            .iter()
            .flat_map(|sample| {
                sample
                    .get("value")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter_map(json_i64)
            .sum();
        let strings = response
            .get("stringTable")
            .and_then(serde_json::Value::as_array)
            .unwrap();

        check!(samples.len() <= 4, "{response}");
        check!(total == 10, "{response}");
        check!(
            strings.iter().any(|value| value.as_str() == Some("other")),
            "{response}"
        );
    }

    #[tokio::test]
    async fn select_merge_stacktraces_stack_trace_selector_filters_call_sites() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("hot.path", 7),
            ("cold.path", 10),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectMergeStacktraces"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "stackTraceSelector": r#"{"callSite":[{"name":"hot.path"}]}"#,
                "start": 0,
                "end": 100,
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let total = response
            .get("flamegraph")
            .and_then(|flamegraph| flamegraph.get("total"))
            .and_then(json_i64);
        assert!(total == Some(7), "{response}");
    }

    #[tokio::test]
    async fn diff_honors_embedded_stack_trace_selectors() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("hot.path", 7),
            ("cold.path", 10),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!("http://{bound}/querier.v1.QuerierService/Diff"))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "left": {
                    "profileTypeID": PT,
                    "labelSelector": r#"{service_name="api"}"#,
                    "stackTraceSelector": r#"{"callSite":[{"name":"hot.path"}]}"#,
                    "start": 0,
                    "end": 100
                },
                "right": {
                    "profileTypeID": PT,
                    "labelSelector": r#"{service_name="api"}"#,
                    "stackTraceSelector": r#"{"callSite":[{"name":"cold.path"}]}"#,
                    "start": 0,
                    "end": 100
                }
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            response.pointer("/flamegraph/leftTicks").and_then(json_i64) == Some(7),
            "{response}"
        );
        assert!(
            response
                .pointer("/flamegraph/rightTicks")
                .and_then(json_i64)
                == Some(10),
            "{response}"
        );
    }

    #[tokio::test]
    async fn profile_types_without_time_range_returns_ingested_types() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/ProfileTypes"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let profile_types = response
            .get("profileTypes")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert!(
            profile_types.iter().any(|profile_type| {
                profile_type
                    .get("ID")
                    .or_else(|| profile_type.get("id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(PT)
            }),
            "{response}"
        );
    }

    #[tokio::test]
    async fn profile_types_health_probe_ignores_query_range_limit_when_range_omitted() {
        let state = Arc::new(QuerierState::new_with_limits(
            Arc::new(store_with_frame("main.work")),
            Limits {
                max_query_length: secs(1),
                ..Limits::default()
            },
        ));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/ProfileTypes"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            response
                .get("profileTypes")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|profile_types| !profile_types.is_empty()),
            "{response}"
        );
    }

    #[tokio::test]
    async fn select_series_stack_trace_selector_filters_call_sites() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_leaf_frames(&[
            ("hot.path", 7),
            ("cold.path", 10),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "stackTraceSelector": {
                    "callSite": [{ "name": "hot.path" }]
                }
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let points = response
            .pointer("/series/0/points")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        assert!(points.len() == 1, "{response}");
        assert!(
            points[0].get("value").and_then(serde_json::Value::as_f64) == Some(7.0),
            "{response}"
        );
    }

    /// The `Series` RPC must emit each label set SORTED by name. That order
    /// matches real Pyroscope's `/series` wire order, for example
    /// `__profile_type__` before `service_name`. The Grafana Profiles Drilldown
    /// compares this order, so an insertion-order response is a wire-compat
    /// regression. This test drives the live handler over HTTP for both the
    /// projected form and the full-label-set form.
    #[tokio::test]
    async fn series_emits_label_sets_sorted_by_name() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_unsorted_labels())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();

        let series_labels = |body: serde_json::Value| {
            let url = format!("http://{bound}/querier.v1.QuerierService/Series");
            async move {
                let response: serde_json::Value = reqwest::Client::new()
                    .post(url)
                    .header("x-scope-orgid", "tenant-a")
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
                    .error_for_status()
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                response
                    .pointer("/labelsSet/0/labels")
                    .and_then(serde_json::Value::as_array)
                    .unwrap_or_else(|| panic!("missing labelsSet: {response}"))
                    .iter()
                    .map(|pair| {
                        pair.get("name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap()
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            }
        };

        // Projected onto the drilldown's exact label list, sent in NON-sorted
        // request order — the response must still be sorted by name.
        let projected = series_labels(json!({
            "matchers": [],
            "labelNames": ["service_name", "__profile_type__"],
        }))
        .await;
        assert!(
            projected == vec!["__profile_type__".to_string(), "service_name".to_string()],
            "{projected:?}"
        );

        // Full label set (`labelNames=[]`) — also sorted by name, not the order
        // the labels were ingested.
        let full = series_labels(json!({
            "matchers": [],
            "labelNames": [],
        }))
        .await;
        assert!(
            full == vec![
                "__name__".to_string(),
                "__profile_type__".to_string(),
                "env".to_string(),
                "service_name".to_string(),
            ],
            "{full:?}"
        );
    }

    #[tokio::test]
    async fn select_series_span_exemplar_returns_span_metadata() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_span_frame(
            "span.path",
            0x2a,
        ))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "exemplarType": "EXEMPLAR_TYPE_SPAN"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplar = response
            .pointer("/series/0/points/0/exemplars/0")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| panic!("missing span exemplar: {response}"));

        check!(exemplar.get("spanId").and_then(serde_json::Value::as_str) == Some("2a"));
        check!(exemplar.get("timestamp").and_then(json_i64) == Some(10));
        check!(exemplar.get("value").and_then(json_i64) == Some(7));
    }

    #[tokio::test]
    async fn select_series_span_exemplar_honors_stack_trace_selector() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_span_leaf_frames(&[
            ("hot.path", 0x2a, 5),
            ("cold.path", 0x2b, 7),
        ]))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "stackTraceSelector": {
                    "callSite": [{ "name": "hot.path" }]
                },
                "exemplarType": "EXEMPLAR_TYPE_SPAN"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplars = response
            .pointer("/series/0/points/0/exemplars")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("missing span exemplars: {response}"));
        let span_ids: Vec<_> = exemplars
            .iter()
            .filter_map(|exemplar| exemplar.get("spanId").and_then(serde_json::Value::as_str))
            .collect();

        assert!(span_ids == vec!["2a"], "{response}");
    }

    #[tokio::test]
    async fn select_series_individual_exemplar_returns_profile_ids() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_profile_ids())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplars = response
            .pointer("/series/0/points/0/exemplars")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("missing individual exemplars: {response}"));
        let profile_ids: Vec<_> = exemplars
            .iter()
            .filter_map(|exemplar| {
                exemplar
                    .get("profileId")
                    .and_then(serde_json::Value::as_str)
            })
            .collect();

        assert!(profile_ids.contains(&"profile-a"), "{response}");
        assert!(profile_ids.contains(&"profile-b"), "{response}");
    }

    #[tokio::test]
    async fn select_series_individual_exemplar_honors_stack_trace_selector() {
        let state = Arc::new(QuerierState::new(Arc::new(
            store_with_profile_ids_and_leaf_frames(&[
                ("profile-a", "hot.path", 5),
                ("profile-b", "cold.path", 7),
            ]),
        )));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectSeries"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "groupBy": ["service_name"],
                "step": 60.0,
                "stackTraceSelector": {
                    "callSite": [{ "name": "hot.path" }]
                },
                "exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplars = response
            .pointer("/series/0/points/0/exemplars")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("missing individual exemplars: {response}"));
        let profile_ids: Vec<_> = exemplars
            .iter()
            .filter_map(|exemplar| {
                exemplar
                    .get("profileId")
                    .and_then(serde_json::Value::as_str)
            })
            .collect();

        assert!(profile_ids == vec!["profile-a"], "{response}");
    }

    #[tokio::test]
    async fn select_heatmap_group_by_returns_labeled_series() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service_name".to_string(), "api".to_string())],
            (0, 1),
            (4, 4),
            0,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service_name".to_string(), "worker".to_string())],
            (0, 2),
            (9, 9),
            0,
        );
        let state = Arc::new(QuerierState::new(Arc::new(store)));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectHeatmap"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": "{}",
                "start": 0,
                "end": 100,
                "step": 100.0,
                "groupBy": ["service_name"],
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let series = response
            .get("series")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        check!(series.len() == 2, "{response}");
        for service in ["api", "worker"] {
            check!(
                series.iter().any(|item| {
                    item.pointer("/labels/0/name")
                        .and_then(serde_json::Value::as_str)
                        == Some("service_name")
                        && item
                            .pointer("/labels/0/value")
                            .and_then(serde_json::Value::as_str)
                            == Some(service)
                }),
                "{response}"
            );
        }
    }

    #[tokio::test]
    async fn select_heatmap_span_exemplar_returns_span_metadata() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_span_frame(
            "span.path",
            0x2a,
        ))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectHeatmap"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "step": 60.0,
                "groupBy": ["service_name"],
                "queryType": "HEATMAP_QUERY_TYPE_SPAN",
                "exemplarType": "EXEMPLAR_TYPE_SPAN"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplar = response
            .pointer("/series/0/slots/0/exemplars/0")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| panic!("missing heatmap span exemplar: {response}"));

        check!(exemplar.get("spanId").and_then(serde_json::Value::as_str) == Some("2a"));
        check!(exemplar.get("timestamp").and_then(json_i64) == Some(10));
        check!(exemplar.get("value").and_then(json_i64) == Some(7));
    }

    #[tokio::test]
    async fn select_heatmap_individual_exemplar_returns_profile_ids() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_profile_ids())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectHeatmap"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "step": 60.0,
                "groupBy": ["service_name"],
                "exemplarType": "EXEMPLAR_TYPE_INDIVIDUAL"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let exemplars = response
            .pointer("/series/0/slots/0/exemplars")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("missing heatmap individual exemplars: {response}"));
        let profile_ids: Vec<_> = exemplars
            .iter()
            .filter_map(|exemplar| {
                exemplar
                    .get("profileId")
                    .and_then(serde_json::Value::as_str)
            })
            .collect();

        assert!(profile_ids.contains(&"profile-a"), "{response}");
        assert!(profile_ids.contains(&"profile-b"), "{response}");
    }

    #[tokio::test]
    async fn select_heatmap_span_query_type_counts_only_span_profiles() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total_and_span(
            ("tenant-a", PT),
            vec![("service_name".to_string(), "api".to_string())],
            (0, 1),
            (7, 7),
            10,
            0x2a,
        );
        store.push_sample_with_total(
            ("tenant-a", PT),
            vec![("service_name".to_string(), "api".to_string())],
            (0, 2),
            (11, 11),
            20,
        );
        let state = Arc::new(QuerierState::new(Arc::new(store)));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/SelectHeatmap"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "profileTypeID": PT,
                "labelSelector": r#"{service_name="api"}"#,
                "start": 0,
                "end": 100,
                "step": 100.0,
                "groupBy": ["service_name"],
                "queryType": "HEATMAP_QUERY_TYPE_SPAN"
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        let count: i64 = response
            .pointer("/series/0/slots/0/counts")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .iter()
            .filter_map(json_i64)
            .sum();

        assert!(count == 1, "{response}");
    }

    #[tokio::test]
    async fn analyze_query_returns_scope_and_impact_for_matching_series() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_two_profile_types())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/AnalyzeQuery"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "start": 0,
                "end": 100,
                "query": format!(r#"{PT}{{service_name="api"}}"#),
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        check!(response.get("valid").is_none(), "{response}");
        check!(
            response
                .pointer("/queryImpact/totalQueriedSeries")
                .and_then(json_i64)
                == Some(1),
            "{response}"
        );
        check!(
            response
                .pointer("/queryScopes/0/componentType")
                .and_then(serde_json::Value::as_str)
                == Some("Long term storage"),
            "{response}"
        );
        check!(
            response
                .pointer("/queryScopes/0/seriesCount")
                .and_then(json_i64)
                == Some(1),
            "{response}"
        );
    }

    #[tokio::test]
    async fn analyze_query_counts_only_the_queried_profile_type() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_two_profile_types())));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/AnalyzeQuery"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({
                "start": 0,
                "end": 100,
                "query": format!(r#"{PT}{{service_name="api"}}"#),
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            response
                .pointer("/queryImpact/totalQueriedSeries")
                .and_then(json_i64)
                == Some(1),
            "{response}"
        );
    }

    #[test]
    fn render_query_splits_profile_type_and_selector() {
        let (profile_type, selector) = parse_render_query(
            r#"process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="api"}"#,
        )
        .unwrap();

        assert!(profile_type == "process_cpu:cpu:nanoseconds:cpu:nanoseconds");
        assert!(selector == r#"{service_name="api"}"#);
    }

    #[test]
    fn render_query_allows_profile_type_only() {
        let (profile_type, selector) =
            parse_render_query("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();

        assert!(profile_type == "process_cpu:cpu:nanoseconds:cpu:nanoseconds");
        assert!(selector == "{}");
    }

    #[test]
    fn flamebearer_json_includes_profile_metadata() {
        let response = flamebearer_json(
            krabka_pprof::FlameGraph {
                names: vec!["total".to_string()],
                levels: Vec::new(),
                total: 7,
                max_self: 7,
            },
            PT,
        );

        let metadata = response.get("metadata").unwrap();
        assert!(
            metadata
                == &json!({
                    "format": "single",
                    "spyName": "process_cpu",
                    "sampleRate": 100,
                    "units": "nanoseconds",
                    "name": "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                })
        );
    }

    #[test]
    fn flamegraph_dot_projects_levels_to_graphviz() {
        let dot = flamegraph_dot(&krabka_pprof::FlameGraph {
            names: vec![
                "total".to_string(),
                "main".to_string(),
                "main.work".to_string(),
            ],
            levels: vec![
                krabka_pprof::Level {
                    values: vec![0, 7, 0, 0],
                },
                krabka_pprof::Level {
                    values: vec![0, 7, 0, 1],
                },
                krabka_pprof::Level {
                    values: vec![0, 7, 7, 2],
                },
            ],
            total: 7,
            max_self: 7,
        });

        check!(dot.starts_with("digraph flamegraph"), "{dot}");
        for needle in ["main.work", "n0 -> n1", "n1 -> n2"] {
            check!(dot.contains(needle), "{dot}");
        }
    }

    #[test]
    fn limit_zero_means_unlimited() {
        assert!(limit(0) == usize::MAX);
        assert!(limit(2) == 2);
    }

    #[test]
    fn render_time_params_accept_now_offsets() {
        let now_ms = NowMs(1_700_000_000_000);

        for (input, want) in [
            (None, 0),
            (Some("now"), now_ms.0),
            (Some("now-1h"), now_ms.0 - 3_600_000),
            (Some("now-15m"), now_ms.0 - 15 * 60_000),
        ] {
            check!(parse_render_time_param(input, now_ms, DefaultMs(0)).unwrap() == want);
        }
    }

    #[test]
    fn render_time_params_accept_unix_seconds_and_millis() {
        let now_ms = NowMs(1_700_000_000_000);

        for (input, want) in [
            ("123", 123_000),
            ("1700000000", 1_700_000_000_000),
            ("1700000000000", 1_700_000_000_000),
        ] {
            check!(parse_render_time_param(Some(input), now_ms, DefaultMs(0)).unwrap() == want);
        }
    }

    #[test]
    fn render_time_params_reject_negative_resolved_bounds() {
        let now_ms = NowMs(1_000);

        // A `now-<offset>` larger than `now` underflows past the epoch, and a
        // literal negative timestamp (seconds or millis heuristic) is rejected.
        for input in ["now-1h", "-5", "-1700000000000"] {
            check!(parse_render_time_param(Some(input), now_ms, DefaultMs(0)).is_err());
        }
        // A valid millisecond timestamp at/above the seconds-vs-millis cutoff is
        // left untouched (not mangled by the heuristic) and accepted.
        check!(
            parse_render_time_param(Some("1700000000000"), now_ms, DefaultMs(0)).unwrap()
                == 1_700_000_000_000
        );
    }

    #[test]
    fn tenant_from_headers_validates_and_defaults() {
        // Absent header -> anonymous.
        let empty = HeaderMap::new();
        assert!(tenant_from_headers(&empty).unwrap() == "anonymous");

        // Valid tenant passes through.
        let mut valid = HeaderMap::new();
        valid.insert("x-scope-orgid", "tenant-a".parse().unwrap());
        assert!(tenant_from_headers(&valid).unwrap() == "tenant-a");

        // Empty header value falls back to anonymous (preserved behaviour).
        let mut blank = HeaderMap::new();
        blank.insert("x-scope-orgid", "".parse().unwrap());
        assert!(tenant_from_headers(&blank).unwrap() == "anonymous");
    }

    #[test]
    fn tenant_from_headers_rejects_path_unsafe_tenant() {
        let mut headers = HeaderMap::new();
        headers.insert("x-scope-orgid", "../escape".parse().unwrap());
        let err = tenant_from_headers(&headers).unwrap_err();

        // Mapped to an invalid-argument-class error with a generic message that
        // does not echo the attacker-supplied id.
        assert!(matches!(err, ProfileError::Plan(_)));
        assert!(connect_error(err).code() == Code::InvalidArgument);
    }

    #[tokio::test]
    async fn invalid_tenant_header_is_rejected_by_connect_handler() {
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let status = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/ProfileTypes"
            ))
            .header("x-scope-orgid", "bad/tenant")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status();

        assert!(status.is_client_error(), "{status}");
    }

    #[test]
    fn default_limits_reject_unbounded_explicit_range() {
        let state = QuerierState::new(Arc::new(InMemoryProfileStore::new()));

        // An explicit `start=0, end=i64::MAX` range (NOT the range-omitted health
        // probe) now exceeds the default `max_query_length` cap.
        let err = state
            .validate_query_range("anonymous", 0, i64::MAX)
            .unwrap_err();
        assert!(err.to_string().contains("query length exceeded"), "{err}");

        // A bounded recent window stays well within the 721h default.
        assert!(state.validate_query_range("anonymous", 0, 60_000).is_ok());
    }

    #[tokio::test]
    async fn profile_types_health_probe_ok_under_default_limits() {
        // The range-omitted (`start==0 && end==0`) health probe must still work
        // even though the default cap now rejects explicit unbounded ranges.
        let state = Arc::new(QuerierState::new(Arc::new(store_with_frame("main.work"))));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let response: serde_json::Value = reqwest::Client::new()
            .post(format!(
                "http://{bound}/querier.v1.QuerierService/ProfileTypes"
            ))
            .header("x-scope-orgid", "tenant-a")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(
            response
                .get("profileTypes")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|profile_types| !profile_types.is_empty()),
            "{response}"
        );
    }

    #[tokio::test]
    async fn profile_error_response_maps_internal_to_generic_500() {
        // Internal failures become a generic 500 with no leaked detail.
        let response = profile_error_response(ProfileError::Exec(
            "datafusion: secret plan detail".to_string(),
        ));
        assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        check!(body == "internal error", "{body}");
        check!(!body.contains("datafusion"), "{body}");
        check!(!body.contains("secret"), "{body}");
    }

    #[tokio::test]
    async fn profile_error_response_preserves_client_error_400() {
        // Client-shaped errors (including limit/range violations surfaced as
        // `Plan`) keep their user-facing message at 400.
        let response =
            profile_error_response(ProfileError::Plan("query length exceeded".to_string()));
        assert!(response.status() == StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("query length exceeded"), "{body}");
    }

    #[test]
    fn parse_span_selectors_accepts_decimal_and_hex() {
        let spans =
            parse_span_selectors(&["42".to_string(), "9a517183f26a089d".to_string()]).unwrap();

        assert!(spans == vec![42, 0x9a51_7183_f26a_089d]);
    }

    #[test]
    fn parse_span_selectors_rejects_bad_span() {
        assert!(parse_span_selectors(&["not-a-span".to_string()]).is_err());
    }

    #[test]
    fn heatmap_time_buckets_ceil_from_step_seconds() {
        check!(heatmap_time_buckets(StartMs(0), EndMs(21_000), secs(10), 4096).unwrap() == 3);
        check!(heatmap_time_buckets(StartMs(0), EndMs(1), <Time as TimeExt>::ZERO, 4096).is_err());
        check!(heatmap_time_buckets(StartMs(1), EndMs(1), secs(1), 4096).is_err());
    }

    #[test]
    fn heatmap_time_buckets_span_uses_nonzero_start() {
        // With a non-zero start, the bucket count depends on `end - start`, not
        // `end + start`. Here span = 20_000ms / 10_000ms/step = 2 buckets.
        // A `+` in the span computation would see 80_000ms → 8 buckets.
        check!(heatmap_time_buckets(StartMs(30_000), EndMs(50_000), secs(10), 4096).unwrap() == 2);
    }

    #[test]
    fn heatmap_time_buckets_rejects_sub_millisecond_steps() {
        for step_secs in [0.0001, 0.0005, 0.000_999_9] {
            let step = Time::from_secs_f64(step_secs);
            check!(
                heatmap_time_buckets(StartMs(0), EndMs(1), step, 4096).is_err(),
                "{step_secs}"
            );
        }
        check!(heatmap_time_buckets(StartMs(0), EndMs(1), millis(1), 4096).unwrap() == 1);
    }

    #[test]
    fn heatmap_time_buckets_caps_large_ranges() {
        assert!(heatmap_time_buckets(StartMs(0), EndMs(i64::MAX), secs(10), 7).unwrap() == 7);
    }

    #[test]
    fn heatmap_series_projects_slots() {
        let series = pb::querier::v1::HeatmapSeries::from(krabka_pprof::Heatmap {
            start_ms: 0,
            end_ms: 20,
            time_buckets: 2,
            value_buckets: 2,
            min_value: 10,
            max_value: 30,
            counts: vec![vec![1, 0], vec![0, 2]],
        });

        assert!(
            series
                == pb::querier::v1::HeatmapSeries {
                    labels: Vec::new(),
                    slots: vec![
                        pb::querier::v1::HeatmapSlot {
                            timestamp: 10,
                            y_min: vec![10.0, 20.0],
                            counts: vec![1, 0],
                            exemplars: Vec::new(),
                        },
                        pb::querier::v1::HeatmapSlot {
                            timestamp: 20,
                            y_min: vec![10.0, 20.0],
                            counts: vec![0, 2],
                            exemplars: Vec::new(),
                        },
                    ],
                }
        );
    }
}

// === split-modules: generated submodules ===
mod analyze_query_handler;
mod analyze_query_inner;
mod connect_error;
mod default_heatmap_time_buckets_max;
mod default_heatmap_value_buckets;
mod default_store;
mod deserialize_group_by;
mod diff_handler;
mod diff_inner;
mod dot_escape;
mod flame_graph;
mod flame_graph_diff;
mod flamebearer_diff_json;
mod flamebearer_json;
mod flamebearer_metadata;
mod flamegraph_dot;
mod frames_match_call_sites;
mod get_profile_stats_handler;
mod get_profile_stats_inner;
mod get_settings_handler;
mod heatmap_individual_exemplars_from_scan;
mod heatmap_series;
mod heatmap_slot_timestamp;
mod heatmap_span_exemplars_by_series;
mod heatmap_span_exemplars_from_scan;
mod heatmap_time_buckets;
mod heatmap_y_mins;
mod individual_exemplars_from_scan;
mod individual_exemplars_from_totals;
mod is_internal_label;
mod label_matcher_value_escape;
mod label_names_handler;
mod label_names_inner;
mod label_pairs;
mod label_values_handler;
mod label_values_inner;
mod limit;
mod merge_label_matcher;
mod merge_profile_id_selector;
mod merge_profile_type_selector;
mod metadata_range;
mod normalize_render_unix_time;
mod parse_matchers;
mod parse_render_offset;
mod parse_render_query;
mod parse_render_time_param;
mod parse_span_selectors;
mod profile_error_response;
mod profile_id_label;
mod profile_types_handler;
mod profile_types_inner;
mod querier_state;
mod query_execution;
mod query_param_i64;
mod query_param_render_time;
mod query_range;
mod query_target;
mod reject_negative_render_time;
mod render_diff_handler;
mod render_diff_inner;
mod render_handler;
mod render_inner;
mod render_query;
mod router;
mod select_heatmap_handler;
mod select_heatmap_inner;
mod select_merge_profile_handler;
mod select_merge_profile_inner;
mod select_merge_span_profile_handler;
mod select_merge_span_profile_inner;
mod select_merge_stacktraces_handler;
mod select_merge_stacktraces_inner;
mod select_series_handler;
mod select_series_inner;
mod series_handler;
mod series_inner;
mod series_key;
mod serve;
mod serve_supervised;
mod set_settings_handler;
mod span_exemplars_by_series;
mod span_exemplars_from_scan;
mod span_exemplars_from_totals;
mod span_heatmap_points_from_scan;
mod stack_trace_call_sites;
mod stack_trace_call_sites_from_json;
mod stack_trace_location_json;
mod stack_trace_selector_json;
mod tenant_from_headers;
mod timed_query;
mod timed_query_response;
mod types_label_pairs;
mod unix_now_ms;

use analyze_query_handler::analyze_query_handler;
use analyze_query_inner::analyze_query_inner;
use connect_error::connect_error;
use default_heatmap_time_buckets_max::DEFAULT_HEATMAP_TIME_BUCKETS_MAX;
use default_heatmap_value_buckets::DEFAULT_HEATMAP_VALUE_BUCKETS;
pub use default_store::DefaultStore;
use deserialize_group_by::deserialize_group_by;
use diff_handler::diff_handler;
use diff_inner::diff_inner;
use dot_escape::dot_escape;
use flamebearer_diff_json::flamebearer_diff_json;
use flamebearer_json::flamebearer_json;
use flamebearer_metadata::flamebearer_metadata;
use flamegraph_dot::flamegraph_dot;
use frames_match_call_sites::frames_match_call_sites;
use get_profile_stats_handler::get_profile_stats_handler;
use get_profile_stats_inner::get_profile_stats_inner;
use get_settings_handler::get_settings_handler;
use heatmap_individual_exemplars_from_scan::heatmap_individual_exemplars_from_scan;
use heatmap_slot_timestamp::heatmap_slot_timestamp;
use heatmap_span_exemplars_by_series::HeatmapSpanExemplarsBySeries;
use heatmap_span_exemplars_from_scan::heatmap_span_exemplars_from_scan;
use heatmap_time_buckets::heatmap_time_buckets;
use heatmap_y_mins::heatmap_y_mins;
use individual_exemplars_from_scan::individual_exemplars_from_scan;
use individual_exemplars_from_totals::individual_exemplars_from_totals;
use is_internal_label::is_internal_label;
use label_matcher_value_escape::label_matcher_value_escape;
use label_names_handler::label_names_handler;
use label_names_inner::label_names_inner;
use label_pairs::label_pairs;
use label_values_handler::label_values_handler;
use label_values_inner::label_values_inner;
use limit::limit;
use merge_label_matcher::merge_label_matcher;
use merge_profile_id_selector::merge_profile_id_selector;
use merge_profile_type_selector::merge_profile_type_selector;
use metadata_range::MetadataRange;
use normalize_render_unix_time::normalize_render_unix_time;
use parse_matchers::parse_matchers;
use parse_render_offset::parse_render_offset;
use parse_render_query::parse_render_query;
use parse_render_time_param::parse_render_time_param;
use parse_span_selectors::parse_span_selectors;
use profile_error_response::profile_error_response;
use profile_id_label::PROFILE_ID_LABEL;
use profile_types_handler::profile_types_handler;
use profile_types_inner::profile_types_inner;
pub use querier_state::QuerierState;
use query_execution::QueryExecution;
use query_param_i64::query_param_i64;
use query_param_render_time::query_param_render_time;
use query_range::QueryRange;
use query_target::QueryTarget;
use reject_negative_render_time::reject_negative_render_time;
use render_diff_handler::render_diff_handler;
use render_diff_inner::render_diff_inner;
use render_handler::render_handler;
use render_inner::render_inner;
use render_query::RenderQuery;
pub use router::router;
use select_heatmap_handler::select_heatmap_handler;
use select_heatmap_inner::select_heatmap_inner;
use select_merge_profile_handler::select_merge_profile_handler;
use select_merge_profile_inner::select_merge_profile_inner;
use select_merge_span_profile_handler::select_merge_span_profile_handler;
use select_merge_span_profile_inner::select_merge_span_profile_inner;
use select_merge_stacktraces_handler::select_merge_stacktraces_handler;
use select_merge_stacktraces_inner::select_merge_stacktraces_inner;
use select_series_handler::select_series_handler;
use select_series_inner::select_series_inner;
use series_handler::series_handler;
use series_inner::series_inner;
use series_key::SeriesKey;
pub use serve::serve;
pub use serve_supervised::serve_supervised;
use set_settings_handler::set_settings_handler;
use span_exemplars_by_series::SpanExemplarsBySeries;
use span_exemplars_from_scan::span_exemplars_from_scan;
use span_exemplars_from_totals::span_exemplars_from_totals;
use span_heatmap_points_from_scan::span_heatmap_points_from_scan;
use stack_trace_call_sites::stack_trace_call_sites;
use stack_trace_call_sites_from_json::stack_trace_call_sites_from_json;
use stack_trace_location_json::StackTraceLocationJson;
use stack_trace_selector_json::StackTraceSelectorJson;
use tenant_from_headers::tenant_from_headers;
use timed_query::timed_query;
use timed_query_response::timed_query_response;
use types_label_pairs::types_label_pairs;
use unix_now_ms::unix_now_ms;
