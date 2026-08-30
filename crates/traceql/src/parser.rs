//! Recursive-descent `TraceQL` parser.

use crate::{
    ast::{
        Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, QueryHints, Scope,
        SpansetExpr, StructuralOp, Value, WithBinding,
    },
    error::{Result, TraceqlError},
    lexer::{Token, lex},
};

macro_rules! eat {
    ($parser:expr, $expected:expr) => {{
        let pos = $parser.pos;
        let matched = $parser.eat($expected);
        if matched && $parser.pos <= pos {
            return Err(TraceqlError::Parse(format!(
                "parser made no progress at token {pos}"
            )));
        }
        matched
    }};
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::ast::*;

    #[test]
    fn bare_dot_is_both_scope() {
        let q = parse("{ .service = \"checkout\" }").unwrap();
        assert!(
            q.root
                == SpansetExpr::Selector(Box::new(FieldExpr::Comparison {
                    lhs: Field {
                        scope: Scope::Both,
                        key: "service".into(),
                    },
                    op: ComparisonOp::Eq,
                    rhs: Value::Str("checkout".into()),
                }))
        );
    }

    #[test]
    fn span_colon_intrinsic_duration() {
        let q = parse("{ span:duration > 100ms }").unwrap();
        assert!(
            q.root
                == SpansetExpr::Selector(Box::new(FieldExpr::Comparison {
                    lhs: Field {
                        scope: Scope::Intrinsic(Intrinsic::Duration),
                        key: "duration".into(),
                    },
                    op: ComparisonOp::Gt,
                    rhs: Value::Duration(100_000_000),
                }))
        );
    }

    #[test]
    fn span_nested_set_parent_intrinsic_resolves() {
        let q = parse("{ span:nestedSetParent > 0 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(lhs.scope == Scope::Intrinsic(Intrinsic::NestedSetParent));
    }

    #[test]
    fn scopeless_intrinsics_resolve_to_intrinsic_scope() {
        // Tempo treats these reserved names as intrinsics when scopeless. Krabka
        // previously parsed them as `Scope::Both` attributes, so they silently
        // matched nothing (`attr.duration`, `attr.nestedSetParent`, …) and broke
        // every Grafana Tempo/Traces-Drilldown query, which writes them bare.
        for (query, want) in [
            ("{ duration > 1s }", Intrinsic::Duration),
            ("{ name != \"\" }", Intrinsic::Name),
            ("{ kind = server }", Intrinsic::Kind),
            ("{ status = error }", Intrinsic::Status),
            ("{ statusMessage != \"\" }", Intrinsic::StatusMessage),
            ("{ childCount > 0 }", Intrinsic::ChildCount),
            ("{ nestedSetLeft > 0 }", Intrinsic::NestedSetLeft),
            ("{ nestedSetRight > 0 }", Intrinsic::NestedSetRight),
            ("{ nestedSetParent < 0 }", Intrinsic::NestedSetParent),
            ("{ rootName != \"\" }", Intrinsic::TraceRootName),
            ("{ rootServiceName != \"\" }", Intrinsic::TraceRootService),
            ("{ traceDuration > 1s }", Intrinsic::TraceDuration),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Selector(fe) = &q.root else {
                panic!("{query}: selector")
            };
            let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
                panic!("{query}: comparison")
            };
            assert!(
                lhs.scope == Scope::Intrinsic(want),
                "{query}: got {:?}",
                lhs.scope
            );
        }
    }

    #[test]
    fn scopeless_duration_parses_duration_literal() {
        // Resolving bare `duration` to the intrinsic also makes `is_duration_field`
        // true, so the RHS lexes as a duration rather than a bare string.
        let q = parse("{ duration > 100ms }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { lhs, rhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(lhs.scope == Scope::Intrinsic(Intrinsic::Duration));
        assert!(*rhs == Value::Duration(100_000_000));
    }

    #[test]
    fn non_intrinsic_bare_ident_stays_attribute() {
        // Only the exact reserved set is promoted; a near-miss like `durations`
        // (not `duration`) is still a bare attribute.
        for key in ["durations", "kindof", "foo"] {
            let q = parse(&format!("{{ {key} = 1 }}")).unwrap();
            let SpansetExpr::Selector(fe) = &q.root else {
                panic!("{key}")
            };
            let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
                panic!("{key}")
            };
            assert!(lhs.scope == Scope::Both, "{key}: {:?}", lhs.scope);
            assert!(lhs.key == key);
        }
    }

    #[test]
    fn leading_dot_keeps_intrinsic_name_as_attribute() {
        // `.duration` / `.name` are explicit attribute lookups, never the intrinsic.
        let q = parse("{ .duration = 5 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(lhs.scope == Scope::Both);
        assert!(lhs.key == "duration");
    }

    #[test]
    fn span_parent_alias_is_not_a_valid_intrinsic() {
        // `span:Parent` was a bogus alias for nestedSetParent inconsistent with
        // Tempo's naming and with the other nested-set intrinsics; it must not
        // resolve.
        let err = parse("{ span:Parent > 0 }");
        assert!(matches!(err, Err(TraceqlError::Parse(_))));
    }

    #[test]
    fn duration_literals_accept_compound_go_durations() {
        let q = parse("{ span:duration > 1m30s }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(*rhs == Value::Duration(90_000_000_000));
    }

    #[test]
    fn duration_literal_arithmetic_obeys_precedence() {
        let q = parse("{ span:duration > 100ms + 2 * 50ms }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(*rhs == Value::Duration(200_000_000));
    }

    #[test]
    fn numeric_literal_arithmetic_obeys_precedence() {
        let q = parse("{ .retries = 1 + 2 * 3 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(*rhs == Value::Int(7));
    }

    #[test]
    fn single_span_rule_intra_brace_is_and() {
        let q = parse("{ .a = 1 && .b = 2 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        assert!(matches!(fe.as_ref(), FieldExpr::And(_, _)));
    }

    #[test]
    fn grouped_field_boolean_parses_inside_selector() {
        let q = parse("{ !(.a = 1 || .b = 2) }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        assert!(
            matches!(fe.as_ref(), FieldExpr::Not(inner) if matches!(inner.as_ref(), FieldExpr::Or(_, _)))
        );
    }

    #[test]
    fn inter_brace_and_is_spanset_level() {
        let q = parse("{ .a = 1 } && { .b = 2 }").unwrap();
        assert!(matches!(q.root, SpansetExpr::And(_, _)));
    }

    #[test]
    fn structural_operators_parse() {
        for (query, expected) in [
            ("{ .a = 1 } >> { .b = 2 }", StructuralOp::Descendant),
            ("{ .a = 1 } << { .b = 2 }", StructuralOp::Ancestor),
            ("{ .a = 1 } > { .b = 2 }", StructuralOp::Child),
            ("{ .a = 1 } < { .b = 2 }", StructuralOp::Parent),
            ("{ .a = 1 } ~ { .b = 2 }", StructuralOp::Sibling),
            ("{ .a = 1 } !>> { .b = 2 }", StructuralOp::NegDescendant),
            ("{ .a = 1 } !<< { .b = 2 }", StructuralOp::NegAncestor),
            ("{ .a = 1 } !> { .b = 2 }", StructuralOp::NegChild),
            ("{ .a = 1 } !< { .b = 2 }", StructuralOp::NegParent),
            ("{ .a = 1 } &>> { .b = 2 }", StructuralOp::UnionDescendant),
            ("{ .a = 1 } &<< { .b = 2 }", StructuralOp::UnionAncestor),
            ("{ .a = 1 } &> { .b = 2 }", StructuralOp::UnionChild),
            ("{ .a = 1 } &< { .b = 2 }", StructuralOp::UnionParent),
            ("{ .a = 1 } &~ { .b = 2 }", StructuralOp::UnionSibling),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Structural { op, .. } = &q.root else {
                panic!("expected structural expression for {query}")
            };
            assert!(*op == expected);
        }
    }

    #[test]
    fn pipeline_count_with_filter() {
        let q = parse("{ .a = 1 } | count() > 2").unwrap();
        assert!(
            q.pipeline
                == vec![
                    Pipeline::Aggregate(Aggregate::Count),
                    Pipeline::Filter {
                        op: ComparisonOp::Gt,
                        value: 2.0,
                    },
                ]
        );
    }

    #[test]
    fn pipeline_scalar_filter_accepts_literal_arithmetic() {
        let q = parse("{ .a = 1 } | count() > 1 + 2 * 3").unwrap();
        assert!(
            q.pipeline
                == vec![
                    Pipeline::Aggregate(Aggregate::Count),
                    Pipeline::Filter {
                        op: ComparisonOp::Gt,
                        value: 7.0,
                    },
                ]
        );
    }

    #[test]
    fn pipeline_adjacent_by_parses_before_filter() {
        let q = parse("{ .a = 1 } | count() by(span.svc) > 2").unwrap();
        assert!(
            q.pipeline
                == vec![
                    Pipeline::Aggregate(Aggregate::Count),
                    Pipeline::By(vec![Field {
                        scope: Scope::Span,
                        key: "svc".into(),
                    }]),
                    Pipeline::Filter {
                        op: ComparisonOp::Gt,
                        value: 2.0,
                    },
                ]
        );
    }

    #[test]
    fn traceql_metrics_pipeline_functions_parse() {
        let q = parse("{ .a = 1 } | rate()").unwrap();
        assert!(q.pipeline == vec![Pipeline::Aggregate(Aggregate::Rate)]);

        let q = parse("{ .a = 1 } | count_over_time()").unwrap();
        assert!(q.pipeline == vec![Pipeline::Aggregate(Aggregate::CountOverTime)]);

        let q = parse("{ .a = 1 } | avg_over_time(span:duration)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [Pipeline::Aggregate(Aggregate::AvgOverTime(_))]
        ));

        let q =
            parse("{ .a = 1 } | quantile_over_time(span:duration, .5, 0.9) by(span.svc)").unwrap();
        let [
            Pipeline::Aggregate(Aggregate::QuantileOverTime { quantiles, .. }),
            Pipeline::By(by),
        ] = q.pipeline.as_slice()
        else {
            panic!("quantile pipeline")
        };
        assert!(*quantiles == vec![0.5, 0.9]);
        assert!(by[0].key == "svc");

        let q = parse("{ .a = 1 } | histogram_over_time(span:duration) by(span.svc)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [
                Pipeline::Aggregate(Aggregate::HistogramOverTime(_)),
                Pipeline::By(_)
            ]
        ));

        let q = parse("{ .a = 1 } | count_over_time() | by(span.svc) | topk(2)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::By(_),
                Pipeline::TopK(2)
            ]
        ));

        let q = parse("{ .a = 1 } | count_over_time() | by(span.svc) | bottomk(1)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::By(_),
                Pipeline::BottomK(1)
            ]
        ));

        let q = parse("{} | compare({ status = error }, 10)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [Pipeline::Compare { top_n: 10, .. }]
        ));
    }

    #[test]
    fn compare_parses_selection_top_n_and_window() {
        // Grafana's Traces Drilldown "Comparison" tab sends
        // `{outer} | compare({selection}, topN)`. The selection is a full
        // spanset reused via parse_spanset_or; topN defaults to 10.
        let q = parse("{} | compare({ status = error }, 5)").unwrap();
        let [
            Pipeline::Compare {
                selection,
                top_n,
                start,
                end,
            },
        ] = q.pipeline.as_slice()
        else {
            panic!("compare pipeline: {:?}", q.pipeline)
        };
        assert!(*top_n == 5);
        assert!(*start == None && *end == None);
        let SpansetExpr::Selector(fe) = selection.as_ref() else {
            panic!("selection selector")
        };
        assert!(matches!(
            fe.as_ref(),
            FieldExpr::Comparison {
                lhs: Field {
                    scope: Scope::Intrinsic(Intrinsic::Status),
                    ..
                },
                op: ComparisonOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn compare_defaults_top_n_to_ten_when_omitted() {
        let q = parse("{} | compare({ status = error })").unwrap();
        let [Pipeline::Compare { top_n, .. }] = q.pipeline.as_slice() else {
            panic!("compare pipeline")
        };
        assert!(*top_n == 10);
    }

    #[test]
    fn compare_accepts_zero_top_n() {
        let q = parse("{} | compare({ status = error }, 0)").unwrap();
        let [Pipeline::Compare { top_n, .. }] = q.pipeline.as_slice() else {
            panic!("compare pipeline")
        };
        assert!(*top_n == 0);
    }

    #[test]
    fn compare_parses_optional_start_end_window() {
        let q = parse("{} | compare({}, 5, 1000, 2000)").unwrap();
        assert!(
            q.pipeline
                == vec![Pipeline::Compare {
                    selection: Box::new(SpansetExpr::Selector(Box::new(FieldExpr::Const(true)))),
                    top_n: 5,
                    start: Some(1000),
                    end: Some(2000),
                }]
        );
    }

    #[test]
    fn compare_selection_supports_and_or() {
        let q = parse("{} | compare({ .a = 1 && .b = 2 }, 3)").unwrap();
        let [Pipeline::Compare { selection, .. }] = q.pipeline.as_slice() else {
            panic!("compare pipeline")
        };
        let SpansetExpr::Selector(fe) = selection.as_ref() else {
            panic!("selection selector")
        };
        assert!(matches!(fe.as_ref(), FieldExpr::And(_, _)));

        let q = parse("{ .svc = \"api\" } | compare({ status = error } || { .a = 1 })").unwrap();
        let [Pipeline::Compare { selection, .. }] = q.pipeline.as_slice() else {
            panic!("compare pipeline")
        };
        assert!(matches!(selection.as_ref(), SpansetExpr::Or(_, _)));
    }

    #[test]
    fn compare_rejects_negative_top_n() {
        let msg = parse_err("{} | compare({ status = error }, -2)");
        assert!(msg.contains("compare topN must be non-negative"));
    }

    #[test]
    fn most_recent_query_hint_parses() {
        let q = parse("{ .a = 1 } with (most_recent=true)").unwrap();
        assert!(q.hints.most_recent);
        assert!(parse("{ .a = 1 } with (unknown=true)").is_err());
    }

    #[test]
    fn exemplars_query_hint_parses() {
        let q = parse("{ .a = 1 } | count_over_time() with (exemplars=false)").unwrap();
        assert!(q.hints.exemplars == Some(false));
    }

    #[test]
    fn sample_query_hint_parses() {
        // Grafana's Traces Drilldown appends `with(sample=true)` to its metrics
        // queries; Krabka must accept it (it computes exact metrics regardless).
        let q = parse("{ nestedSetParent < 0 } | histogram_over_time(duration) with(sample=true)")
            .unwrap();
        assert!(q.hints.sample == Some(true));
    }

    #[test]
    fn pipeline_with_bindings_parse() {
        let q = parse("{ .a = 1 } | with(error = span:status = error)").unwrap();
        let [Pipeline::With(bindings)] = q.pipeline.as_slice() else {
            panic!("with pipeline")
        };
        assert!(bindings.len() == 1);
        check!(bindings[0].name == "error");
        assert!(matches!(
            bindings[0].expr,
            FieldExpr::Comparison {
                lhs: Field {
                    scope: Scope::Intrinsic(Intrinsic::Status),
                    ..
                },
                op: ComparisonOp::Eq,
                rhs: Value::Str(ref value),
            } if value == "error"
        ));
    }

    #[test]
    fn double_equals_is_rejected() {
        assert!(parse("{ .a == 1 }").is_err());
    }

    #[test]
    fn spaced_double_equals_reports_single_equals_error() {
        let msg = parse_err("{ .a = = 1 }");
        assert!(msg.contains("use single = for equality"));
    }

    #[test]
    fn value_fold_min_div_neg_one_errors_not_panics() {
        // (0 - 9223372036854775807 - 1) folds to i64::MIN; (0 - 1) folds to -1.
        // i64::MIN / -1 and i64::MIN % -1 overflow and must surface as a Parse
        // error rather than panicking the parser (DoS via crafted query).
        let div = parse("{ .x = (0 - 9223372036854775807 - 1) / (0 - 1) }");
        assert!(matches!(div, Err(TraceqlError::Parse(_))));

        let rem = parse("{ .x = (0 - 9223372036854775807 - 1) % (0 - 1) }");
        assert!(matches!(rem, Err(TraceqlError::Parse(_))));
    }

    #[test]
    fn value_fold_div_and_mod_still_work() {
        for (query, want) in [
            ("{ .x = 6 / 2 }", Value::Int(3)),
            ("{ .x = 7 % 3 }", Value::Int(1)),
        ] {
            assert!(selector_rhs(query) == want, "value mismatch for {query}");
        }
    }

    #[test]
    fn quantile_leading_zero_fraction_preserved() {
        for (query, expected) in [
            ("{ .a = 1 } | quantile_over_time(span:duration, .05)", 0.05),
            ("{ .a = 1 } | quantile_over_time(span:duration, .99)", 0.99),
            ("{ .a = 1 } | quantile_over_time(span:duration, .5)", 0.5),
            ("{ .a = 1 } | quantile_over_time(span:duration, .9)", 0.9),
        ] {
            let q = parse(query).unwrap();
            let [Pipeline::Aggregate(Aggregate::QuantileOverTime { quantiles, .. })] =
                q.pipeline.as_slice()
            else {
                panic!("quantile pipeline for {query}")
            };
            assert!(*quantiles == vec![expected]);
        }
    }

    fn selector_rhs(query: &str) -> Value {
        let q = parse(query).unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector for {query}")
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!("comparison for {query}")
        };
        rhs.clone()
    }

    fn parse_err(query: &str) -> String {
        match parse(query) {
            Err(TraceqlError::Parse(msg)) => msg,
            other => panic!("expected parse error for {query}, got {other:?}"),
        }
    }

    // ---- query hints ----

    #[test]
    fn query_hint_non_boolean_value_errors() {
        let msg = parse_err("{ .a = 1 } with (most_recent=5)");
        assert!(msg.contains("expected boolean query hint value"));
    }

    #[test]
    fn query_hint_multiple_entries_parse() {
        let q = parse("{ .a = 1 } | count_over_time() with (most_recent=true, exemplars=true)")
            .unwrap();
        assert!(q.hints.most_recent);
        assert!(q.hints.exemplars == Some(true));
    }

    #[test]
    fn query_hint_missing_equals_errors() {
        // `with (` followed by an identifier then a non-`=` token.
        let msg = parse_err("{ .a = 1 } with (most_recent true)");
        assert!(msg.contains("expected Eq"));
    }

    // ---- pipeline stages ----

    #[test]
    fn unsupported_pipeline_stage_errors() {
        let msg = parse_err("{ .a = 1 } | bogus()");
        assert!(msg.contains("unsupported pipeline stage"));
        assert!(msg.contains("bogus"));
    }

    #[test]
    fn sum_avg_max_min_aggregates_parse() {
        for (query, want) in [
            (
                "{ .a = 1 } | sum(.x)",
                Aggregate::Sum(Field {
                    scope: Scope::Both,
                    key: "x".into(),
                }),
            ),
            (
                "{ .a = 1 } | avg(.x)",
                Aggregate::Avg(Field {
                    scope: Scope::Both,
                    key: "x".into(),
                }),
            ),
            (
                "{ .a = 1 } | max(.x)",
                Aggregate::Max(Field {
                    scope: Scope::Both,
                    key: "x".into(),
                }),
            ),
            (
                "{ .a = 1 } | min(.x)",
                Aggregate::Min(Field {
                    scope: Scope::Both,
                    key: "x".into(),
                }),
            ),
        ] {
            let q = parse(query).unwrap();
            assert!(
                q.pipeline == vec![Pipeline::Aggregate(want)],
                "aggregate mismatch for {query}"
            );
        }
    }

    #[test]
    fn over_time_aggregates_parse() {
        for (query, want) in [
            (
                "{ .a = 1 } | sum_over_time(span:duration)",
                Aggregate::SumOverTime(Field {
                    scope: Scope::Intrinsic(Intrinsic::Duration),
                    key: "duration".into(),
                }),
            ),
            (
                "{ .a = 1 } | min_over_time(span:duration)",
                Aggregate::MinOverTime(Field {
                    scope: Scope::Intrinsic(Intrinsic::Duration),
                    key: "duration".into(),
                }),
            ),
            (
                "{ .a = 1 } | max_over_time(span:duration)",
                Aggregate::MaxOverTime(Field {
                    scope: Scope::Intrinsic(Intrinsic::Duration),
                    key: "duration".into(),
                }),
            ),
        ] {
            let q = parse(query).unwrap();
            assert!(
                q.pipeline == vec![Pipeline::Aggregate(want)],
                "over-time aggregate mismatch for {query}"
            );
        }
    }

    #[test]
    fn histogram_over_time_defaults_to_duration_when_empty() {
        let q = parse("{ .a = 1 } | histogram_over_time()").unwrap();
        let [Pipeline::Aggregate(Aggregate::HistogramOverTime(field))] = q.pipeline.as_slice()
        else {
            panic!("histogram pipeline")
        };
        assert!(field.scope == Scope::Intrinsic(Intrinsic::Duration));
        assert!(field.key == "duration");
    }

    #[test]
    fn select_and_coalesce_pipeline_stages_parse() {
        let q = parse("{ .a = 1 } | select(.x, .y)").unwrap();
        assert!(
            q.pipeline
                == vec![Pipeline::Select(vec![
                    Field {
                        scope: Scope::Both,
                        key: "x".into(),
                    },
                    Field {
                        scope: Scope::Both,
                        key: "y".into(),
                    },
                ])]
        );

        let q = parse("{ .a = 1 } | coalesce()").unwrap();
        assert!(q.pipeline == vec![Pipeline::Coalesce]);
    }

    #[test]
    fn with_pipeline_supports_multiple_bindings() {
        let q = parse("{ .a = 1 } | with(x = .foo, y = .bar)").unwrap();
        assert!(
            q.pipeline
                == vec![Pipeline::With(vec![
                    WithBinding {
                        name: "x".into(),
                        expr: FieldExpr::Field(Field {
                            scope: Scope::Both,
                            key: "foo".into(),
                        }),
                    },
                    WithBinding {
                        name: "y".into(),
                        expr: FieldExpr::Field(Field {
                            scope: Scope::Both,
                            key: "bar".into(),
                        }),
                    },
                ])]
        );
    }

    // ---- rank limits ----

    #[test]
    fn rank_limit_requires_integer() {
        let msg = parse_err("{ .a = 1 } | topk(.5)");
        assert!(msg.contains("expected integer rank limit"));
    }

    #[test]
    fn rank_limit_accepts_zero() {
        let q = parse("{ .a = 1 } | count_over_time() | topk(0)").unwrap();
        assert!(matches!(q.pipeline.as_slice(), [_, Pipeline::TopK(0)]));

        let q = parse("{ .a = 1 } | count_over_time() | bottomk(0)").unwrap();
        assert!(matches!(q.pipeline.as_slice(), [_, Pipeline::BottomK(0)]));
    }

    #[test]
    fn rank_limit_rejects_negative() {
        let msg = parse_err("{ .a = 1 } | bottomk(0 - 1)");
        // `0 - 1` is two int tokens; the rank parser only reads the first Int so
        // it sees `0` then a stray token. Use a directly negative literal path.
        // A bare negative integer is lexed as Minus Int, so topk reads Minus.
        assert!(!msg.is_empty());
        let msg = parse_err("{ .a = 1 } | topk(-2)");
        assert!(msg.contains("expected integer rank limit"));
    }

    // ---- quantile edge cases ----

    #[test]
    fn quantile_accepts_integer_zero_and_one() {
        let q = parse("{ .a = 1 } | quantile_over_time(span:duration, 0, 1)").unwrap();
        let [Pipeline::Aggregate(Aggregate::QuantileOverTime { quantiles, .. })] =
            q.pipeline.as_slice()
        else {
            panic!("quantile pipeline")
        };
        assert!(*quantiles == vec![0.0, 1.0]);
    }

    #[test]
    fn quantile_out_of_range_errors() {
        let msg = parse_err("{ .a = 1 } | quantile_over_time(span:duration, 2)");
        assert!(msg.contains("quantile out of range"));
    }

    #[test]
    fn quantile_non_numeric_token_errors() {
        let msg = parse_err("{ .a = 1 } | quantile_over_time(span:duration, foo)");
        assert!(msg.contains("expected quantile"));
    }

    #[test]
    fn quantile_leading_dot_non_digit_errors() {
        let msg = parse_err("{ .a = 1 } | quantile_over_time(span:duration, .foo)");
        assert!(msg.contains("expected quantile digits"));
    }

    // ---- spanset / structural parsing ----

    #[test]
    fn parenthesized_spanset_groups_or() {
        let q = parse("({ .a = 1 } || { .b = 2 }) && { .c = 3 }").unwrap();
        let SpansetExpr::And(lhs, _) = &q.root else {
            panic!("and at top")
        };
        assert!(matches!(lhs.as_ref(), SpansetExpr::Or(_, _)));
    }

    #[test]
    fn spanset_primary_requires_brace_or_paren() {
        let msg = parse_err(".a = 1");
        assert!(msg.contains("expected spanset"));
    }

    #[test]
    fn inter_brace_or_is_spanset_level() {
        let q = parse("{ .a = 1 } || { .b = 2 }").unwrap();
        assert!(matches!(q.root, SpansetExpr::Or(_, _)));
    }

    // ---- field parsing ----

    #[test]
    fn explicit_scope_dot_key_resolves() {
        for (query, scope) in [
            ("{ resource.region = \"us\" }", Scope::Resource),
            ("{ event.foo = 1 }", Scope::Event),
            ("{ link.foo = 1 }", Scope::Link),
            ("{ instrumentation.foo = 1 }", Scope::Instrumentation),
            ("{ parent.foo = 1 }", Scope::Parent),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Selector(fe) = &q.root else {
                panic!("selector for {query}")
            };
            let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
                panic!("comparison for {query}")
            };
            assert!(lhs.scope == scope, "scope mismatch for {query}");
        }
    }

    #[test]
    fn unknown_scope_prefix_errors() {
        let msg = parse_err("{ bogus.foo = 1 }");
        assert!(msg.contains("unknown scope"));
    }

    #[test]
    fn unknown_intrinsic_errors() {
        let msg = parse_err("{ span:bogus = 1 }");
        assert!(msg.contains("unknown intrinsic"));
    }

    #[test]
    fn bare_ident_is_both_scope() {
        let q = parse("{ foo = 1 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector")
        };
        let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
            panic!("comparison")
        };
        assert!(lhs.scope == Scope::Both);
        assert!(lhs.key == "foo");
    }

    #[test]
    fn bare_field_without_comparison_is_existence() {
        let q = parse("{ .foo }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector")
        };
        let FieldExpr::Field(field) = fe.as_ref() else {
            panic!("bare field")
        };
        assert!(field.key == "foo");
    }

    #[test]
    fn trace_and_event_intrinsics_resolve() {
        for (query, intrinsic) in [
            ("{ trace:rootName = \"x\" }", Intrinsic::TraceRootName),
            ("{ trace:rootService = \"x\" }", Intrinsic::TraceRootService),
            ("{ trace:id = \"x\" }", Intrinsic::TraceId),
            ("{ event:name = \"x\" }", Intrinsic::EventName),
            (
                "{ event:timeSinceStart > 5ms }",
                Intrinsic::EventTimeSinceStart,
            ),
            ("{ link:traceID = \"x\" }", Intrinsic::LinkTraceId),
            ("{ link:spanID = \"x\" }", Intrinsic::LinkSpanId),
            (
                "{ instrumentation:version = \"x\" }",
                Intrinsic::InstrumentationVersion,
            ),
            ("{ span:nestedSetLeft > 0 }", Intrinsic::NestedSetLeft),
            ("{ span:nestedSetRight > 0 }", Intrinsic::NestedSetRight),
            ("{ span:parentId = \"x\" }", Intrinsic::ParentId),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Selector(fe) = &q.root else {
                panic!("selector for {query}")
            };
            let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
                panic!("comparison for {query}")
            };
            assert!(
                lhs.scope == Scope::Intrinsic(intrinsic),
                "intrinsic mismatch for {query}"
            );
        }
    }

    // ---- value parsing ----

    #[test]
    fn primary_values_cover_all_literal_kinds() {
        for (query, want) in [
            ("{ .a = \"s\" }", Value::Str("s".into())),
            ("{ .a = 42 }", Value::Int(42)),
            ("{ .a = 1.5 }", Value::Float(1.5)),
            ("{ .a = true }", Value::Bool(true)),
            ("{ .a = nil }", Value::Nil),
            // bare identifier on a non-duration field folds to a string value.
            ("{ .a = ident }", Value::Str("ident".into())),
        ] {
            assert!(selector_rhs(query) == want, "value mismatch for {query}");
        }
    }

    #[test]
    fn parenthesized_value_groups_arithmetic() {
        assert!(selector_rhs("{ .a = (1 + 2) * 3 }") == Value::Int(9));
    }

    #[test]
    fn missing_value_errors() {
        let msg = parse_err("{ .a = }");
        assert!(msg.contains("expected value"));
    }

    #[test]
    fn unary_negation_folds() {
        assert!(selector_rhs("{ .a = -5 }") == Value::Int(-5));
        assert!(selector_rhs("{ .a = - -5 }") == Value::Int(5));
    }

    #[test]
    fn power_operator_folds() {
        assert!(selector_rhs("{ .a = 2 ^ 3 }") == Value::Int(8));
        // negative exponent falls back to float.
        assert!(selector_rhs("{ .a = 2 ^ (0 - 1) }") == Value::Float(0.5));
    }

    #[test]
    fn modulo_operator_folds() {
        assert!(selector_rhs("{ .a = 10 % 3 }") == Value::Int(1));
    }

    // ---- value arithmetic helpers: mixed int/float ----

    #[test]
    fn mixed_int_float_arithmetic_promotes_to_float() {
        for (query, want) in [
            ("{ .a = 1 + 2.0 }", 3.0),
            ("{ .a = 2.0 + 1 }", 3.0),
            ("{ .a = 5 - 1.5 }", 3.5),
            ("{ .a = 5.5 - 1 }", 4.5),
            ("{ .a = 2 * 1.5 }", 3.0),
            ("{ .a = 1.5 * 2 }", 3.0),
            ("{ .a = 3 / 1.5 }", 2.0),
            ("{ .a = 3.0 / 2 }", 1.5),
            ("{ .a = 1.0 + 2.0 }", 3.0),
            ("{ .a = 6.0 / 2.0 }", 3.0),
            ("{ .a = 1.0 - 0.5 }", 0.5),
            ("{ .a = 2.0 * 2.0 }", 4.0),
            ("{ .a = 2.0 * 3.0 }", 6.0),
            ("{ .a = 5 / 2 }", 2.5),
        ] {
            assert!(
                selector_rhs(query) == Value::Float(want),
                "value mismatch for {query}"
            );
        }
    }

    #[test]
    fn float_power_variants_fold() {
        for query in ["{ .a = 2.0 ^ 2.0 }", "{ .a = 2 ^ 2.0 }", "{ .a = 2.0 ^ 2 }"] {
            assert!(
                selector_rhs(query) == Value::Float(4.0),
                "value mismatch for {query}"
            );
        }
    }

    // ---- duration arithmetic ----

    #[test]
    fn duration_subtraction_and_modulo_fold() {
        assert!(selector_rhs("{ span:duration = 100ms - 40ms }") == Value::Duration(60_000_000));
        assert!(selector_rhs("{ span:duration = 100ms % 30ms }") == Value::Duration(10_000_000));
    }

    #[test]
    fn duration_scalar_division_folds() {
        assert!(selector_rhs("{ span:duration = 100ms / 4 }") == Value::Duration(25_000_000));
    }

    #[test]
    fn duration_negation_folds() {
        assert!(selector_rhs("{ span:duration = 0ms - 5ms }") == Value::Duration(-5_000_000));
    }

    #[test]
    fn float_negation_folds() {
        assert!(selector_rhs("{ .a = 0.0 - 2.5 }") == Value::Float(-2.5));
        assert!(selector_rhs("{ .a = -2.5 }") == Value::Float(-2.5));
    }

    // ---- arithmetic error / overflow paths ----

    #[test]
    fn division_by_zero_errors() {
        assert!(parse_err("{ .a = 1 / 0 }").contains("division by zero"));
        assert!(parse_err("{ .a = 1.0 / 0.0 }").contains("division by zero"));
    }

    #[test]
    fn modulo_by_zero_errors() {
        assert!(parse_err("{ .a = 1 % 0 }").contains("modulo by zero"));
    }

    #[test]
    fn integer_addition_overflow_errors() {
        let msg = parse_err("{ .a = 9223372036854775807 + 1 }");
        assert!(msg.contains("integer addition out of range"));
    }

    #[test]
    fn integer_multiplication_overflow_errors() {
        let msg = parse_err("{ .a = 9223372036854775807 * 2 }");
        assert!(msg.contains("integer multiplication out of range"));
    }

    #[test]
    fn integer_exponentiation_overflow_errors() {
        let msg = parse_err("{ .a = 9223372036854775807 ^ 2 }");
        assert!(msg.contains("integer exponentiation out of range"));
    }

    #[test]
    fn type_mismatched_arithmetic_errors() {
        // adding a string to an int is unsupported.
        let msg = parse_err("{ .a = 1 + \"x\" }");
        assert!(msg.contains("is not supported"));
    }

    #[test]
    fn unary_negation_of_string_errors() {
        let msg = parse_err("{ .a = -\"x\" }");
        assert!(msg.contains("unary - is not supported"));
    }

    #[test]
    fn duration_plus_int_is_type_error() {
        // duration + bare int is not a supported combination.
        let msg = parse_err("{ span:duration = 100ms + 5 }");
        assert!(msg.contains("is not supported"));
    }

    // ---- numeric pipeline filter value validation ----

    #[test]
    fn pipeline_filter_rejects_non_numeric_value() {
        let msg = parse_err("{ .a = 1 } | count() > \"x\"");
        assert!(msg.contains("expected numeric filter value"));
    }

    #[test]
    fn pipeline_filter_accepts_float_value() {
        let q = parse("{ .a = 1 } | count() > 1.5").unwrap();
        assert!(
            q.pipeline[1]
                == Pipeline::Filter {
                    op: ComparisonOp::Gt,
                    value: 1.5,
                }
        );
    }

    // ---- duration literal parsing edge cases ----

    #[test]
    fn duration_units_all_resolve() {
        for (query, want_ns) in [
            ("{ span:duration = 5ns }", 5),
            ("{ span:duration = 5us }", 5_000),
            ("{ span:duration = 5ms }", 5_000_000),
            ("{ span:duration = 5s }", 5_000_000_000),
            ("{ span:duration = 2m }", 120_000_000_000),
            ("{ span:duration = 1h }", 3_600_000_000_000),
        ] {
            assert!(
                selector_rhs(query) == Value::Duration(want_ns),
                "duration mismatch for {query}"
            );
        }
    }

    #[test]
    fn duration_fractional_component_folds() {
        assert!(selector_rhs("{ span:duration = 1.5s }") == Value::Duration(1_500_000_000));
    }

    #[test]
    fn duration_unknown_unit_errors() {
        let msg = parse_err("{ span:duration = 5zz }");
        assert!(msg.contains("unknown duration unit"));
    }

    #[test]
    fn non_numeric_duration_ident_errors() {
        // A bare identifier against a duration field is routed to the duration
        // parser, which fails because it has no leading number.
        let msg = parse_err("{ span:duration = abc }");
        assert!(msg.contains("duration number") || msg.contains("duration"));
    }

    #[test]
    fn duration_number_without_unit_errors() {
        // `5x` lexes as one duration ident: number `5` then unit `x`, which is
        // an unknown unit. `12` followed by a non-unit char trips the missing
        // unit path; emulate via a digits-only ident such as `5k`.
        let msg = parse_err("{ span:duration = 5k }");
        assert!(msg.contains("unknown duration unit"));
    }

    #[test]
    fn bare_int_against_duration_field_folds_as_int() {
        // A plain integer literal against a duration field is lexed as an Int
        // token (not a duration ident), so it folds to an Int value.
        assert!(selector_rhs("{ span:duration = 5 }") == Value::Int(5));
    }

    #[test]
    fn all_comparison_operators_parse() {
        for (query, expected) in [
            ("{ .a < 5 }", ComparisonOp::Lt),
            ("{ .a <= 5 }", ComparisonOp::Lte),
            ("{ .a > 5 }", ComparisonOp::Gt),
            ("{ .a >= 5 }", ComparisonOp::Gte),
            ("{ .a != 5 }", ComparisonOp::Neq),
            ("{ .a =~ \"x\" }", ComparisonOp::Re),
            ("{ .a !~ \"x\" }", ComparisonOp::Nre),
            ("{ .a = 5 }", ComparisonOp::Eq),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Selector(fe) = &q.root else {
                panic!("selector for {query}")
            };
            let FieldExpr::Comparison { op, .. } = fe.as_ref() else {
                panic!("comparison for {query}")
            };
            assert!(*op == expected, "op mismatch for {query}");
        }
    }

    #[test]
    fn leading_dot_duration_fraction_folds() {
        // `.5s` lexes as one duration ident with an empty whole part and a
        // fractional part, exercising the fraction-scaling branch.
        assert!(selector_rhs("{ span:duration = .5s }") == Value::Duration(500_000_000));
    }

    #[test]
    fn duration_overflow_errors() {
        // A duration literal far beyond i64 nanoseconds must surface as a parse
        // error rather than overflowing (the i64::try_from at the end fails).
        let msg = parse_err("{ span:duration = 100000000000h }");
        assert!(msg.contains("range"));
    }

    #[test]
    fn duration_i128_multiply_overflow_errors() {
        // A whole-number part that parses as i128 but overflows when scaled by
        // the hour multiplier must surface "duration out of range". 30 digits
        // (~1e29) is well within i128, but ×3.6e12 exceeds i128::MAX.
        let big = "1".to_string() + &"0".repeat(29);
        let msg = parse_err(&format!("{{ span:duration = {big}h }}"));
        assert!(msg.contains("duration out of range"));
    }
}

// === split-modules: generated submodules ===
mod arithmetic_type_error;
mod default_compare_top_n;
mod i64_to_f64;
mod intrinsic;
mod is_duration_field;
mod numeric_filter_field;
mod numeric_filter_value;
mod parse;
mod parse_duration_component_nanos;
mod parse_duration_nanos;
mod parser;
mod scope;
mod scopeless_intrinsic;
mod value_add;
mod value_div;
mod value_mod;
mod value_mul;
mod value_neg;
mod value_pow;
mod value_sub;

use arithmetic_type_error::arithmetic_type_error;
use default_compare_top_n::DEFAULT_COMPARE_TOP_N;
use i64_to_f64::i64_to_f64;
use intrinsic::intrinsic;
use is_duration_field::is_duration_field;
use numeric_filter_field::numeric_filter_field;
use numeric_filter_value::numeric_filter_value;
pub use parse::parse;
use parse_duration_component_nanos::parse_duration_component_nanos;
use parse_duration_nanos::parse_duration_nanos;
use parser::Parser;
use scope::scope;
use scopeless_intrinsic::scopeless_intrinsic;
use value_add::value_add;
use value_div::value_div;
use value_mod::value_mod;
use value_mul::value_mul;
use value_neg::value_neg;
use value_pow::value_pow;
use value_sub::value_sub;
