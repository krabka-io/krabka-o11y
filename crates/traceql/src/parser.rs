//! Recursive-descent `TraceQL` parser.

use crate::{
    ast::{
        Aggregate, ArithmeticOp, ComparisonOp, Field, FieldExpr, Intrinsic, NumericFilter,
        Pipeline, Query, QueryHints, ScalarExpr, Scope, SpansetExpr, StructuralOp, Value,
        WithBinding,
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

struct ArithmeticOperands {
    lhs: Value,
    rhs: Value,
}

#[derive(Clone, Copy)]
enum ValueArithmetic {
    Add,
    Subtract,
    Multiply,
}

impl ValueArithmetic {
    fn apply(self, lhs: f64, rhs: f64) -> f64 {
        match self {
            Self::Add => lhs + rhs,
            Self::Subtract => lhs - rhs,
            Self::Multiply => lhs * rhs,
        }
    }

    fn checked_int(self, lhs: i64, rhs: i64) -> Option<i64> {
        match self {
            Self::Add => lhs.checked_add(rhs),
            Self::Subtract => lhs.checked_sub(rhs),
            Self::Multiply => lhs.checked_mul(rhs),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Add => "addition",
            Self::Subtract => "subtraction",
            Self::Multiply => "multiplication",
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
        }
    }
}

fn apply_value_arithmetic(lhs: Value, rhs: Value, operation: ValueArithmetic) -> Result<Value> {
    match (ArithmeticOperands { lhs, rhs }) {
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Int(rhs),
        } => operation
            .checked_int(lhs, rhs)
            .map(Value::Int)
            .ok_or_else(|| {
                TraceqlError::Parse(format!("integer {} out of range", operation.name()))
            }),
        ArithmeticOperands {
            lhs: Value::Float(lhs),
            rhs: Value::Float(rhs),
        } => Ok(Value::Float(operation.apply(lhs, rhs))),
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Float(rhs),
        } => Ok(Value::Float(operation.apply(i64_to_f64(lhs)?, rhs))),
        ArithmeticOperands {
            lhs: Value::Float(lhs),
            rhs: Value::Int(rhs),
        } => Ok(Value::Float(operation.apply(lhs, i64_to_f64(rhs)?))),
        ArithmeticOperands {
            lhs: Value::Duration(lhs),
            rhs: Value::Duration(rhs),
        } if !matches!(operation, ValueArithmetic::Multiply) => operation
            .checked_int(lhs, rhs)
            .map(Value::Duration)
            .ok_or_else(|| {
                TraceqlError::Parse(format!("duration {} out of range", operation.name()))
            }),
        ArithmeticOperands {
            lhs: Value::Duration(lhs),
            rhs: Value::Int(rhs),
        }
        | ArithmeticOperands {
            lhs: Value::Int(rhs),
            rhs: Value::Duration(lhs),
        } if matches!(operation, ValueArithmetic::Multiply) => operation
            .checked_int(lhs, rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration multiplication out of range".into())),
        ArithmeticOperands { lhs, rhs } => arithmetic_type_error(operation.symbol(), &lhs, &rhs),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    struct QueryExpectation<T> {
        query: &'static str,
        expected: T,
    }
    use crate::ast::*;

    #[test]
    fn scalar_field_arithmetic_has_real_ast_precedence_and_rejects_dynamic_regex() {
        let query =
            parse("{ (span.amount + span.tax) * 2 >= span.total && -duration < 0ns }").unwrap();
        let SpansetExpr::Selector(expr) = query.root else {
            panic!("selector")
        };
        let FieldExpr::And(lhs, rhs) = *expr else {
            panic!("composed predicate")
        };
        let FieldExpr::ExpressionComparison { lhs, .. } = *lhs else {
            panic!("arithmetic predicate")
        };
        let ScalarExpr::Binary {
            op: ArithmeticOp::Mul,
            lhs,
            ..
        } = lhs
        else {
            panic!("multiplication")
        };
        assert!(matches!(
            *lhs,
            ScalarExpr::Binary {
                op: ArithmeticOp::Add,
                ..
            }
        ));
        assert!(matches!(
            *rhs,
            FieldExpr::ExpressionComparison {
                lhs: ScalarExpr::Negate(_),
                ..
            }
        ));
        for query in [
            "{ .foo =~ .bar }",
            "{ .foo !~ .bar }",
            "{ (name + 1) > 0 }",
            "{ -(true) > 0 }",
        ] {
            assert!(parse(query).is_err(), "{query}");
        }
        assert!(parse("{ .foo =~ \"bar.*\" }").is_ok());
    }

    #[test]
    fn regex_operand_types_are_validated_before_attribute_discovery() {
        for operand in ["1", "1.5", "true", "nil"] {
            for operator in ["=~", "!~"] {
                for selector in [
                    format!(".missing {operator} {operand}"),
                    format!("(.missing {operator} {operand}) || name = \"known\""),
                ] {
                    assert!(matches!(
                        parse(&format!("{{ {selector} }}")),
                        Err(TraceqlError::Plan(message)) if message == "regex comparison requires string value"
                    ));
                }
            }
        }
        assert!(parse(r#"{ .missing =~ "known.*" }"#).is_ok());
    }

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
                        key: "span:duration".into(),
                    },
                    op: ComparisonOp::Gt,
                    rhs: Value::Duration(100_000_000),
                }))
        );
    }

    #[test]
    fn span_nested_set_parent_intrinsic_resolves() {
        let lhs = selector_lhs("{ span:nestedSetParent > 0 }");
        assert!(lhs.scope == Scope::Intrinsic(Intrinsic::NestedSetParent));
    }

    #[test]
    fn scopeless_intrinsics_resolve_to_intrinsic_scope() {
        // Tempo treats these reserved names as intrinsics when scopeless. Krabka
        // previously parsed them as `Scope::Both` attributes, so they silently
        // matched nothing (`attr.duration`, `attr.nestedSetParent`, …) and broke
        // every Grafana Tempo/Traces-Drilldown query, which writes them bare.
        for QueryExpectation {
            query,
            expected: want,
        } in [
            QueryExpectation {
                query: "{ duration > 1s }",
                expected: Intrinsic::Duration,
            },
            QueryExpectation {
                query: "{ name != \"\" }",
                expected: Intrinsic::Name,
            },
            QueryExpectation {
                query: "{ kind = server }",
                expected: Intrinsic::Kind,
            },
            QueryExpectation {
                query: "{ status = error }",
                expected: Intrinsic::Status,
            },
            QueryExpectation {
                query: "{ statusMessage != \"\" }",
                expected: Intrinsic::StatusMessage,
            },
            QueryExpectation {
                query: "{ childCount > 0 }",
                expected: Intrinsic::ChildCount,
            },
            QueryExpectation {
                query: "{ nestedSetLeft > 0 }",
                expected: Intrinsic::NestedSetLeft,
            },
            QueryExpectation {
                query: "{ nestedSetRight > 0 }",
                expected: Intrinsic::NestedSetRight,
            },
            QueryExpectation {
                query: "{ nestedSetParent < 0 }",
                expected: Intrinsic::NestedSetParent,
            },
            QueryExpectation {
                query: "{ rootName != \"\" }",
                expected: Intrinsic::TraceRootName,
            },
            QueryExpectation {
                query: "{ rootServiceName != \"\" }",
                expected: Intrinsic::TraceRootService,
            },
            QueryExpectation {
                query: "{ traceDuration > 1s }",
                expected: Intrinsic::TraceDuration,
            },
        ] {
            let lhs = selector_lhs(query);
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
            let lhs = selector_lhs(&format!("{{ {key} = 1 }}"));
            assert!(lhs.scope == Scope::Both, "{key}: {:?}", lhs.scope);
            assert!(lhs.key == key);
        }
    }

    #[test]
    fn leading_dot_keeps_intrinsic_name_as_attribute() {
        // `.duration` / `.name` are explicit attribute lookups, never the intrinsic.
        let lhs = selector_lhs("{ .duration = 5 }");
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
        for QueryExpectation { query, expected } in [
            QueryExpectation {
                query: "{ .a = 1 } >> { .b = 2 }",
                expected: StructuralOp::Descendant,
            },
            QueryExpectation {
                query: "{ .a = 1 } << { .b = 2 }",
                expected: StructuralOp::Ancestor,
            },
            QueryExpectation {
                query: "{ .a = 1 } > { .b = 2 }",
                expected: StructuralOp::Child,
            },
            QueryExpectation {
                query: "{ .a = 1 } < { .b = 2 }",
                expected: StructuralOp::Parent,
            },
            QueryExpectation {
                query: "{ .a = 1 } ~ { .b = 2 }",
                expected: StructuralOp::Sibling,
            },
            QueryExpectation {
                query: "{ .a = 1 } !>> { .b = 2 }",
                expected: StructuralOp::NegDescendant,
            },
            QueryExpectation {
                query: "{ .a = 1 } !<< { .b = 2 }",
                expected: StructuralOp::NegAncestor,
            },
            QueryExpectation {
                query: "{ .a = 1 } !> { .b = 2 }",
                expected: StructuralOp::NegChild,
            },
            QueryExpectation {
                query: "{ .a = 1 } !< { .b = 2 }",
                expected: StructuralOp::NegParent,
            },
            QueryExpectation {
                query: "{ .a = 1 } !~ { .b = 2 }",
                expected: StructuralOp::NegSibling,
            },
            QueryExpectation {
                query: "{ .a = 1 } &>> { .b = 2 }",
                expected: StructuralOp::UnionDescendant,
            },
            QueryExpectation {
                query: "{ .a = 1 } &<< { .b = 2 }",
                expected: StructuralOp::UnionAncestor,
            },
            QueryExpectation {
                query: "{ .a = 1 } &> { .b = 2 }",
                expected: StructuralOp::UnionChild,
            },
            QueryExpectation {
                query: "{ .a = 1 } &< { .b = 2 }",
                expected: StructuralOp::UnionParent,
            },
            QueryExpectation {
                query: "{ .a = 1 } &~ { .b = 2 }",
                expected: StructuralOp::UnionSibling,
            },
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
        let unknown = parse("{ .a = 1 } with (unknown=true)").unwrap();
        assert!(
            unknown.hints.values
                == vec![QueryHintValue {
                    name: "unknown".into(),
                    value: Value::Bool(true),
                }]
        );
        assert!(!unknown.hints.most_recent);
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
        for QueryExpectation {
            query,
            expected: want,
        } in [
            QueryExpectation {
                query: "{ .x = 6 / 2 }",
                expected: Value::Int(3),
            },
            QueryExpectation {
                query: "{ .x = 7 % 3 }",
                expected: Value::Int(1),
            },
        ] {
            assert!(selector_rhs(query) == want, "value mismatch for {query}");
        }
    }

    #[test]
    fn quantile_leading_zero_fraction_preserved() {
        for QueryExpectation { query, expected } in [
            QueryExpectation {
                query: "{ .a = 1 } | quantile_over_time(span:duration, .05)",
                expected: 0.05,
            },
            QueryExpectation {
                query: "{ .a = 1 } | quantile_over_time(span:duration, .99)",
                expected: 0.99,
            },
            QueryExpectation {
                query: "{ .a = 1 } | quantile_over_time(span:duration, .5)",
                expected: 0.5,
            },
            QueryExpectation {
                query: "{ .a = 1 } | quantile_over_time(span:duration, .9)",
                expected: 0.9,
            },
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

    fn selector_lhs(query: &str) -> Field {
        let q = parse(query).unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector for {query}")
        };
        let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
            panic!("comparison for {query}")
        };
        lhs.clone()
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
    fn query_hints_accept_static_values_and_preserve_unknown_or_wrong_types() {
        let q = parse("{} with(most_recent=5, unused=\"opaque\", sample=0.5, timeout=1s)").unwrap();
        assert!(!q.hints.most_recent);
        assert!(
            q.hints.values
                == vec![
                    QueryHintValue {
                        name: "most_recent".into(),
                        value: Value::Int(5),
                    },
                    QueryHintValue {
                        name: "unused".into(),
                        value: Value::Str("opaque".into()),
                    },
                    QueryHintValue {
                        name: "sample".into(),
                        value: Value::Float(0.5),
                    },
                    QueryHintValue {
                        name: "timeout".into(),
                        value: Value::Duration(1_000_000_000),
                    }
                ]
        );
        assert!(parse("{} with(unused=span.name)").is_err());
    }

    #[test]
    fn hints_keep_first_boolean_and_prefer_float_over_integer_sampling() {
        let q = parse(
            "{} with(most_recent=false, most_recent=true, sample=1, sample=0.5, sample=0.25)",
        )
        .unwrap();
        assert!(!q.hints.most_recent);
        assert!(q.hints.numeric("sample").unwrap().to_bits() == 0.5_f64.to_bits());
        for query in [
            "{} with(unused=1+2)",
            "{} with(unused=nil)",
            "{} with(unused=unknown)",
            "{} with(unused=-1)",
            "{} | count() > 1 < 2",
        ] {
            assert!(parse(query).is_err(), "{query}");
        }
        let q = parse("{} | min_over_time(duration) > 10ms < 1s").unwrap();
        assert!(
            q.pipeline[1]
                == Pipeline::Filter {
                    op: ComparisonOp::Gt,
                    value: 0.01
                }
        );
        assert!(
            q.pipeline[2]
                == Pipeline::Filter {
                    op: ComparisonOp::Lt,
                    value: 1.0
                }
        );
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
        for QueryExpectation {
            query,
            expected: want,
        } in [
            QueryExpectation {
                query: "{ .a = 1 } | sum(.x)",
                expected: Aggregate::Sum(Field {
                    scope: Scope::Both,
                    key: "x".into(),
                }),
            },
            QueryExpectation {
                query: "{ .a = 1 } | avg(.x)",
                expected: Aggregate::Avg(Field {
                    scope: Scope::Both,
                    key: "x".into(),
                }),
            },
            QueryExpectation {
                query: "{ .a = 1 } | max(.x)",
                expected: Aggregate::Max(Field {
                    scope: Scope::Both,
                    key: "x".into(),
                }),
            },
            QueryExpectation {
                query: "{ .a = 1 } | min(.x)",
                expected: Aggregate::Min(Field {
                    scope: Scope::Both,
                    key: "x".into(),
                }),
            },
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
        for QueryExpectation {
            query,
            expected: want,
        } in [
            QueryExpectation {
                query: "{ .a = 1 } | sum_over_time(span:duration)",
                expected: Aggregate::SumOverTime(Field {
                    scope: Scope::Intrinsic(Intrinsic::Duration),
                    key: "span:duration".into(),
                }),
            },
            QueryExpectation {
                query: "{ .a = 1 } | min_over_time(span:duration)",
                expected: Aggregate::MinOverTime(Field {
                    scope: Scope::Intrinsic(Intrinsic::Duration),
                    key: "span:duration".into(),
                }),
            },
            QueryExpectation {
                query: "{ .a = 1 } | max_over_time(span:duration)",
                expected: Aggregate::MaxOverTime(Field {
                    scope: Scope::Intrinsic(Intrinsic::Duration),
                    key: "span:duration".into(),
                }),
            },
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
        for QueryExpectation {
            query,
            expected: scope,
        } in [
            QueryExpectation {
                query: "{ resource.region = \"us\" }",
                expected: Scope::Resource,
            },
            QueryExpectation {
                query: "{ event.foo = 1 }",
                expected: Scope::Event,
            },
            QueryExpectation {
                query: "{ link.foo = 1 }",
                expected: Scope::Link,
            },
            QueryExpectation {
                query: "{ instrumentation.foo = 1 }",
                expected: Scope::Instrumentation,
            },
            QueryExpectation {
                query: "{ parent.foo = 1 }",
                expected: Scope::Parent,
            },
        ] {
            let lhs = selector_lhs(query);
            assert!(lhs.scope == scope, "scope mismatch for {query}");
        }
    }

    #[test]
    fn unknown_scope_prefix_errors() {
        let msg = parse_err("{ bogus.foo = 1 }");
        assert!(msg.contains("unknown scope"));
    }

    #[test]
    fn intrinsic_keys_preserve_scoped_names_and_canonical_id_aliases() {
        for QueryExpectation { query, expected } in [
            QueryExpectation {
                query: "{} | count_over_time() by(name)",
                expected: "name",
            },
            QueryExpectation {
                query: "{} | count_over_time() by(span:name)",
                expected: "span:name",
            },
            QueryExpectation {
                query: "{} | count_over_time() by(duration)",
                expected: "duration",
            },
            QueryExpectation {
                query: "{} | count_over_time() by(trace:duration)",
                expected: "trace:duration",
            },
            QueryExpectation {
                query: "{} | count_over_time() by(span:parentId)",
                expected: "span:parentID",
            },
            QueryExpectation {
                query: "{} | count_over_time() by(link:spanId)",
                expected: "link:spanID",
            },
        ] {
            let query = parse(query).unwrap();
            let Pipeline::By(fields) = &query.pipeline[1] else {
                panic!("metrics grouping");
            };
            assert!(fields[0].key == expected);
        }
    }

    #[test]
    fn unknown_intrinsic_errors() {
        let msg = parse_err("{ span:bogus = 1 }");
        assert!(msg.contains("unknown intrinsic"));
    }

    #[test]
    fn bare_ident_is_both_scope() {
        let lhs = selector_lhs("{ foo = 1 }");
        assert!(lhs.scope == Scope::Both);
        assert!(lhs.key == "foo");
    }

    #[test]
    fn bare_field_without_comparison_is_boolean_predicate() {
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
    fn scalar_aggregate_and_group_expressions_retain_ast_and_validate_types() {
        let query = parse("{} | by(span.cost / 2) | avg(duration * 2) > 10ms").unwrap();
        assert!(matches!(
            &query.pipeline[0],
            Pipeline::Group(ScalarExpr::Binary {
                op: ArithmeticOp::Div,
                ..
            })
        ));
        assert!(matches!(
            &query.pipeline[1],
            Pipeline::Aggregate(Aggregate::Expression { .. })
        ));
        for query in [
            "{} | avg(name)",
            "{} | sum(2)",
            "{} | by(2)",
            "{} | by(name + 1)",
        ] {
            assert!(parse(query).is_err(), "{query}");
        }
        assert!(parse("{} | by(name)").is_ok());
        let boolean = parse("{} | by(span.cost > 1)").unwrap();
        assert!(matches!(
            &boolean.pipeline[0],
            Pipeline::Group(ScalarExpr::Predicate(_))
        ));
        assert!(parse("{} | avg(span.cost > 1)").is_err());
    }

    #[test]
    fn parent_expression_rejections_preserve_legacy_constant_parent_selectors() {
        for query in [
            "{} | by(parent.foo)",
            "{} | by(span.foo, parent.bar)",
            "{} | by(parent.foo = 1)",
            "{} | sum(parent.foo + 1) > 1",
            "{ parent.foo + 1 > span.foo }",
            "{ parent.foo }",
        ] {
            assert!(parse(query).is_err(), "{query}");
        }
        assert!(parse("{ parent.foo = 1 }").is_ok());
        assert!(parse("{ parent.foo != nil }").is_ok());
        assert!(parse("{} | by(span.foo = 1)").is_ok());
    }

    #[test]
    fn boolean_predicates_and_not_exists_follow_intrinsic_types() {
        for query in [
            "{ name }",
            "{ duration }",
            "{ span:childCount }",
            "{ trace:id = nil }",
            "{ event:name = nil }",
            "{ instrumentation:name = nil }",
            "{ resource.service.name = nil }",
        ] {
            assert!(parse(query).is_err(), "{query}");
        }
        for query in [
            "{ span.flag }",
            "{ event.flag }",
            "{ instrumentation.flag }",
            "{ .missing = nil }",
            "{ trace:id != nil }",
            "{ resource.service.name != nil }",
        ] {
            assert!(parse(query).is_ok(), "{query}");
        }
    }

    #[test]
    fn trace_and_event_intrinsics_resolve() {
        for QueryExpectation {
            query,
            expected: intrinsic,
        } in [
            QueryExpectation {
                query: "{ trace:rootName = \"x\" }",
                expected: Intrinsic::TraceRootName,
            },
            QueryExpectation {
                query: "{ trace:rootService = \"x\" }",
                expected: Intrinsic::TraceRootService,
            },
            QueryExpectation {
                query: "{ trace:id = \"x\" }",
                expected: Intrinsic::TraceId,
            },
            QueryExpectation {
                query: "{ event:name = \"x\" }",
                expected: Intrinsic::EventName,
            },
            QueryExpectation {
                query: "{ event:timeSinceStart > 5ms }",
                expected: Intrinsic::EventTimeSinceStart,
            },
            QueryExpectation {
                query: "{ link:traceID = \"x\" }",
                expected: Intrinsic::LinkTraceId,
            },
            QueryExpectation {
                query: "{ link:spanID = \"x\" }",
                expected: Intrinsic::LinkSpanId,
            },
            QueryExpectation {
                query: "{ instrumentation:version = \"x\" }",
                expected: Intrinsic::InstrumentationVersion,
            },
            QueryExpectation {
                query: "{ span:nestedSetLeft > 0 }",
                expected: Intrinsic::NestedSetLeft,
            },
            QueryExpectation {
                query: "{ span:nestedSetRight > 0 }",
                expected: Intrinsic::NestedSetRight,
            },
            QueryExpectation {
                query: "{ span:parentId = \"x\" }",
                expected: Intrinsic::ParentId,
            },
        ] {
            let lhs = selector_lhs(query);
            assert!(
                lhs.scope == Scope::Intrinsic(intrinsic),
                "intrinsic mismatch for {query}"
            );
        }
    }

    // ---- value parsing ----

    #[test]
    fn primary_values_cover_all_literal_kinds() {
        for QueryExpectation {
            query,
            expected: want,
        } in [
            QueryExpectation {
                query: "{ .a = \"s\" }",
                expected: Value::Str("s".into()),
            },
            QueryExpectation {
                query: "{ .a = 42 }",
                expected: Value::Int(42),
            },
            QueryExpectation {
                query: "{ .a = 1.5 }",
                expected: Value::Float(1.5),
            },
            QueryExpectation {
                query: "{ .a = true }",
                expected: Value::Bool(true),
            },
            QueryExpectation {
                query: "{ .a = nil }",
                expected: Value::Nil,
            },
            // bare identifier on a non-duration field folds to a string value.
            QueryExpectation {
                query: "{ .a = ident }",
                expected: Value::Str("ident".into()),
            },
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
        for QueryExpectation {
            query,
            expected: want,
        } in [
            QueryExpectation {
                query: "{ .a = 1 + 2.0 }",
                expected: 3.0,
            },
            QueryExpectation {
                query: "{ .a = 2.0 + 1 }",
                expected: 3.0,
            },
            QueryExpectation {
                query: "{ .a = 5 - 1.5 }",
                expected: 3.5,
            },
            QueryExpectation {
                query: "{ .a = 5.5 - 1 }",
                expected: 4.5,
            },
            QueryExpectation {
                query: "{ .a = 2 * 1.5 }",
                expected: 3.0,
            },
            QueryExpectation {
                query: "{ .a = 1.5 * 2 }",
                expected: 3.0,
            },
            QueryExpectation {
                query: "{ .a = 3 / 1.5 }",
                expected: 2.0,
            },
            QueryExpectation {
                query: "{ .a = 3.0 / 2 }",
                expected: 1.5,
            },
            QueryExpectation {
                query: "{ .a = 1.0 + 2.0 }",
                expected: 3.0,
            },
            QueryExpectation {
                query: "{ .a = 6.0 / 2.0 }",
                expected: 3.0,
            },
            QueryExpectation {
                query: "{ .a = 1.0 - 0.5 }",
                expected: 0.5,
            },
            QueryExpectation {
                query: "{ .a = 2.0 * 2.0 }",
                expected: 4.0,
            },
            QueryExpectation {
                query: "{ .a = 2.0 * 3.0 }",
                expected: 6.0,
            },
            QueryExpectation {
                query: "{ .a = 5 / 2 }",
                expected: 2.5,
            },
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
        for QueryExpectation {
            query,
            expected: want_ns,
        } in [
            QueryExpectation {
                query: "{ span:duration = 5ns }",
                expected: 5,
            },
            QueryExpectation {
                query: "{ span:duration = 5us }",
                expected: 5_000,
            },
            QueryExpectation {
                query: "{ span:duration = 5ms }",
                expected: 5_000_000,
            },
            QueryExpectation {
                query: "{ span:duration = 5s }",
                expected: 5_000_000_000,
            },
            QueryExpectation {
                query: "{ span:duration = 2m }",
                expected: 120_000_000_000,
            },
            QueryExpectation {
                query: "{ span:duration = 1h }",
                expected: 3_600_000_000_000,
            },
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
        for QueryExpectation { query, expected } in [
            QueryExpectation {
                query: "{ .a < 5 }",
                expected: ComparisonOp::Lt,
            },
            QueryExpectation {
                query: "{ .a <= 5 }",
                expected: ComparisonOp::Lte,
            },
            QueryExpectation {
                query: "{ .a > 5 }",
                expected: ComparisonOp::Gt,
            },
            QueryExpectation {
                query: "{ .a >= 5 }",
                expected: ComparisonOp::Gte,
            },
            QueryExpectation {
                query: "{ .a != 5 }",
                expected: ComparisonOp::Neq,
            },
            QueryExpectation {
                query: "{ .a =~ \"x\" }",
                expected: ComparisonOp::Re,
            },
            QueryExpectation {
                query: "{ .a !~ \"x\" }",
                expected: ComparisonOp::Nre,
            },
            QueryExpectation {
                query: "{ .a = 5 }",
                expected: ComparisonOp::Eq,
            },
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
        assert!(
            selector_rhs("{ span:duration = 9223372036854775807ns }") == Value::Duration(i64::MAX)
        );
        // Both one nanosecond beyond the boundary and a scaled duration must
        // return our diagnostic, independent of integer-conversion wording.
        for literal in ["9223372036854775808ns", "100000000000h"] {
            let msg = parse_err(&format!("{{ span:duration = {literal} }}"));
            assert!(msg == format!("duration out of range: {literal:?}"));
        }
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
mod duration;
use duration::parse_duration_nanos;
mod arithmetic;
use arithmetic::{value_div, value_pow};
mod intrinsic;
mod parser_type;
use intrinsic::intrinsic;
use parser_type::Parser;

mod scalar_type;
use scalar_type::validate_scalar_comparison;

/// Default `topN` for `compare()` when the query omits the argument, as in Tempo.
pub(crate) const DEFAULT_COMPARE_TOP_N: usize = 10;

pub(crate) fn arithmetic_type_error(op: &str, lhs: &Value, rhs: &Value) -> Result<Value> {
    Err(TraceqlError::Parse(format!(
        "operator {op} is not supported for {lhs:?} and {rhs:?}"
    )))
}

pub(crate) fn i64_to_f64(value: i64) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Parse(e.to_string()))
}

pub(crate) fn is_duration_field(field: &Field) -> bool {
    matches!(
        field.scope,
        Scope::Intrinsic(
            Intrinsic::Duration | Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart
        )
    )
}

pub(crate) fn numeric_filter_field() -> Field {
    Field {
        scope: Scope::Intrinsic(Intrinsic::Duration),
        key: String::new(),
    }
}

pub(crate) fn numeric_filter_value(value: Value) -> Result<f64> {
    match value {
        Value::Int(value) | Value::Duration(value) => i64_to_f64(value),
        Value::Float(value) => Ok(value),
        other => Err(TraceqlError::Parse(format!(
            "expected numeric filter value, got {other:?}"
        ))),
    }
}

/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn parse(query: &str) -> Result<Query> {
    Parser {
        tokens: lex(query)?,
        pos: 0,
    }
    .parse_query()
}

pub(crate) fn value_add(lhs: Value, rhs: Value) -> Result<Value> {
    apply_value_arithmetic(lhs, rhs, ValueArithmetic::Add)
}

pub(crate) fn value_mul(lhs: Value, rhs: Value) -> Result<Value> {
    apply_value_arithmetic(lhs, rhs, ValueArithmetic::Multiply)
}

pub(crate) fn value_sub(lhs: Value, rhs: Value) -> Result<Value> {
    apply_value_arithmetic(lhs, rhs, ValueArithmetic::Subtract)
}

pub(crate) fn scope(s: &str) -> Result<Scope> {
    match s {
        "span" => Ok(Scope::Span),
        "resource" => Ok(Scope::Resource),
        "parent" => Ok(Scope::Parent),
        "event" => Ok(Scope::Event),
        "link" => Ok(Scope::Link),
        "instrumentation" => Ok(Scope::Instrumentation),
        _ => Err(TraceqlError::Parse(format!("unknown scope {s:?}"))),
    }
}

/// Resolves a bare identifier with no scope to a `TraceQL` intrinsic.
///
/// The set matches the reserved intrinsic field names that Tempo recognizes
/// without a scope. A name outside this set is a span or resource attribute,
/// that is, `Scope::Both`. This function excludes `parentID`, `id`, `traceID`,
/// and the event, link, and instrumentation intrinsics on purpose. Tempo needs
/// an explicit scope such as `span:` or `trace:` for those names.
pub(crate) fn scopeless_intrinsic(name: &str) -> Option<Intrinsic> {
    Some(match name {
        "duration" => Intrinsic::Duration,
        "kind" => Intrinsic::Kind,
        "name" => Intrinsic::Name,
        "status" => Intrinsic::Status,
        "statusMessage" => Intrinsic::StatusMessage,
        "childCount" => Intrinsic::ChildCount,
        "nestedSetLeft" => Intrinsic::NestedSetLeft,
        "nestedSetRight" => Intrinsic::NestedSetRight,
        "nestedSetParent" => Intrinsic::NestedSetParent,
        "rootName" => Intrinsic::TraceRootName,
        "rootServiceName" => Intrinsic::TraceRootService,
        "traceDuration" => Intrinsic::TraceDuration,
        _ => return None,
    })
}

pub(crate) fn value_mod(lhs: Value, rhs: Value) -> Result<Value> {
    let operands = ArithmeticOperands { lhs, rhs };
    match operands {
        ArithmeticOperands {
            rhs: Value::Int(0) | Value::Duration(0),
            ..
        } => Err(TraceqlError::Parse("modulo by zero".into())),
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Int(rhs),
        } => lhs
            .checked_rem(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer modulo out of range".into())),
        ArithmeticOperands {
            lhs: Value::Duration(lhs),
            rhs: Value::Duration(rhs),
        } => lhs
            .checked_rem(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration modulo out of range".into())),
        ArithmeticOperands { lhs, rhs } => arithmetic_type_error("%", &lhs, &rhs),
    }
}

pub(crate) fn value_neg(value: Value) -> Result<Value> {
    match value {
        Value::Int(value) => value
            .checked_neg()
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer negation out of range".into())),
        Value::Float(value) => Ok(Value::Float(-value)),
        Value::Duration(value) => value
            .checked_neg()
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration negation out of range".into())),
        other => Err(TraceqlError::Parse(format!(
            "unary - is not supported for {other:?}"
        ))),
    }
}
