use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_units::{ByteSize, convert::ByteSizeExt};

use crate::{
    ast::{ComparisonOp, Field, FieldExpr, Intrinsic, Scope, Value},
    error::{Result, TraceqlError},
    planner::{PlannedSpanset, PlannerContext},
    span_columns::{
        ATTR_PREFIX, COL_CHILD_COUNT, COL_DURATION, COL_EVENT_NAME, COL_EVENT_TIME_SINCE_START,
        COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND, COL_LINK_SPAN_ID,
        COL_LINK_TRACE_ID, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID,
        COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_STATUS_CODE,
        COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, INSTRUMENTATION_ATTR_PREFIX,
    },
    store::{MatchCmp, MatchScope, MatchValue, SpanMatcher, SpanStore},
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::{SpansetExpr, parser::parse};

    fn selector(query: &str) -> FieldExpr {
        let q = parse(query).unwrap();
        let SpansetExpr::Selector(fe) = q.root else {
            panic!("selector")
        };
        *fe
    }

    #[test]
    fn conjunctive_comparisons_become_prefilter_matchers() {
        let ms = field_expr_to_matchers(&selector("{ .a = 1 && .b =~ \"x\" }"));
        assert!(
            ms == vec![
                SpanMatcher {
                    scope: MatchScope::Both,
                    key: "a".into(),
                    op: MatchCmp::Eq,
                    value: MatchValue::Int(1),
                    negated: false,
                },
                SpanMatcher {
                    scope: MatchScope::Both,
                    key: "b".into(),
                    op: MatchCmp::Re,
                    value: MatchValue::Str("x".into()),
                    negated: false,
                },
            ]
        );
    }

    #[test]
    fn disjunction_does_not_prefilter() {
        let ms = field_expr_to_matchers(&selector("{ .a = 1 || .b = 2 }"));
        assert!(ms.is_empty());
    }

    #[test]
    fn const_true_is_and_identity_in_matcher_disjuncts() {
        // `{ .a != nil }` is one disjunct of one matcher; ANDing `&& true` (a
        // `FieldExpr::Const(true)`) must NOT collapse the DNF to `None`, which
        // would drop the `attr.a` projection and make planning fail. The const
        // is the AND-identity: the disjuncts are unchanged.
        let base = field_expr_to_matcher_disjuncts(&selector("{ .a != nil }")).unwrap();
        for q in ["{ .a != nil && true }", "{ true && .a != nil }"] {
            let with_const = field_expr_to_matcher_disjuncts(&selector(q)).unwrap();
            assert!(with_const == base, "{q}: {with_const:?} != {base:?}");
        }
        // The prefilter matcher is still collected so the scan projects `attr.a`.
        let ms = field_expr_to_matchers(&selector("{ .a != nil && true }"));
        assert!(ms.len() == 1 && ms[0].key == "a");
    }

    #[test]
    fn const_false_is_match_none_in_matcher_disjuncts() {
        // `false` is the annihilator: zero disjuncts (match nothing), and ANDing
        // it in drops all other disjuncts.
        assert!(
            field_expr_to_matcher_disjuncts(&selector("{ false }")).unwrap()
                == Vec::<Vec<SpanMatcher>>::new()
        );
        assert!(
            field_expr_to_matcher_disjuncts(&selector("{ .a != nil && false }"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn const_true_alone_is_single_empty_match_all_disjunct() {
        // `{}` / `{ true }` => exactly one disjunct with no matchers, which the
        // planner treats as an unfiltered (match-all) scan.
        let d = field_expr_to_matcher_disjuncts(&selector("{ true }")).unwrap();
        assert!(d.len() == 1 && d[0].is_empty());
    }

    #[test]
    fn nil_comparison_is_presence_prefilter() {
        let ms = field_expr_to_matchers(&selector("{ .a != nil }"));
        assert!(
            ms == vec![SpanMatcher {
                scope: MatchScope::Both,
                key: "a".into(),
                op: MatchCmp::Neq,
                value: MatchValue::Nil,
                negated: false,
            }]
        );
    }

    #[test]
    fn duration_value_maps_to_integer_nanos() {
        let ms = field_expr_to_matchers(&selector("{ span:duration > 100ms }"));
        assert!(ms[0].scope == MatchScope::Intrinsic);
        assert!(ms[0].value == MatchValue::Int(100_000_000));
    }

    #[test]
    fn non_finite_folded_float_comparison_errors_cleanly() {
        // A non-finite folded float (e.g. from overflowing float multiplication)
        // must be rejected by the SQL emitter rather than interpolated as a
        // literal `inf`/`NaN`, which DataFusion cannot parse.
        let field = Field {
            scope: Scope::Both,
            key: "x".into(),
        };
        for bad in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let err = comparison_to_sql(&field, ComparisonOp::Gt, &Value::Float(bad));
            assert!(matches!(err, Err(TraceqlError::Plan(_))));
        }
        // Finite floats still produce SQL.
        let ok = comparison_to_sql(&field, ComparisonOp::Gt, &Value::Float(1.5));
        assert!(ok.is_ok());
    }

    fn intrinsic_field(intrinsic: Intrinsic) -> Field {
        Field {
            scope: Scope::Intrinsic(intrinsic),
            key: String::new(),
        }
    }

    fn attr_field(scope: Scope, key: &str) -> Field {
        Field {
            scope,
            key: key.into(),
        }
    }

    // ---- field_to_column: every intrinsic + every attribute scope ----

    #[test]
    fn field_to_column_maps_all_intrinsics() {
        let cases = [
            (Intrinsic::Name, COL_NAME),
            (Intrinsic::Duration, COL_DURATION),
            (Intrinsic::Kind, COL_KIND),
            (Intrinsic::Status, COL_STATUS_CODE),
            (Intrinsic::StatusMessage, COL_STATUS_MESSAGE),
            (Intrinsic::Id, COL_SPAN_ID),
            (Intrinsic::ParentId, COL_PARENT_SPAN_ID),
            (Intrinsic::TraceDuration, COL_TRACE_DURATION),
            (Intrinsic::TraceRootName, COL_ROOT_SPAN_NAME),
            (Intrinsic::TraceRootService, COL_ROOT_SERVICE_NAME),
            (Intrinsic::TraceId, COL_TRACE_ID),
            (Intrinsic::NestedSetLeft, COL_NS_LEFT),
            (Intrinsic::NestedSetRight, COL_NS_RIGHT),
            (Intrinsic::NestedSetParent, COL_PARENT_ID),
            (Intrinsic::ChildCount, COL_CHILD_COUNT),
            (Intrinsic::InstrumentationName, COL_INSTRUMENTATION_NAME),
            (
                Intrinsic::InstrumentationVersion,
                COL_INSTRUMENTATION_VERSION,
            ),
            (Intrinsic::EventName, COL_EVENT_NAME),
            (Intrinsic::EventTimeSinceStart, COL_EVENT_TIME_SINCE_START),
            (Intrinsic::LinkTraceId, COL_LINK_TRACE_ID),
            (Intrinsic::LinkSpanId, COL_LINK_SPAN_ID),
        ];
        for (intrinsic, expected) in cases {
            let col = field_to_column(&intrinsic_field(intrinsic.clone()));
            assert!(col == expected, "intrinsic {intrinsic:?} -> {col}");
        }
    }

    #[test]
    fn field_to_column_service_name_resolves_to_root_service() {
        // `service.name` short-circuits to the root-service column for both the
        // ambiguous (Both) and explicit Resource scopes.
        assert!(field_to_column(&attr_field(Scope::Both, "service.name")) == COL_ROOT_SERVICE_NAME);
        assert!(
            field_to_column(&attr_field(Scope::Resource, "service.name")) == COL_ROOT_SERVICE_NAME
        );
    }

    #[test]
    fn field_to_column_attribute_scopes_get_attr_prefix() {
        for scope in [
            Scope::Both,
            Scope::Span,
            Scope::Resource,
            Scope::Parent,
            Scope::Event,
            Scope::Link,
        ] {
            let col = field_to_column(&attr_field(scope.clone(), "region"));
            assert!(
                col == format!("{ATTR_PREFIX}region"),
                "scope {scope:?} -> {col}"
            );
        }
        assert!(
            field_to_column(&attr_field(Scope::Instrumentation, "region"))
                == format!("{ATTR_PREFIX}{INSTRUMENTATION_ATTR_PREFIX}region")
        );
    }

    // ---- comparison_to_sql: every operator, nil, regex ----

    #[test]
    fn comparison_to_sql_covers_all_operators() {
        let field = attr_field(Scope::Both, "x");
        let col = ident(&field_to_column(&field));
        let cases = [
            (ComparisonOp::Eq, format!("{col} = 1")),
            (ComparisonOp::Neq, format!("{col} != 1")),
            (ComparisonOp::Lt, format!("{col} < 1")),
            (ComparisonOp::Lte, format!("{col} <= 1")),
            (ComparisonOp::Gt, format!("{col} > 1")),
            (ComparisonOp::Gte, format!("{col} >= 1")),
        ];
        for (op, expected) in cases {
            let sql = comparison_to_sql(&field, op, &Value::Int(1)).unwrap();
            assert!(sql == expected, "{op:?} -> {sql}");
        }
    }

    #[test]
    fn comparison_to_sql_nil_uses_null_predicates() {
        let field = attr_field(Scope::Both, "x");
        let col = ident(&field_to_column(&field));
        assert!(
            comparison_to_sql(&field, ComparisonOp::Eq, &Value::Nil).unwrap()
                == format!("{col} IS NULL")
        );
        assert!(
            comparison_to_sql(&field, ComparisonOp::Neq, &Value::Nil).unwrap()
                == format!("{col} IS NOT NULL")
        );
    }

    #[test]
    fn comparison_to_sql_regex_is_anchored() {
        let field = attr_field(Scope::Both, "x");
        let col = ident(&field_to_column(&field));
        let re = comparison_to_sql(&field, ComparisonOp::Re, &Value::Str("ab".into())).unwrap();
        assert!(re == format!("regexp_like({col}, '^(?:ab)$')"));
        let nre = comparison_to_sql(&field, ComparisonOp::Nre, &Value::Str("ab".into())).unwrap();
        assert!(nre == format!("NOT regexp_like({col}, '^(?:ab)$')"));
    }

    #[test]
    fn comparison_to_sql_regex_against_non_string_errors() {
        let field = attr_field(Scope::Both, "x");
        for op in [ComparisonOp::Re, ComparisonOp::Nre] {
            let err = comparison_to_sql(&field, op, &Value::Int(3));
            assert!(matches!(err, Err(TraceqlError::Plan(_))));
        }
    }

    // ---- field_expr_to_sql: And / Or / Not / Field ----

    #[test]
    fn field_expr_to_sql_combines_boolean_operators() {
        for (query, expected) in [
            (
                "{ .a = 1 && .b = 2 }",
                "(\"attr.a\" = 1 AND \"attr.b\" = 2)",
            ),
            ("{ .a = 1 || .b = 2 }", "(\"attr.a\" = 1 OR \"attr.b\" = 2)"),
            ("{ !(.a = 1) }", "(NOT \"attr.a\" = 1)"),
        ] {
            let sql = field_expr_to_sql(&selector(query)).unwrap();
            assert!(sql == expected, "{query} -> {sql}");
        }
    }

    #[test]
    fn field_expr_to_sql_bare_field_is_presence_check() {
        let sql = field_expr_to_sql(&selector("{ .a }")).unwrap();
        assert!(sql == "\"attr.a\" IS NOT NULL");
    }

    // ---- selector_sql variants: span-only, parent-join, nested ----

    #[test]
    fn selector_sql_plain_predicate_filters_table() {
        let sql = selector_sql("\"spans\"", &selector("{ .a = 1 }")).unwrap();
        assert!(sql == "SELECT * FROM \"spans\" WHERE \"attr.a\" = 1");
    }

    #[test]
    fn selector_sql_parent_scope_emits_self_join() {
        let sql = selector_sql("\"spans\"", &selector("{ parent.a = 1 }")).unwrap();
        // Parent scope joins the table to itself on trace_id / parent_id linkage
        // and qualifies the parent predicate with the `p` alias.
        check!(sql.contains("FROM \"spans\" AS s JOIN \"spans\" AS p"));
        check!(sql.contains("WHERE p.\"attr.a\" = 1"));
        check!(sql.contains("s.\"parent_id\" = p.\"nested_set_left\""));
    }

    #[test]
    fn selector_sql_nested_scope_without_parent_selects_all() {
        // An event/link scoped selector has its filtering applied at scan time,
        // so the SQL projection is an unfiltered passthrough.
        let sql = selector_sql("\"spans\"", &selector("{ event.foo = 1 }")).unwrap();
        assert!(sql == "SELECT * FROM \"spans\"");
    }

    #[test]
    fn selector_sql_nested_and_parent_emits_qualified_parent_join() {
        // Mixing a nested (event) scope with a parent scope drives the
        // parent-qualified branch of `selector_sql_with_parent_table`.
        let fe = selector("{ event.foo = 1 && parent.a = 2 }");
        let sql = selector_sql_with_parent_table("\"spans\"", "\"parents\"", &fe).unwrap();
        assert!(sql.contains("FROM \"spans\" AS s JOIN \"parents\" AS p"));
        assert!(sql.contains("p.\"attr.a\" = 2"));
    }

    // ---- parent_field_expr_to_sql_qualified: And/Or/Not pruning ----

    #[test]
    fn parent_predicate_extracts_only_parent_conjuncts() {
        // AND keeps the parent conjunct and drops the non-parent one.
        let fe = selector("{ parent.a = 1 && .b = 2 }");
        let pred = parent_field_expr_to_sql_qualified(&fe, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "p.\"attr.a\" = 1");
    }

    #[test]
    fn parent_predicate_keeps_both_parent_conjuncts() {
        let fe = selector("{ parent.a = 1 && parent.b = 2 }");
        let pred = parent_field_expr_to_sql_qualified(&fe, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "(p.\"attr.a\" = 1 AND p.\"attr.b\" = 2)");
    }

    #[test]
    fn parent_predicate_bare_parent_field_is_presence() {
        let fe = selector("{ parent.a }");
        let pred = parent_field_expr_to_sql_qualified(&fe, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "p.\"attr.a\" IS NOT NULL");
    }

    #[test]
    fn parent_predicate_or_requires_both_sides_parent() {
        // A mixed OR cannot be pushed into the parent join (no safe predicate).
        let mixed = selector("{ parent.a = 1 || .b = 2 }");
        assert!(
            parent_field_expr_to_sql_qualified(&mixed, "s", "p")
                .unwrap()
                .is_none()
        );

        // Both sides parent -> a parent OR predicate is produced.
        let both = selector("{ parent.a = 1 || parent.b = 2 }");
        let pred = parent_field_expr_to_sql_qualified(&both, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "(p.\"attr.a\" = 1 OR p.\"attr.b\" = 2)");
    }

    #[test]
    fn parent_predicate_negation_wraps_inner() {
        let fe = selector("{ !(parent.a = 1) }");
        let pred = parent_field_expr_to_sql_qualified(&fe, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "(NOT p.\"attr.a\" = 1)");
    }

    #[test]
    fn parent_predicate_non_parent_leaf_yields_none() {
        let fe = selector("{ .b = 2 }");
        assert!(
            parent_field_expr_to_sql_qualified(&fe, "s", "p")
                .unwrap()
                .is_none()
        );
        let bare = selector("{ .b }");
        assert!(
            parent_field_expr_to_sql_qualified(&bare, "s", "p")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parent_predicate_and_of_non_parent_leaves_yields_none() {
        // Drives the And arm where both sides lower to None (no parent conjunct
        // anywhere) -> the whole And predicate is None.
        let fe = selector("{ .a = 1 && .b = 2 }");
        assert!(
            parent_field_expr_to_sql_qualified(&fe, "s", "p")
                .unwrap()
                .is_none()
        );
    }

    // ---- field_expr_to_sql_qualified: parent alias routing ----

    #[test]
    fn qualified_sql_routes_parent_to_parent_alias() {
        for (query, expected) in [
            (
                "{ parent.a = 1 && .b = 2 }",
                "(p.\"attr.a\" = 1 AND s.\"attr.b\" = 2)",
            ),
            (
                "{ parent.a = 1 || .b = 2 }",
                "(p.\"attr.a\" = 1 OR s.\"attr.b\" = 2)",
            ),
            ("{ !(parent.a = 1) }", "(NOT p.\"attr.a\" = 1)"),
            ("{ parent.a }", "p.\"attr.a\" IS NOT NULL"),
        ] {
            let sql = field_expr_to_sql_qualified(&selector(query), "s", "p").unwrap();
            assert!(sql == expected, "{query} -> {sql}");
        }
    }

    // ---- comparison_value_sql: enums, hex widths, errors ----

    #[test]
    fn comparison_value_sql_maps_status_enum() {
        let status = intrinsic_field(Intrinsic::Status);
        for (name, code) in [("unset", 0), ("ok", 1), ("error", 2), ("ERROR", 2)] {
            let sql = comparison_value_sql(&status, &Value::Str(name.into())).unwrap();
            assert!(sql == code.to_string(), "status {name} -> {sql}");
        }
        let err = comparison_value_sql(&status, &Value::Str("bogus".into()));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_maps_kind_enum() {
        let kind = intrinsic_field(Intrinsic::Kind);
        for (name, code) in [
            ("unspecified", 0),
            ("internal", 1),
            ("server", 2),
            ("client", 3),
            ("producer", 4),
            ("consumer", 5),
            ("Server", 2),
        ] {
            let sql = comparison_value_sql(&kind, &Value::Str(name.into())).unwrap();
            assert!(sql == code.to_string(), "kind {name} -> {sql}");
        }
        let err = comparison_value_sql(&kind, &Value::Str("bogus".into()));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_non_string_enum_falls_through_to_int() {
        // A numeric value against an enum intrinsic skips enum mapping and is
        // emitted as a plain integer literal.
        let kind = intrinsic_field(Intrinsic::Kind);
        let sql = comparison_value_sql(&kind, &Value::Int(3)).unwrap();
        assert!(sql == "3");
    }

    #[test]
    fn comparison_value_sql_trace_id_requires_16_byte_hex() {
        let trace = intrinsic_field(Intrinsic::TraceId);
        let hex = "0123456789abcdef0123456789abcdef"; // 32 chars = 16 bytes
        let sql = comparison_value_sql(&trace, &Value::Str(hex.into())).unwrap();
        assert!(sql == format!("X'{hex}'"));

        // Wrong length is rejected.
        let err = comparison_value_sql(&trace, &Value::Str("abcd".into()));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));

        // Non-string value is rejected with the hex-string error.
        let err = comparison_value_sql(&trace, &Value::Int(1));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_span_id_requires_8_byte_hex() {
        for intrinsic in [Intrinsic::Id, Intrinsic::ParentId, Intrinsic::LinkSpanId] {
            let field = intrinsic_field(intrinsic.clone());
            let hex = "0011223344556677"; // 16 chars = 8 bytes
            let sql = comparison_value_sql(&field, &Value::Str(hex.into())).unwrap();
            assert!(sql == format!("X'{hex}'"), "{intrinsic:?}");
        }
        let link_trace = intrinsic_field(Intrinsic::LinkTraceId);
        let hex = "0123456789abcdef0123456789abcdef";
        let sql = comparison_value_sql(&link_trace, &Value::Str(hex.into())).unwrap();
        assert!(sql == format!("X'{hex}'"));
    }

    #[test]
    fn comparison_value_sql_uppercases_to_lowercase_hex() {
        let trace = intrinsic_field(Intrinsic::TraceId);
        let hex = "0123456789ABCDEF0123456789ABCDEF";
        let sql = comparison_value_sql(&trace, &Value::Str(hex.into())).unwrap();
        assert!(sql == "X'0123456789abcdef0123456789abcdef'");
    }

    #[test]
    fn fixed_hex_lit_rejects_non_hex_characters() {
        // Right length but a non-hex digit ('g') -> error.
        let err = fixed_hex_lit("0123456789abcdeg", 8);
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_plain_field_uses_value_sql() {
        let field = attr_field(Scope::Both, "x");
        for (value, expected) in [
            (Value::Int(7), "7"),
            (Value::Str("hi".into()), "'hi'"),
            (Value::Bool(true), "true"),
            (Value::Duration(5), "5"),
        ] {
            let sql = comparison_value_sql(&field, &value).unwrap();
            assert!(sql == expected, "{value:?} -> {sql}");
        }
    }

    // ---- value_sql: bool literal, nil error ----

    #[test]
    fn value_sql_bool_and_nil() {
        assert!(value_sql(&Value::Bool(true)).unwrap() == "true");
        assert!(value_sql(&Value::Bool(false)).unwrap() == "false");
        let err = value_sql(&Value::Nil);
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    // ---- intrinsic_name: error-message labels ----

    #[test]
    fn intrinsic_name_labels_known_intrinsics() {
        let cases = [
            (Scope::Intrinsic(Intrinsic::TraceId), "trace:id"),
            (Scope::Intrinsic(Intrinsic::Id), "span:id"),
            (Scope::Intrinsic(Intrinsic::ParentId), "span:parentID"),
            (Scope::Intrinsic(Intrinsic::Kind), "span:kind"),
            (Scope::Intrinsic(Intrinsic::Status), "span:status"),
            // Anything else collapses to the generic label.
            (Scope::Both, "intrinsic"),
            (Scope::Intrinsic(Intrinsic::Name), "intrinsic"),
        ];
        for (scope, expected) in cases {
            let name = intrinsic_name(&scope);
            assert!(name == expected, "{scope:?} -> {name}");
        }
    }

    #[test]
    fn enum_value_sql_non_enum_scope_errors() {
        // enum_value_sql guards against being called for a non-enum scope.
        let err = enum_value_sql(&Scope::Both, "ok");
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    // ---- comparison_to_sql_qualified: operators + nil + regex ----

    #[test]
    fn comparison_to_sql_qualified_covers_operators_nil_and_regex() {
        let field = attr_field(Scope::Parent, "a");
        let col = qualified_field_ident(&field, "s", "p");
        let cases = [
            (ComparisonOp::Eq, Value::Int(1), format!("{col} = 1")),
            (ComparisonOp::Neq, Value::Int(1), format!("{col} != 1")),
            (ComparisonOp::Lt, Value::Int(1), format!("{col} < 1")),
            (ComparisonOp::Lte, Value::Int(1), format!("{col} <= 1")),
            (ComparisonOp::Gt, Value::Int(1), format!("{col} > 1")),
            (ComparisonOp::Gte, Value::Int(1), format!("{col} >= 1")),
            // nil
            (ComparisonOp::Eq, Value::Nil, format!("{col} IS NULL")),
            (ComparisonOp::Neq, Value::Nil, format!("{col} IS NOT NULL")),
            // regex
            (
                ComparisonOp::Re,
                Value::Str("x".into()),
                format!("regexp_like({col}, '^(?:x)$')"),
            ),
            (
                ComparisonOp::Nre,
                Value::Str("x".into()),
                format!("NOT regexp_like({col}, '^(?:x)$')"),
            ),
        ];
        for (op, value, expected) in cases {
            let sql = comparison_to_sql_qualified(&field, op, &value, "s", "p").unwrap();
            assert!(sql == expected, "{op:?} {value:?} -> {sql}");
        }
        // regex against non-string errors
        let err = comparison_to_sql_qualified(&field, ComparisonOp::Re, &Value::Int(1), "s", "p");
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn qualified_field_ident_routes_non_parent_to_span_alias() {
        let span = attr_field(Scope::Span, "a");
        assert!(qualified_field_ident(&span, "s", "p") == "s.\"attr.a\"");
        let parent = attr_field(Scope::Parent, "a");
        assert!(qualified_field_ident(&parent, "s", "p") == "p.\"attr.a\"");
    }

    // ---- matcher disjuncts: nested negation + Or prefilter ----

    #[test]
    fn nested_negation_lowers_to_negated_matcher_disjuncts() {
        // `!{ event.foo = 1 }` is a nested scope negation, which lowers to a
        // single disjunct of one negated matcher and is usable as a prefilter.
        let ms = field_expr_to_matchers(&selector("{ !(event.foo = 1) }"));
        assert!(
            ms == vec![SpanMatcher {
                scope: MatchScope::Event,
                key: "foo".into(),
                op: MatchCmp::Eq,
                value: MatchValue::Int(1),
                negated: true,
            }]
        );
    }

    #[test]
    fn non_nested_negation_does_not_prefilter() {
        // A negation over a non-nested scope returns no matchers.
        let ms = field_expr_to_matchers(&selector("{ !(.a = 1) }"));
        assert!(ms.is_empty());
    }

    #[test]
    fn or_of_comparisons_produces_disjunct_per_branch() {
        let disjuncts = field_expr_to_matcher_disjuncts(&selector("{ .a = 1 || .b = 2 }")).unwrap();
        assert!(
            disjuncts
                == vec![
                    vec![SpanMatcher {
                        scope: MatchScope::Both,
                        key: "a".into(),
                        op: MatchCmp::Eq,
                        value: MatchValue::Int(1),
                        negated: false,
                    }],
                    vec![SpanMatcher {
                        scope: MatchScope::Both,
                        key: "b".into(),
                        op: MatchCmp::Eq,
                        value: MatchValue::Int(2),
                        negated: false,
                    }],
                ]
        );
    }

    #[test]
    fn and_of_comparisons_cross_products_into_single_disjunct() {
        let disjuncts = field_expr_to_matcher_disjuncts(&selector("{ .a = 1 && .b = 2 }")).unwrap();
        assert!(disjuncts.len() == 1);
        assert!(disjuncts[0].len() == 2);
    }

    #[test]
    fn top_level_negation_of_non_nested_has_no_disjuncts() {
        // `field_expr_to_matcher_disjuncts` returns None for a non-nested Not,
        // signalling the prefilter cannot be derived.
        assert!(field_expr_to_matcher_disjuncts(&selector("{ !(.a = 1) }")).is_none());
    }

    #[test]
    fn nested_de_morgan_negation_expands_disjuncts() {
        // !(event.a = 1 || event.b = 2) -> AND of two negated matchers -> single disjunct.
        let disjuncts =
            field_expr_to_matcher_disjuncts(&selector("{ !(event.a = 1 || event.b = 2) }"))
                .unwrap();
        assert!(
            disjuncts
                == vec![vec![
                    SpanMatcher {
                        scope: MatchScope::Event,
                        key: "a".into(),
                        op: MatchCmp::Eq,
                        value: MatchValue::Int(1),
                        negated: true,
                    },
                    SpanMatcher {
                        scope: MatchScope::Event,
                        key: "b".into(),
                        op: MatchCmp::Eq,
                        value: MatchValue::Int(2),
                        negated: true,
                    },
                ]]
        );
    }

    #[test]
    fn nested_de_morgan_negation_of_and_expands_to_two_disjuncts() {
        // !(event.a = 1 && event.b = 2) -> OR of two negated matchers -> two disjuncts.
        let disjuncts =
            field_expr_to_matcher_disjuncts(&selector("{ !(event.a = 1 && event.b = 2) }"))
                .unwrap();
        assert!(disjuncts.len() == 2);
        assert!(disjuncts.iter().all(|d| d.len() == 1 && d[0].negated));
    }

    #[test]
    fn double_nested_negation_restores_positive_matcher() {
        // !!(event.a = 1) -> back to a non-negated matcher.
        let disjuncts =
            field_expr_to_matcher_disjuncts(&selector("{ !(!(event.a = 1)) }")).unwrap();
        assert!(
            disjuncts
                == vec![vec![SpanMatcher {
                    scope: MatchScope::Event,
                    key: "a".into(),
                    op: MatchCmp::Eq,
                    value: MatchValue::Int(1),
                    negated: false,
                }]]
        );
    }

    // ---- matcher_from_field_expr & friends: scope / cmp / value mapping ----

    #[test]
    fn matcher_from_bare_field_is_presence_neq_nil() {
        let m = matcher_from_field_expr(&selector("{ resource.region }")).unwrap();
        assert!(
            m == SpanMatcher {
                scope: MatchScope::Resource,
                key: "region".into(),
                op: MatchCmp::Neq,
                value: MatchValue::Nil,
                negated: false,
            }
        );
    }

    #[test]
    fn matcher_from_boolean_expr_is_none() {
        for query in [
            "{ .a = 1 && .b = 2 }",
            "{ .a = 1 || .b = 2 }",
            "{ !(.a = 1) }",
        ] {
            assert!(
                matcher_from_field_expr(&selector(query)).is_none(),
                "{query}"
            );
        }
    }

    #[test]
    fn match_scope_covers_every_scope() {
        let cases = [
            (Scope::Both, MatchScope::Both),
            (Scope::Span, MatchScope::Span),
            (Scope::Resource, MatchScope::Resource),
            (Scope::Parent, MatchScope::Parent),
            (Scope::Event, MatchScope::Event),
            (Scope::Link, MatchScope::Link),
            (Scope::Instrumentation, MatchScope::Instrumentation),
            (Scope::Intrinsic(Intrinsic::Name), MatchScope::Intrinsic),
        ];
        for (scope, expected) in cases {
            let got = match_scope(&scope);
            assert!(got == expected, "{scope:?} -> {got:?}");
        }
    }

    #[test]
    fn match_cmp_covers_every_operator() {
        let cases = [
            (ComparisonOp::Eq, MatchCmp::Eq),
            (ComparisonOp::Neq, MatchCmp::Neq),
            (ComparisonOp::Lt, MatchCmp::Lt),
            (ComparisonOp::Lte, MatchCmp::Lte),
            (ComparisonOp::Gt, MatchCmp::Gt),
            (ComparisonOp::Gte, MatchCmp::Gte),
            (ComparisonOp::Re, MatchCmp::Re),
            (ComparisonOp::Nre, MatchCmp::Nre),
        ];
        for (op, expected) in cases {
            let got = match_cmp(op);
            assert!(got == expected, "{op:?} -> {got:?}");
        }
    }

    #[test]
    fn match_value_covers_every_value_kind() {
        let cases = [
            (Value::Str("x".into()), MatchValue::Str("x".into())),
            (Value::Int(3), MatchValue::Int(3)),
            (Value::Duration(9), MatchValue::Int(9)),
            (Value::Float(1.5), MatchValue::Float(1.5)),
            (Value::Bool(true), MatchValue::Bool(true)),
            (Value::Nil, MatchValue::Nil),
        ];
        for (value, expected) in cases {
            let got = match_value(&value);
            assert!(got == expected, "{value:?} -> {got:?}");
        }
    }

    #[test]
    fn matcher_key_uses_intrinsic_canonical_names() {
        let cases = [
            (Intrinsic::Name, "span:name"),
            (Intrinsic::Duration, "span:duration"),
            (Intrinsic::Kind, "span:kind"),
            (Intrinsic::Status, "span:status"),
            (Intrinsic::StatusMessage, "span:statusMessage"),
            (Intrinsic::Id, "span:id"),
            (Intrinsic::ParentId, "span:parentID"),
            (Intrinsic::TraceDuration, "trace:duration"),
            (Intrinsic::TraceRootName, "trace:rootName"),
            (Intrinsic::TraceRootService, "trace:rootService"),
            (Intrinsic::TraceId, "trace:id"),
            (Intrinsic::NestedSetLeft, "span:nestedSetLeft"),
            (Intrinsic::NestedSetRight, "span:nestedSetRight"),
            (Intrinsic::NestedSetParent, "span:nestedSetParent"),
            (Intrinsic::ChildCount, "span:childCount"),
            (Intrinsic::InstrumentationName, "instrumentation:name"),
            (Intrinsic::InstrumentationVersion, "instrumentation:version"),
            (Intrinsic::EventName, "event:name"),
            (Intrinsic::EventTimeSinceStart, "event:timeSinceStart"),
            (Intrinsic::LinkTraceId, "link:traceID"),
            (Intrinsic::LinkSpanId, "link:spanID"),
        ];
        for (intrinsic, expected) in cases {
            let key = matcher_key(&intrinsic_field(intrinsic.clone()));
            assert!(key == expected, "{intrinsic:?} -> {key}");
        }
        // Non-intrinsic scopes keep the raw attribute key.
        assert!(matcher_key(&attr_field(Scope::Span, "http.method")) == "http.method");
    }

    // ---- ident / string_lit / anchored escaping ----

    #[test]
    fn ident_escapes_embedded_quotes() {
        assert!(ident("a\"b") == "\"a\"\"b\"");
    }

    #[test]
    fn string_lit_escapes_single_quotes() {
        assert!(string_lit("a'b") == "'a''b'");
    }

    #[test]
    fn anchored_wraps_pattern() {
        assert!(anchored("ab") == "^(?:ab)$");
    }

    // ---- has_nested_scope / has_parent_scope across combinators ----

    #[test]
    fn has_nested_scope_detects_event_link_and_intrinsics() {
        for (query, expected) in [
            ("{ event.foo = 1 }", true),
            ("{ link.foo = 1 }", true),
            ("{ event:name = \"x\" }", true),
            ("{ link:traceID = \"x\" }", true),
            ("{ .a = 1 || event.b = 2 }", true),
            ("{ !(link.b = 2) }", true),
            ("{ .a = 1 && .b = 2 }", false),
        ] {
            let got = has_nested_scope(&selector(query));
            assert!(got == expected, "{query} -> {got}");
        }
    }

    #[test]
    fn has_parent_scope_detects_parent_across_combinators() {
        for (query, expected) in [
            ("{ parent.a = 1 }", true),
            ("{ .a = 1 && parent.b = 2 }", true),
            ("{ !(parent.b = 2) }", true),
            ("{ .a = 1 }", false),
        ] {
            let got = has_parent_scope(&selector(query));
            assert!(got == expected, "{query} -> {got}");
        }
    }

    #[test]
    fn unfiltered_parent_table_is_needed_only_for_nested_parent_selectors() {
        for (query, expected) in [
            ("{ event:name = \"x\" }", false),
            ("{ parent.a = 1 }", false),
            ("{ event:name = \"x\" && parent.a = 1 }", true),
        ] {
            let got = needs_unfiltered_parent_table(&selector(query));
            assert!(got == expected, "{query} -> {got}");
        }
    }

    #[test]
    fn negate_matcher_toggles_flag() {
        let m = SpanMatcher {
            scope: MatchScope::Span,
            key: "a".into(),
            op: MatchCmp::Eq,
            value: MatchValue::Int(1),
            negated: false,
        };
        let n = negate_matcher(m.clone());
        assert!(n.negated);
        let back = negate_matcher(n);
        assert!(!back.negated);
    }
}

mod anchored;
mod collect_table;
mod comparison_to_sql;
mod comparison_to_sql_qualified;
mod comparison_value_sql;
mod enum_value_sql;
mod field_expr_to_matcher_disjuncts;
mod field_expr_to_matchers;
mod field_expr_to_negated_matcher_disjuncts;
mod field_expr_to_sql;
mod field_expr_to_sql_qualified;
mod field_to_column;
mod fixed_hex_lit;
mod has_nested_scope;
mod has_parent_scope;
mod ident;
mod intrinsic_match_key;
mod intrinsic_name;
mod kind_enum_value;
mod match_cmp;
mod match_scope;
mod match_value;
mod matcher_from_field_expr;
mod matcher_key;
mod needs_unfiltered_parent_table;
mod negate_matcher;
mod parent_field_expr_to_sql_qualified;
mod plan_selector;
mod plan_selector_disjuncts;
mod qualified_field_ident;
mod register_unfiltered_parent_table;
mod selector_sql;
mod selector_sql_with_parent_table;
mod status_enum_value;
mod string_lit;
mod value_sql;

use anchored::anchored;
use collect_table::collect_table;
pub(crate) use comparison_to_sql::comparison_to_sql;
use comparison_to_sql_qualified::comparison_to_sql_qualified;
use comparison_value_sql::comparison_value_sql;
use enum_value_sql::enum_value_sql;
use field_expr_to_matcher_disjuncts::field_expr_to_matcher_disjuncts;
pub(crate) use field_expr_to_matchers::field_expr_to_matchers;
use field_expr_to_negated_matcher_disjuncts::field_expr_to_negated_matcher_disjuncts;
pub(crate) use field_expr_to_sql::field_expr_to_sql;
use field_expr_to_sql_qualified::field_expr_to_sql_qualified;
pub(crate) use field_to_column::field_to_column;
use fixed_hex_lit::fixed_hex_lit;
pub(crate) use has_nested_scope::has_nested_scope;
pub(crate) use has_parent_scope::has_parent_scope;
pub(crate) use ident::ident;
use intrinsic_match_key::intrinsic_match_key;
use intrinsic_name::intrinsic_name;
use kind_enum_value::kind_enum_value;
use match_cmp::match_cmp;
use match_scope::match_scope;
use match_value::match_value;
use matcher_from_field_expr::matcher_from_field_expr;
use matcher_key::matcher_key;
use needs_unfiltered_parent_table::needs_unfiltered_parent_table;
use negate_matcher::negate_matcher;
use parent_field_expr_to_sql_qualified::parent_field_expr_to_sql_qualified;
pub(crate) use plan_selector::plan_selector;
use plan_selector_disjuncts::plan_selector_disjuncts;
use qualified_field_ident::qualified_field_ident;
use register_unfiltered_parent_table::register_unfiltered_parent_table;
pub(crate) use selector_sql::selector_sql;
pub(crate) use selector_sql_with_parent_table::selector_sql_with_parent_table;
use status_enum_value::status_enum_value;
use string_lit::string_lit;
use value_sql::value_sql;
