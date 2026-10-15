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

// Comparison To Sql Qualified
/// A `field <op> operand` comparison inside a span-to-parent join, whose
/// span and parent sides are the tables aliased `span_alias` and
/// `parent_alias`.
pub(crate) struct QualifiedComparison<'a> {
    pub(crate) field: &'a Field,
    pub(crate) op: ComparisonOp,
    pub(crate) operand: &'a Value,
    pub(crate) span_alias: &'a str,
    pub(crate) parent_alias: &'a str,
}

pub(crate) fn comparison_to_sql_qualified(comparison: &QualifiedComparison<'_>) -> Result<String> {
    let col = qualified_field_ident(
        comparison.field,
        comparison.span_alias,
        comparison.parent_alias,
    );
    column_comparison_sql(ColumnComparison {
        col: &col,
        field: comparison.field,
        op: comparison.op,
        operand: comparison.operand,
    })
}

// Field Expr To Matchers
pub(crate) fn field_expr_to_matchers(fe: &FieldExpr) -> Vec<SpanMatcher> {
    match fe {
        FieldExpr::And(a, b) => {
            let mut out = field_expr_to_matchers(a);
            out.extend(field_expr_to_matchers(b));
            out
        }
        FieldExpr::Comparison { .. } => matcher_from_field_expr(fe).into_iter().collect(),
        FieldExpr::Not(inner) if has_nested_scope(inner) => {
            field_expr_to_negated_matcher_disjuncts(inner)
                .filter(|disjuncts| disjuncts.len() == 1)
                .and_then(|mut disjuncts| disjuncts.pop())
                .unwrap_or_default()
        }
        // A constant filter carries no per-span matcher; the SQL predicate
        // (`TRUE`/`FALSE`) is authoritative, so it contributes no pre-filter.
        FieldExpr::ExpressionComparison { .. }
        | FieldExpr::FieldComparison { .. }
        | FieldExpr::Or(_, _)
        | FieldExpr::Not(_)
        | FieldExpr::Field(_)
        | FieldExpr::Const(_) => {
            vec![]
        }
    }
}

// Field Expr To Matcher Disjuncts
pub(crate) fn field_expr_to_matcher_disjuncts(fe: &FieldExpr) -> Option<Vec<Vec<SpanMatcher>>> {
    build_disjuncts(fe, false)
}

pub(super) fn build_disjuncts(fe: &FieldExpr, negated: bool) -> Option<Vec<Vec<SpanMatcher>>> {
    match fe {
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) => {
            let matcher = matcher_from_field_expr(fe);
            if negated {
                matcher.map(|matcher| vec![vec![negate_matcher(matcher)]])
            } else {
                Some(vec![vec![matcher.expect(
                    "comparison and field expressions lower to matchers",
                )]])
            }
        }
        // A constant filter is the identity/annihilator of the matcher DNF, not
        // "unrepresentable": `true` is match-all (one disjunct with no matchers,
        // the AND-identity) and `false` is match-none (zero disjuncts). Returning
        // `None` here would poison an enclosing `And` via `?`, dropping the
        // sibling's matchers and the attribute columns they project (e.g.
        // `{ span.http.method != nil && true }` would lose the `attr.http.method`
        // projection and fail to plan).
        FieldExpr::Const(value) => Some(if *value ^ negated {
            vec![vec![]]
        } else {
            vec![]
        }),
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            let left = build_disjuncts(a, negated)?;
            let right = build_disjuncts(b, negated)?;
            let is_and = matches!(fe, FieldExpr::And(..));
            if is_and ^ negated {
                Some(
                    left.iter()
                        .flat_map(|l| {
                            right.iter().map(move |r| {
                                let mut out = l.clone();
                                out.extend(r.clone());
                                out
                            })
                        })
                        .collect(),
                )
            } else {
                let mut out = left;
                out.extend(right);
                Some(out)
            }
        }
        FieldExpr::Not(inner) if has_nested_scope(inner) => {
            if negated {
                Some(build_disjuncts(inner, false)?)
            } else {
                Some(build_disjuncts(inner, true)?)
            }
        }
        FieldExpr::Not(inner) if negated => Some(build_disjuncts(inner, false)?),
        FieldExpr::ExpressionComparison { .. }
        | FieldExpr::FieldComparison { .. }
        | FieldExpr::Not(_) => None,
    }
}

// Field To Column
pub(crate) fn field_to_column(field: &Field) -> String {
    let col = match &field.scope {
        Scope::Intrinsic(i) => match i {
            Intrinsic::Name => COL_NAME,
            Intrinsic::Duration => COL_DURATION,
            Intrinsic::Kind => COL_KIND,
            Intrinsic::Status => COL_STATUS_CODE,
            Intrinsic::StatusMessage => COL_STATUS_MESSAGE,
            Intrinsic::Id => COL_SPAN_ID,
            Intrinsic::ParentId => COL_PARENT_SPAN_ID,
            Intrinsic::TraceDuration => COL_TRACE_DURATION,
            Intrinsic::TraceRootName => COL_ROOT_SPAN_NAME,
            Intrinsic::TraceRootService => COL_ROOT_SERVICE_NAME,
            Intrinsic::TraceId => COL_TRACE_ID,
            Intrinsic::NestedSetLeft => COL_NS_LEFT,
            Intrinsic::NestedSetRight => COL_NS_RIGHT,
            Intrinsic::NestedSetParent => COL_PARENT_ID,
            Intrinsic::ChildCount => COL_CHILD_COUNT,
            Intrinsic::InstrumentationName => COL_INSTRUMENTATION_NAME,
            Intrinsic::InstrumentationVersion => COL_INSTRUMENTATION_VERSION,
            Intrinsic::EventName => COL_EVENT_NAME,
            Intrinsic::EventTimeSinceStart => COL_EVENT_TIME_SINCE_START,
            Intrinsic::LinkTraceId => COL_LINK_TRACE_ID,
            Intrinsic::LinkSpanId => COL_LINK_SPAN_ID,
        },
        Scope::Both | Scope::Resource if field.key == "service.name" => COL_ROOT_SERVICE_NAME,
        Scope::Instrumentation => {
            return format!("{ATTR_PREFIX}{INSTRUMENTATION_ATTR_PREFIX}{}", field.key);
        }
        Scope::Both
        | Scope::Span
        | Scope::Resource
        | Scope::Parent
        | Scope::Event
        | Scope::Link => {
            return format!("{ATTR_PREFIX}{}", field.key);
        }
    };
    col.to_string()
}

// Matcher From Field Expr
pub(crate) fn matcher_from_field_expr(fe: &FieldExpr) -> Option<SpanMatcher> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } => Some(SpanMatcher {
            scope: match_scope(&lhs.scope),
            key: matcher_key(lhs),
            op: match_cmp(*op),
            value: match_value(rhs),
            negated: false,
        }),
        FieldExpr::Field(field) => Some(SpanMatcher {
            scope: match_scope(&field.scope),
            key: matcher_key(field),
            op: if matches!(field.scope, Scope::Parent) {
                MatchCmp::Neq
            } else {
                MatchCmp::Eq
            },
            value: if matches!(field.scope, Scope::Parent) {
                MatchValue::Nil
            } else {
                MatchValue::Bool(true)
            },
            negated: false,
        }),
        FieldExpr::ExpressionComparison { .. }
        | FieldExpr::FieldComparison { .. }
        | FieldExpr::And(_, _)
        | FieldExpr::Or(_, _)
        | FieldExpr::Not(_)
        | FieldExpr::Const(_) => None,
    }
}


// Plan Selector
pub(crate) async fn plan_selector<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    fe: &FieldExpr,
) -> Result<PlannedSpanset> {
    if has_nested_scope(fe)
        && !fe.has_field_comparison()
        && let Some(disjuncts) = field_expr_to_matcher_disjuncts(fe)
        && disjuncts.len() > 1
    {
        return plan_selector_disjuncts(store, ctx, &disjuncts).await;
    }

    let matchers = field_expr_to_matchers(fe);
    let scan = store
        .scan_with_options(
            &ctx.tenant,
            &matchers,
            ctx.start_ns.into(),
            ctx.end_ns.into(),
            &ctx.scan_options,
        )
        .await?;
    super::registration::register_field_comparison_columns(&scan.ctx, &scan.span_table, &[fe]).await?;
    let inspected = scan.inspected;
    let parent_table = if needs_unfiltered_parent_table(fe) {
        register_unfiltered_parent_table(store, ctx, &scan.ctx).await?
    } else {
        scan.span_table.clone()
    };
    if !has_nested_scope(fe)
        && !fe.has_field_comparison()
        && !has_parent_scope(fe)
        && field_expr_to_matcher_disjuncts(fe).is_some_and(|disjuncts| disjuncts.len() == 1)
    {
        let plan = scan
            .ctx
            .table(&scan.span_table)
            .await?
            .into_unoptimized_plan();
        return Ok(PlannedSpanset {
            ctx: scan.ctx,
            plan,
            inspected,
            sampling_factor: 1.0,
            spanset_pipeline_had_input: false,
        });
    }
    let table = ident(&scan.span_table);
    let sql = selector_sql_with_parent_table(&table, &ident(&parent_table), fe)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
        inspected,
        sampling_factor: 1.0,
        spanset_pipeline_had_input: false,
    })
}

// Plan Selector Disjuncts
pub(crate) async fn plan_selector_disjuncts<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    disjuncts: &[Vec<SpanMatcher>],
) -> Result<PlannedSpanset> {
    let mut batches = Vec::new();
    let mut schema = None;
    let mut inspected = <ByteSize as ByteSizeExt>::ZERO;
    for matchers in disjuncts {
        let scan = store
            .scan_with_options(
                &ctx.tenant,
                matchers,
                ctx.start_ns.into(),
                ctx.end_ns.into(),
                &ctx.scan_options,
            )
            .await?;
        inspected += scan.inspected;
        let mut scan_batches = collect_table(&scan.ctx, &scan.span_table).await?;
        if schema.is_none() {
            schema = scan_batches.first().map(RecordBatch::schema);
        }
        batches.append(&mut scan_batches);
    }

    let schema = schema.unwrap_or_else(crate::span_columns::span_schema);
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("spans", Arc::new(table))?;
    let df = ctx.sql("SELECT DISTINCT * FROM spans").await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx,
        plan,
        inspected,
        sampling_factor: 1.0,
        spanset_pipeline_had_input: false,
    })
}

// Selector Sql With Parent Table
pub(crate) fn selector_sql_with_parent_table(
    table: &str,
    parent_table: &str,
    fe: &FieldExpr,
) -> Result<String> {
    if has_nested_scope(fe) {
        if has_parent_scope(fe)
            && let Some(predicate) = parent_field_expr_to_sql_qualified(fe, "s", "p")?
        {
            let trace = ident(COL_TRACE_ID);
            let parent = ident(COL_PARENT_ID);
            let left = ident(COL_NS_LEFT);
            return Ok(format!(
                "SELECT s.* FROM {table} AS s JOIN {parent_table} AS p \
                 ON s.{trace} = p.{trace} AND s.{parent} = p.{left} \
                 WHERE {predicate}"
            ));
        }
        if fe.has_field_comparison() {
            let predicate = field_expr_to_sql(fe)?;
            return Ok(format!("SELECT * FROM {table} WHERE {predicate}"));
        }
        return Ok(format!("SELECT * FROM {table}"));
    }
    if has_parent_scope(fe) {
        let predicate = field_expr_to_sql_qualified(fe, "s", "p")?;
        let trace = ident(COL_TRACE_ID);
        let parent = ident(COL_PARENT_ID);
        let left = ident(COL_NS_LEFT);
        Ok(format!(
            "SELECT s.* FROM {table} AS s JOIN {table} AS p \
             ON s.{trace} = p.{trace} AND s.{parent} = p.{left} \
             WHERE {predicate}"
        ))
    } else {
        let predicate = field_expr_to_sql(fe)?;
        Ok(format!("SELECT * FROM {table} WHERE {predicate}"))
    }
}


#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::{SpansetExpr, parser::parse};

    struct IntrinsicColumnCase {
        intrinsic: Intrinsic,
        column: &'static str,
    }

    struct ComparisonSqlCase {
        op: ComparisonOp,
        expected: String,
    }

    struct QuerySqlCase {
        query: &'static str,
        expected: &'static str,
    }

    struct EnumCodeCase {
        name: &'static str,
        code: i32,
    }

    struct ValueSqlCase {
        value: Value,
        expected: &'static str,
    }

    struct ScopeNameCase {
        scope: Scope,
        expected: &'static str,
    }

    struct QualifiedComparisonCase {
        op: ComparisonOp,
        value: Value,
        expected: String,
    }

    struct ScopeMatchCase {
        scope: Scope,
        expected: MatchScope,
    }

    struct ComparisonMatchCase {
        op: ComparisonOp,
        expected: MatchCmp,
    }

    struct ValueMatchCase {
        value: Value,
        expected: MatchValue,
    }

    struct IntrinsicKeyCase {
        intrinsic: Intrinsic,
        expected: &'static str,
    }

    struct QueryBoolCase {
        query: &'static str,
        expected: bool,
    }

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
            IntrinsicColumnCase {
                intrinsic: Intrinsic::Name,
                column: COL_NAME,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::Duration,
                column: COL_DURATION,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::Kind,
                column: COL_KIND,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::Status,
                column: COL_STATUS_CODE,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::StatusMessage,
                column: COL_STATUS_MESSAGE,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::Id,
                column: COL_SPAN_ID,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::ParentId,
                column: COL_PARENT_SPAN_ID,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::TraceDuration,
                column: COL_TRACE_DURATION,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::TraceRootName,
                column: COL_ROOT_SPAN_NAME,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::TraceRootService,
                column: COL_ROOT_SERVICE_NAME,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::TraceId,
                column: COL_TRACE_ID,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::NestedSetLeft,
                column: COL_NS_LEFT,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::NestedSetRight,
                column: COL_NS_RIGHT,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::NestedSetParent,
                column: COL_PARENT_ID,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::ChildCount,
                column: COL_CHILD_COUNT,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::InstrumentationName,
                column: COL_INSTRUMENTATION_NAME,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::InstrumentationVersion,
                column: COL_INSTRUMENTATION_VERSION,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::EventName,
                column: COL_EVENT_NAME,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::EventTimeSinceStart,
                column: COL_EVENT_TIME_SINCE_START,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::LinkTraceId,
                column: COL_LINK_TRACE_ID,
            },
            IntrinsicColumnCase {
                intrinsic: Intrinsic::LinkSpanId,
                column: COL_LINK_SPAN_ID,
            },
        ];
        for case in cases {
            let col = field_to_column(&intrinsic_field(case.intrinsic.clone()));
            assert!(
                col == case.column,
                "intrinsic {:?} -> {col}",
                case.intrinsic
            );
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
            ComparisonSqlCase {
                op: ComparisonOp::Eq,
                expected: format!("{col} = 1"),
            },
            ComparisonSqlCase {
                op: ComparisonOp::Neq,
                expected: format!("{col} != 1"),
            },
            ComparisonSqlCase {
                op: ComparisonOp::Lt,
                expected: format!("{col} < 1"),
            },
            ComparisonSqlCase {
                op: ComparisonOp::Lte,
                expected: format!("{col} <= 1"),
            },
            ComparisonSqlCase {
                op: ComparisonOp::Gt,
                expected: format!("{col} > 1"),
            },
            ComparisonSqlCase {
                op: ComparisonOp::Gte,
                expected: format!("{col} >= 1"),
            },
        ];
        for case in cases {
            let op = case.op;
            let sql = comparison_to_sql(&field, op, &Value::Int(1)).unwrap();
            assert!(sql == case.expected, "{op:?} -> {sql}");
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
        for case in [
            QuerySqlCase {
                query: "{ .a = 1 && .b = 2 }",
                expected: "(\"attr.a\" = 1 AND \"attr.b\" = 2)",
            },
            QuerySqlCase {
                query: "{ .a = 1 || .b = 2 }",
                expected: "(\"attr.a\" = 1 OR \"attr.b\" = 2)",
            },
            QuerySqlCase {
                query: "{ !(.a = 1) }",
                expected: "(NOT \"attr.a\" = 1)",
            },
        ] {
            let sql = field_expr_to_sql(&selector(case.query)).unwrap();
            assert!(sql == case.expected, "{} -> {sql}", case.query);
        }
    }

    #[test]
    fn field_expr_to_sql_bare_field_requires_typed_boolean_evaluation() {
        let sql = field_expr_to_sql(&selector("{ .a }")).unwrap();
        assert!(sql.contains("__traceql_field_comparison_Field"));
        assert!(!sql.contains("IS NOT NULL"));
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
        assert!(matches!(
            parse("{ parent.a }"),
            Err(TraceqlError::Unsupported(_))
        ));
        let fe = FieldExpr::Field(attr_field(Scope::Parent, "a"));
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
        for case in [
            QuerySqlCase {
                query: "{ parent.a = 1 && .b = 2 }",
                expected: "(p.\"attr.a\" = 1 AND s.\"attr.b\" = 2)",
            },
            QuerySqlCase {
                query: "{ parent.a = 1 || .b = 2 }",
                expected: "(p.\"attr.a\" = 1 OR s.\"attr.b\" = 2)",
            },
            QuerySqlCase {
                query: "{ !(parent.a = 1) }",
                expected: "(NOT p.\"attr.a\" = 1)",
            },
        ] {
            let sql = field_expr_to_sql_qualified(&selector(case.query), "s", "p").unwrap();
            assert!(sql == case.expected, "{} -> {sql}", case.query);
        }
    }

    // ---- comparison_value_sql: enums, hex widths, errors ----

    #[test]
    fn comparison_value_sql_maps_status_enum() {
        let status = intrinsic_field(Intrinsic::Status);
        for case in [
            EnumCodeCase {
                name: "unset",
                code: 0,
            },
            EnumCodeCase {
                name: "ok",
                code: 1,
            },
            EnumCodeCase {
                name: "error",
                code: 2,
            },
            EnumCodeCase {
                name: "ERROR",
                code: 2,
            },
        ] {
            let sql = comparison_value_sql(&status, &Value::Str(case.name.into())).unwrap();
            assert!(
                sql == case.code.to_string(),
                "status {} -> {sql}",
                case.name
            );
        }
        let err = comparison_value_sql(&status, &Value::Str("bogus".into()));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_maps_kind_enum() {
        let kind = intrinsic_field(Intrinsic::Kind);
        for case in [
            EnumCodeCase {
                name: "unspecified",
                code: 0,
            },
            EnumCodeCase {
                name: "internal",
                code: 1,
            },
            EnumCodeCase {
                name: "server",
                code: 2,
            },
            EnumCodeCase {
                name: "client",
                code: 3,
            },
            EnumCodeCase {
                name: "producer",
                code: 4,
            },
            EnumCodeCase {
                name: "consumer",
                code: 5,
            },
            EnumCodeCase {
                name: "Server",
                code: 2,
            },
        ] {
            let sql = comparison_value_sql(&kind, &Value::Str(case.name.into())).unwrap();
            assert!(sql == case.code.to_string(), "kind {} -> {sql}", case.name);
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
        for case in [
            ValueSqlCase {
                value: Value::Int(7),
                expected: "7",
            },
            ValueSqlCase {
                value: Value::Str("hi".into()),
                expected: "'hi'",
            },
            ValueSqlCase {
                value: Value::Bool(true),
                expected: "true",
            },
            ValueSqlCase {
                value: Value::Duration(5),
                expected: "5",
            },
        ] {
            let sql = comparison_value_sql(&field, &case.value).unwrap();
            assert!(sql == case.expected, "{:?} -> {sql}", case.value);
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
            ScopeNameCase {
                scope: Scope::Intrinsic(Intrinsic::TraceId),
                expected: "trace:id",
            },
            ScopeNameCase {
                scope: Scope::Intrinsic(Intrinsic::Id),
                expected: "span:id",
            },
            ScopeNameCase {
                scope: Scope::Intrinsic(Intrinsic::ParentId),
                expected: "span:parentID",
            },
            ScopeNameCase {
                scope: Scope::Intrinsic(Intrinsic::Kind),
                expected: "span:kind",
            },
            ScopeNameCase {
                scope: Scope::Intrinsic(Intrinsic::Status),
                expected: "span:status",
            },
            // Anything else collapses to the generic label.
            ScopeNameCase {
                scope: Scope::Both,
                expected: "intrinsic",
            },
            ScopeNameCase {
                scope: Scope::Intrinsic(Intrinsic::Name),
                expected: "intrinsic",
            },
        ];
        for case in cases {
            let name = intrinsic_name(&case.scope);
            assert!(name == case.expected, "{:?} -> {name}", case.scope);
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
            QualifiedComparisonCase {
                op: ComparisonOp::Eq,
                value: Value::Int(1),
                expected: format!("{col} = 1"),
            },
            QualifiedComparisonCase {
                op: ComparisonOp::Neq,
                value: Value::Int(1),
                expected: format!("{col} != 1"),
            },
            QualifiedComparisonCase {
                op: ComparisonOp::Lt,
                value: Value::Int(1),
                expected: format!("{col} < 1"),
            },
            QualifiedComparisonCase {
                op: ComparisonOp::Lte,
                value: Value::Int(1),
                expected: format!("{col} <= 1"),
            },
            QualifiedComparisonCase {
                op: ComparisonOp::Gt,
                value: Value::Int(1),
                expected: format!("{col} > 1"),
            },
            QualifiedComparisonCase {
                op: ComparisonOp::Gte,
                value: Value::Int(1),
                expected: format!("{col} >= 1"),
            },
            // nil
            QualifiedComparisonCase {
                op: ComparisonOp::Eq,
                value: Value::Nil,
                expected: format!("{col} IS NULL"),
            },
            QualifiedComparisonCase {
                op: ComparisonOp::Neq,
                value: Value::Nil,
                expected: format!("{col} IS NOT NULL"),
            },
            // regex
            QualifiedComparisonCase {
                op: ComparisonOp::Re,
                value: Value::Str("x".into()),
                expected: format!("regexp_like({col}, '^(?:x)$')"),
            },
            QualifiedComparisonCase {
                op: ComparisonOp::Nre,
                value: Value::Str("x".into()),
                expected: format!("NOT regexp_like({col}, '^(?:x)$')"),
            },
        ];
        for case in cases {
            let sql = comparison_to_sql_qualified(&QualifiedComparison {
                field: &field,
                op: case.op,
                operand: &case.value,
                span_alias: "s",
                parent_alias: "p",
            })
            .unwrap();
            assert!(
                sql == case.expected,
                "{:?} {:?} -> {sql}",
                case.op,
                case.value
            );
        }
        // regex against non-string errors
        let err = comparison_to_sql_qualified(&QualifiedComparison {
            field: &field,
            op: ComparisonOp::Re,
            operand: &Value::Int(1),
            span_alias: "s",
            parent_alias: "p",
        });
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
    fn matcher_from_bare_field_requires_boolean_true() {
        let m = matcher_from_field_expr(&selector("{ resource.region }")).unwrap();
        assert!(
            m == SpanMatcher {
                scope: MatchScope::Resource,
                key: "region".into(),
                op: MatchCmp::Eq,
                value: MatchValue::Bool(true),
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
            ScopeMatchCase {
                scope: Scope::Both,
                expected: MatchScope::Both,
            },
            ScopeMatchCase {
                scope: Scope::Span,
                expected: MatchScope::Span,
            },
            ScopeMatchCase {
                scope: Scope::Resource,
                expected: MatchScope::Resource,
            },
            ScopeMatchCase {
                scope: Scope::Parent,
                expected: MatchScope::Parent,
            },
            ScopeMatchCase {
                scope: Scope::Event,
                expected: MatchScope::Event,
            },
            ScopeMatchCase {
                scope: Scope::Link,
                expected: MatchScope::Link,
            },
            ScopeMatchCase {
                scope: Scope::Instrumentation,
                expected: MatchScope::Instrumentation,
            },
            ScopeMatchCase {
                scope: Scope::Intrinsic(Intrinsic::Name),
                expected: MatchScope::Intrinsic,
            },
        ];
        for case in cases {
            let got = match_scope(&case.scope);
            assert!(got == case.expected, "{:?} -> {got:?}", case.scope);
        }
    }

    #[test]
    fn match_cmp_covers_every_operator() {
        let cases = [
            ComparisonMatchCase {
                op: ComparisonOp::Eq,
                expected: MatchCmp::Eq,
            },
            ComparisonMatchCase {
                op: ComparisonOp::Neq,
                expected: MatchCmp::Neq,
            },
            ComparisonMatchCase {
                op: ComparisonOp::Lt,
                expected: MatchCmp::Lt,
            },
            ComparisonMatchCase {
                op: ComparisonOp::Lte,
                expected: MatchCmp::Lte,
            },
            ComparisonMatchCase {
                op: ComparisonOp::Gt,
                expected: MatchCmp::Gt,
            },
            ComparisonMatchCase {
                op: ComparisonOp::Gte,
                expected: MatchCmp::Gte,
            },
            ComparisonMatchCase {
                op: ComparisonOp::Re,
                expected: MatchCmp::Re,
            },
            ComparisonMatchCase {
                op: ComparisonOp::Nre,
                expected: MatchCmp::Nre,
            },
        ];
        for case in cases {
            let got = match_cmp(case.op);
            assert!(got == case.expected, "{:?} -> {got:?}", case.op);
        }
    }

    #[test]
    fn match_value_covers_every_value_kind() {
        let cases = [
            ValueMatchCase {
                value: Value::Str("x".into()),
                expected: MatchValue::Str("x".into()),
            },
            ValueMatchCase {
                value: Value::Int(3),
                expected: MatchValue::Int(3),
            },
            ValueMatchCase {
                value: Value::Duration(9),
                expected: MatchValue::Int(9),
            },
            ValueMatchCase {
                value: Value::Float(1.5),
                expected: MatchValue::Float(1.5),
            },
            ValueMatchCase {
                value: Value::Bool(true),
                expected: MatchValue::Bool(true),
            },
            ValueMatchCase {
                value: Value::Nil,
                expected: MatchValue::Nil,
            },
        ];
        for case in cases {
            let got = match_value(&case.value);
            assert!(got == case.expected, "{:?} -> {got:?}", case.value);
        }
    }

    #[test]
    fn matcher_key_uses_intrinsic_canonical_names() {
        // `Intrinsic::tag_name` pins every variant's name; the key delegates to it.
        let cases = [
            IntrinsicKeyCase {
                intrinsic: Intrinsic::Name,
                expected: "span:name",
            },
            IntrinsicKeyCase {
                intrinsic: Intrinsic::ParentId,
                expected: "span:parentID",
            },
            IntrinsicKeyCase {
                intrinsic: Intrinsic::TraceRootService,
                expected: "trace:rootService",
            },
            IntrinsicKeyCase {
                intrinsic: Intrinsic::NestedSetParent,
                expected: "span:nestedSetParent",
            },
            IntrinsicKeyCase {
                intrinsic: Intrinsic::LinkSpanId,
                expected: "link:spanID",
            },
        ];
        for case in cases {
            let key = matcher_key(&intrinsic_field(case.intrinsic.clone()));
            assert!(key == case.expected, "{:?} -> {key}", case.intrinsic);
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
        for case in [
            QueryBoolCase {
                query: "{ event.foo = 1 }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ link.foo = 1 }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ event:name = \"x\" }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ link:traceID = \"x\" }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ .a = 1 || event.b = 2 }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ !(link.b = 2) }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ .a = 1 && .b = 2 }",
                expected: false,
            },
        ] {
            let got = has_nested_scope(&selector(case.query));
            assert!(got == case.expected, "{} -> {got}", case.query);
        }
    }

    #[test]
    fn has_parent_scope_detects_parent_across_combinators() {
        for case in [
            QueryBoolCase {
                query: "{ parent.a = 1 }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ .a = 1 && parent.b = 2 }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ !(parent.b = 2) }",
                expected: true,
            },
            QueryBoolCase {
                query: "{ .a = 1 }",
                expected: false,
            },
        ] {
            let got = has_parent_scope(&selector(case.query));
            assert!(got == case.expected, "{} -> {got}", case.query);
        }
    }

    #[test]
    fn unfiltered_parent_table_is_needed_only_for_nested_parent_selectors() {
        for case in [
            QueryBoolCase {
                query: "{ event:name = \"x\" }",
                expected: false,
            },
            QueryBoolCase {
                query: "{ parent.a = 1 }",
                expected: false,
            },
            QueryBoolCase {
                query: "{ event:name = \"x\" && parent.a = 1 }",
                expected: true,
            },
        ] {
            let got = needs_unfiltered_parent_table(&selector(case.query));
            assert!(got == case.expected, "{} -> {got}", case.query);
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
mod sql;
use sql::{ColumnComparison, column_comparison_sql, field_expr_to_sql_qualified, parent_field_expr_to_sql_qualified};
pub(crate) use sql::field_expr_to_sql;

use crate::span_enum_codes::{kind_enum_value, status_enum_value};

pub(crate) fn anchored(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

pub(crate) async fn collect_table(ctx: &SessionContext, table: &str) -> Result<Vec<RecordBatch>> {
    Ok(ctx.table(table).await?.collect().await?)
}

pub(crate) fn field_expr_to_negated_matcher_disjuncts(
    fe: &FieldExpr,
) -> Option<Vec<Vec<SpanMatcher>>> {
    build_disjuncts(fe, true)
}

pub(crate) fn has_parent_scope(fe: &FieldExpr) -> bool {
    field_expr_has_field(fe, &|field| matches!(field.scope, Scope::Parent))
}

pub(crate) fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub(crate) fn intrinsic_name(scope: &Scope) -> &'static str {
    match scope {
        Scope::Intrinsic(Intrinsic::TraceId) => "trace:id",
        Scope::Intrinsic(Intrinsic::Id) => "span:id",
        Scope::Intrinsic(Intrinsic::ParentId) => "span:parentID",
        Scope::Intrinsic(Intrinsic::Kind) => "span:kind",
        Scope::Intrinsic(Intrinsic::Status) => "span:status",
        _ => "intrinsic",
    }
}

pub(crate) fn match_value(value: &Value) -> MatchValue {
    match value {
        Value::Array(_) => unreachable!("parser does not produce array literals"),
        Value::Str(v) => MatchValue::Str(v.clone()),
        Value::Int(v) | Value::Duration(v) => MatchValue::Int(*v),
        Value::Float(v) => MatchValue::Float(*v),
        Value::Bool(v) => MatchValue::Bool(*v),
        Value::Nil => MatchValue::Nil,
    }
}

pub(crate) fn matcher_key(field: &Field) -> String {
    match &field.scope {
        Scope::Intrinsic(intrinsic) => intrinsic.tag_name().to_string(),
        _ => field.key.clone(),
    }
}

pub(crate) fn needs_unfiltered_parent_table(fe: &FieldExpr) -> bool {
    has_nested_scope(fe) && has_parent_scope(fe)
}

pub(crate) fn negate_matcher(mut matcher: SpanMatcher) -> SpanMatcher {
    matcher.negated = !matcher.negated;
    matcher
}

pub(crate) fn qualified_field_ident(field: &Field, span_alias: &str, parent_alias: &str) -> String {
    let alias = if matches!(field.scope, Scope::Parent) {
        parent_alias
    } else {
        span_alias
    };
    format!("{alias}.{}", ident(&field_to_column(field)))
}

pub(crate) fn selector_sql(table: &str, fe: &FieldExpr) -> Result<String> {
    selector_sql_with_parent_table(table, table, fe)
}

pub(crate) fn string_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub(crate) fn comparison_to_sql(field: &Field, op: ComparisonOp, value: &Value) -> Result<String> {
    column_comparison_sql(ColumnComparison {
        col: &ident(&field_to_column(field)),
        field,
        op,
        operand: value,
    })
}

pub(crate) fn comparison_value_sql(field: &Field, value: &Value) -> Result<String> {
    if matches!(
        field.scope,
        Scope::Intrinsic(Intrinsic::Kind | Intrinsic::Status)
    ) && let Value::Str(name) = value
    {
        return enum_value_sql(&field.scope, name);
    }
    let width = match field.scope {
        Scope::Intrinsic(Intrinsic::TraceId | Intrinsic::LinkTraceId) => Some(16),
        Scope::Intrinsic(Intrinsic::Id | Intrinsic::ParentId | Intrinsic::LinkSpanId) => Some(8),
        _ => None,
    };
    if let Some(width) = width {
        let Value::Str(hex) = value else {
            return Err(TraceqlError::Plan(format!(
                "{} comparisons require a hex string value",
                intrinsic_name(&field.scope)
            )));
        };
        return fixed_hex_lit(hex, width);
    }
    value_sql(value)
}

pub(crate) fn enum_value_sql(scope: &Scope, name: &str) -> Result<String> {
    let normalized = name.to_ascii_lowercase();
    let value = match scope {
        Scope::Intrinsic(Intrinsic::Status) => status_enum_value(&normalized),
        Scope::Intrinsic(Intrinsic::Kind) => kind_enum_value(&normalized),
        _ => {
            return Err(TraceqlError::Plan(format!(
                "unknown {} enum value {name:?}",
                intrinsic_name(scope)
            )));
        }
    };
    value.map(|v| v.to_string()).ok_or_else(|| {
        TraceqlError::Plan(format!(
            "unknown {} enum value {name:?}",
            intrinsic_name(scope)
        ))
    })
}

pub(super) fn field_expr_has_field(fe: &FieldExpr, predicate: &impl Fn(&Field) -> bool) -> bool {
    match fe {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            let mut fields = Vec::new();
            lhs.collect_fields(&mut fields);
            rhs.collect_fields(&mut fields);
            fields.into_iter().any(predicate)
        }
        FieldExpr::FieldComparison { lhs, rhs, .. } => predicate(lhs) || predicate(rhs),
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => predicate(lhs),
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            field_expr_has_field(a, predicate) || field_expr_has_field(b, predicate)
        }
        FieldExpr::Not(inner) => field_expr_has_field(inner, predicate),
        FieldExpr::Const(_) => false,
    }
}

pub(crate) fn fixed_hex_lit(hex: &str, width: usize) -> Result<String> {
    let expected_len = width * 2;
    if hex.len() != expected_len {
        return Err(TraceqlError::Plan(format!(
            "expected {expected_len} hex characters, got {}",
            hex.len()
        )));
    }
    if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(TraceqlError::Plan(
            "hex string contains non-hex characters".into(),
        ));
    }
    Ok(format!("X'{}'", hex.to_ascii_lowercase()))
}

pub(crate) fn has_nested_scope(fe: &FieldExpr) -> bool {
    field_expr_has_field(fe, &|field| {
        matches!(field.scope, Scope::Event | Scope::Link)
            || matches!(
                field.scope,
                Scope::Intrinsic(
                    Intrinsic::EventName
                        | Intrinsic::EventTimeSinceStart
                        | Intrinsic::LinkTraceId
                        | Intrinsic::LinkSpanId
                )
            )
    })
}

pub(crate) fn match_cmp(op: ComparisonOp) -> MatchCmp {
    match op {
        ComparisonOp::Eq => MatchCmp::Eq,
        ComparisonOp::Neq => MatchCmp::Neq,
        ComparisonOp::Lt => MatchCmp::Lt,
        ComparisonOp::Lte => MatchCmp::Lte,
        ComparisonOp::Gt => MatchCmp::Gt,
        ComparisonOp::Gte => MatchCmp::Gte,
        ComparisonOp::Re => MatchCmp::Re,
        ComparisonOp::Nre => MatchCmp::Nre,
    }
}

pub(crate) fn match_scope(scope: &Scope) -> MatchScope {
    match scope {
        Scope::Both => MatchScope::Both,
        Scope::Span => MatchScope::Span,
        Scope::Resource => MatchScope::Resource,
        Scope::Parent => MatchScope::Parent,
        Scope::Event => MatchScope::Event,
        Scope::Link => MatchScope::Link,
        Scope::Instrumentation => MatchScope::Instrumentation,
        Scope::Intrinsic(_) => MatchScope::Intrinsic,
    }
}

pub(crate) async fn register_unfiltered_parent_table<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    target_ctx: &SessionContext,
) -> Result<String> {
    let parent_scan = store
        .scan_with_options(
            &ctx.tenant,
            &[],
            ctx.start_ns.into(),
            ctx.end_ns.into(),
            &ctx.scan_options,
        )
        .await?;
    let batches = collect_table(&parent_scan.ctx, &parent_scan.span_table).await?;
    let schema = batches
        .first()
        .map_or_else(crate::span_columns::span_schema, RecordBatch::schema);
    let table = MemTable::try_new(schema, vec![batches])?;
    let table_name = "parent_spans";
    target_ctx.register_table(table_name, Arc::new(table))?;
    Ok(table_name.to_string())
}

pub(crate) fn value_sql(value: &Value) -> Result<String> {
    match value {
        Value::Array(_) => Err(TraceqlError::Unsupported("array literals".into())),
        Value::Str(v) => Ok(string_lit(v)),
        Value::Int(v) | Value::Duration(v) => Ok(v.to_string()),
        Value::Float(v) => {
            if !v.is_finite() {
                return Err(TraceqlError::Plan("comparison value is not finite".into()));
            }
            Ok(v.to_string())
        }
        Value::Bool(v) => Ok(v.to_string()),
        Value::Nil => Err(TraceqlError::Plan(
            "nil only supports equality comparisons".into(),
        )),
    }
}
