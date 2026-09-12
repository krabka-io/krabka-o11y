//! Property tests for the `TraceQL` lexer and parser.
//!
//! `lex_and_parse_never_panic` is the totality property that the code style
//! guide requires of anything that reads untrusted input: `/api/search` hands
//! the `q` parameter straight to `parse`, so every string here is one a caller
//! can send, and the only acceptable answers are a [`Query`] or a
//! `TraceqlError`.
//!
//! `rendering_a_query_round_trips` is the round-trip property. The crate has
//! no formatter of its own, so this file writes one: `render_query` turns a
//! generated tree back into `TraceQL` text, and the parser has to recover the
//! tree it started from. The renderer is deliberately independent of the
//! parser, which is what makes the property worth running. A shared helper
//! would only prove that the two agree with themselves. `tests/golden_queries.rs`
//! pins the concrete syntax of individual constructs; this pins that the
//! grammar composes.
//!
//! The generator produces only trees the parser can reach, because that is
//! the set the property is about. Where a construct has more than one
//! spelling, the renderer emits the canonical one: an intrinsic always carries
//! its scope, a string is always quoted, and a `by` clause is always its own
//! pipeline stage.

use std::fmt::Write as _;

use assert2::assert;
use krabka_traceql::{
    Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, Scope, SpansetExpr,
    StructuralOp, Value, parse,
};
use proptest::prelude::*;

/// Every intrinsic, with the scope and key that name it. The parser stores the
/// key beside the scope, so a rendered intrinsic has to use the same key that
/// the generated [`Field`] carries.
const INTRINSICS: &[(&str, &str, Intrinsic)] = &[
    ("span", "name", Intrinsic::Name),
    ("span", "duration", Intrinsic::Duration),
    ("span", "kind", Intrinsic::Kind),
    ("span", "status", Intrinsic::Status),
    ("span", "statusMessage", Intrinsic::StatusMessage),
    ("span", "id", Intrinsic::Id),
    ("span", "parentID", Intrinsic::ParentId),
    ("span", "childCount", Intrinsic::ChildCount),
    ("span", "nestedSetLeft", Intrinsic::NestedSetLeft),
    ("span", "nestedSetRight", Intrinsic::NestedSetRight),
    ("span", "nestedSetParent", Intrinsic::NestedSetParent),
    ("trace", "duration", Intrinsic::TraceDuration),
    ("trace", "rootName", Intrinsic::TraceRootName),
    ("trace", "rootService", Intrinsic::TraceRootService),
    ("trace", "id", Intrinsic::TraceId),
    ("event", "name", Intrinsic::EventName),
    ("event", "timeSinceStart", Intrinsic::EventTimeSinceStart),
    ("link", "traceID", Intrinsic::LinkTraceId),
    ("link", "spanID", Intrinsic::LinkSpanId),
    ("instrumentation", "name", Intrinsic::InstrumentationName),
    (
        "instrumentation",
        "version",
        Intrinsic::InstrumentationVersion,
    ),
];

/// Attribute keys. The lexer takes `-` and `.` inside an identifier, so the
/// table covers both.
const ATTRIBUTE_KEYS: &[&str] = &["svc", "http.method", "service.name", "x-y", "_a", "ok"];

/// Valid queries, used as the seeds the mutation strategy edits.
const SEED_QUERIES: &[&str] = &[
    "{}",
    "{ true }",
    r#"{ .svc = "a" }"#,
    r"{ .svc != nil && span:status = error }",
    r#"{ .svc = "a" } && { .b = 2 }"#,
    r#"{ .svc = "a" } >> { .svc = "c" }"#,
    r#"{ .svc = "a" } !>> { .svc = "c" }"#,
    r#"{ .svc = "a" } &~ { .svc = "b" }"#,
    "{ span:duration > 100ms }",
    "{ span:childCount = 2 }",
    r#"{ span:name =~ "child-.*" }"#,
    "{ .ratio = 1.5 }",
    "{ .svc != nil } | count() > 1",
    "{ .svc != nil } | rate() | by(span.svc)",
    "{ .svc != nil } | quantile_over_time(span:duration, .5, .99)",
    "{ .svc != nil } | histogram_over_time(span:duration)",
    "{ .svc != nil } | count_over_time() | by(span.svc) | topk(10)",
    r#"{ .svc = "x" } | compare({ status = error }, 10)"#,
    "{ .svc != nil } | select(span.a, resource.b)",
    "{ .svc != nil } | with(a = .b)",
    "{} with(most_recent=true)",
    "({ .a = 1 } || { .b = 2 }) && { .c = 3 }",
];

/// Every token the grammar gives meaning to, so a generated string drives the
/// lexer and the parser deep instead of dying on its first byte.
const TOKENS: &[&str] = &[
    "{",
    "}",
    "(",
    ")",
    "|",
    "&&",
    "||",
    "!",
    "=",
    "!=",
    "<",
    "<=",
    ">",
    ">=",
    "=~",
    "!~",
    "+",
    "-",
    "*",
    "/",
    "%",
    "^",
    ">>",
    "<<",
    "~",
    "!>>",
    "!<<",
    "!>",
    "!<",
    "&>>",
    "&<<",
    "&>",
    "&<",
    "&~",
    ".",
    ":",
    ",",
    "\"",
    "\\",
    " ",
    "span",
    "resource",
    "event",
    "link",
    "instrumentation",
    "parent",
    "trace",
    "duration",
    "name",
    "status",
    "kind",
    "nestedSetParent",
    "true",
    "false",
    "nil",
    "error",
    "unset",
    "1",
    "1.5",
    ".5",
    "100ms",
    "10h30m",
    "count",
    "rate",
    "count_over_time",
    "quantile_over_time",
    "histogram_over_time",
    "sum_over_time",
    "by",
    "topk",
    "bottomk",
    "compare",
    "select",
    "coalesce",
    "with",
    "most_recent",
    "\"unterminated",
    "==",
];

fn arbitrary_text() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            2 => any::<char>(),
            3 => prop::char::range('\u{0}', '\u{7f}'),
            1 => Just('µ'),
            1 => Just('\u{10ffff}'),
            1 => Just('\u{1f600}'),
        ],
        0..48,
    )
    .prop_map(String::from_iter)
}

fn token_salad() -> impl Strategy<Value = String> {
    prop::collection::vec((prop::sample::select(TOKENS), any::<bool>()), 1..14).prop_map(|parts| {
        let mut out = String::new();
        for (token, spaced) in parts {
            out.push_str(token);
            if spaced {
                out.push(' ');
            }
        }
        out
    })
}

/// A valid seed with one splice applied: a range of it is replaced by a slice
/// of another seed, or by nothing.
fn mutated_seed() -> impl Strategy<Value = String> {
    (
        prop::sample::select(SEED_QUERIES),
        prop::sample::select(SEED_QUERIES),
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
    )
        .prop_map(|(base, donor, cut_a, cut_b, paste_a, paste_b)| {
            let cut = ordered_char_bounds(base, cut_a, cut_b);
            let paste = ordered_char_bounds(donor, paste_a, paste_b);
            let mut out = String::new();
            out.push_str(&base[..cut.0]);
            out.push_str(&donor[paste.0..paste.1]);
            out.push_str(&base[cut.1..]);
            out
        })
}

/// Two ascending char boundaries into `text`, so the generator's own slicing
/// never splits a multi-byte character.
fn ordered_char_bounds(
    text: &str,
    first: prop::sample::Index,
    second: prop::sample::Index,
) -> (usize, usize) {
    let bounds: Vec<usize> = (0..=text.len())
        .filter(|at| text.is_char_boundary(*at))
        .collect();
    let a = *first.get(&bounds);
    let b = *second.get(&bounds);
    (a.min(b), a.max(b))
}

fn arbitrary_query() -> impl Strategy<Value = String> {
    prop_oneof![
        2 => arbitrary_text(),
        3 => token_salad(),
        3 => mutated_seed(),
    ]
}

// -- Generators for the round-trip property ------------------------------

fn arb_attribute_field() -> impl Strategy<Value = Field> {
    (
        prop::sample::select(&[
            Scope::Both,
            Scope::Span,
            Scope::Resource,
            Scope::Parent,
            Scope::Event,
            Scope::Link,
            Scope::Instrumentation,
        ]),
        prop::sample::select(ATTRIBUTE_KEYS),
    )
        .prop_map(|(scope, key)| Field {
            scope,
            key: key.to_owned(),
        })
}

fn arb_intrinsic_field() -> impl Strategy<Value = Field> {
    prop::sample::select(INTRINSICS).prop_map(|(_, key, intrinsic)| Field {
        scope: Scope::Intrinsic(intrinsic),
        key: key.to_owned(),
    })
}

fn arb_field() -> impl Strategy<Value = Field> {
    prop_oneof![3 => arb_attribute_field(), 2 => arb_intrinsic_field()]
}

/// A duration field, which is the only left-hand side that gives a bare
/// duration literal on the right the type [`Value::Duration`].
fn arb_duration_field() -> impl Strategy<Value = Field> {
    prop::sample::select(&[
        (Intrinsic::Duration, "duration"),
        (Intrinsic::TraceDuration, "duration"),
        (Intrinsic::EventTimeSinceStart, "timeSinceStart"),
    ])
    .prop_map(|(intrinsic, key)| Field {
        scope: Scope::Intrinsic(intrinsic),
        key: key.to_owned(),
    })
}

/// A float the renderer can write and the lexer can read back exactly.
/// Dividing a bounded integer by 1000 keeps `{:?}` in plain decimal form; the
/// exponent form that `{:?}` reaches for on large or tiny values would lex as
/// an identifier instead of a number.
fn arb_float() -> impl Strategy<Value = f64> {
    (-1_000_000_i32..1_000_000).prop_map(|raw| f64::from(raw) / 1000.0)
}

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        prop::sample::select(&[
            "a", "b", "GET", "", "a\"b", "a\\b", "a\nb", "server", "error"
        ])
        .prop_map(|text| Value::Str(text.to_owned())),
        (-1_000_000_i64..1_000_000).prop_map(Value::Int),
        arb_float().prop_map(Value::Float),
        any::<bool>().prop_map(Value::Bool),
        Just(Value::Nil),
    ]
}

fn arb_comparison() -> impl Strategy<Value = FieldExpr> {
    let op = prop::sample::select(&[
        ComparisonOp::Eq,
        ComparisonOp::Neq,
        ComparisonOp::Lt,
        ComparisonOp::Lte,
        ComparisonOp::Gt,
        ComparisonOp::Gte,
        ComparisonOp::Re,
        ComparisonOp::Nre,
    ]);
    prop_oneof![
        // A duration literal only keeps its type on a duration field.
        4 => (arb_field(), op.clone(), arb_value())
            .prop_map(|(lhs, op, rhs)| FieldExpr::Comparison { lhs, op, rhs }),
        1 => (arb_duration_field(), op, 0_i64..1_000_000_000_000)
            .prop_map(|(lhs, op, nanos)| FieldExpr::Comparison {
                lhs,
                op,
                rhs: Value::Duration(nanos),
            }),
    ]
}

/// A field expression, at most three levels deep so a counterexample stays
/// readable.
fn arb_field_expr() -> impl Strategy<Value = FieldExpr> {
    prop_oneof![
        4 => arb_comparison(),
        1 => arb_field().prop_map(FieldExpr::Field),
        1 => any::<bool>().prop_map(FieldExpr::Const),
    ]
    .prop_recursive(3, 12, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone())
                .prop_map(|(lhs, rhs)| FieldExpr::And(Box::new(lhs), Box::new(rhs))),
            (inner.clone(), inner.clone())
                .prop_map(|(lhs, rhs)| FieldExpr::Or(Box::new(lhs), Box::new(rhs))),
            inner.prop_map(|inner| FieldExpr::Not(Box::new(inner))),
        ]
    })
}

fn arb_spanset() -> impl Strategy<Value = SpansetExpr> {
    arb_field_expr()
        .prop_map(|expr| SpansetExpr::Selector(Box::new(expr)))
        .prop_recursive(3, 10, 2, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone())
                    .prop_map(|(lhs, rhs)| SpansetExpr::And(Box::new(lhs), Box::new(rhs))),
                (inner.clone(), inner.clone())
                    .prop_map(|(lhs, rhs)| SpansetExpr::Or(Box::new(lhs), Box::new(rhs))),
                (
                    inner.clone(),
                    prop::sample::select(&[
                        StructuralOp::Descendant,
                        StructuralOp::Ancestor,
                        StructuralOp::Child,
                        StructuralOp::Parent,
                        StructuralOp::Sibling,
                        StructuralOp::NegDescendant,
                        StructuralOp::NegAncestor,
                        StructuralOp::NegChild,
                        StructuralOp::NegParent,
                        StructuralOp::NegSibling,
                        StructuralOp::UnionDescendant,
                        StructuralOp::UnionAncestor,
                        StructuralOp::UnionChild,
                        StructuralOp::UnionParent,
                        StructuralOp::UnionSibling,
                    ]),
                    inner,
                )
                    .prop_map(|(lhs, op, rhs)| SpansetExpr::Structural {
                        op,
                        lhs: Box::new(lhs),
                        rhs: Box::new(rhs),
                    }),
            ]
        })
}

fn arb_aggregate() -> impl Strategy<Value = Aggregate> {
    prop_oneof![
        Just(Aggregate::Count),
        Just(Aggregate::Rate),
        Just(Aggregate::CountOverTime),
        arb_field().prop_map(Aggregate::SumOverTime),
        arb_field().prop_map(Aggregate::AvgOverTime),
        arb_field().prop_map(Aggregate::MinOverTime),
        arb_field().prop_map(Aggregate::MaxOverTime),
        arb_field().prop_map(Aggregate::HistogramOverTime),
        arb_field().prop_map(Aggregate::Sum),
        arb_field().prop_map(Aggregate::Avg),
        arb_field().prop_map(Aggregate::Max),
        arb_field().prop_map(Aggregate::Min),
        (
            arb_field(),
            prop::collection::vec(
                (0_u32..=100).prop_map(|hundredths| f64::from(hundredths) / 100.0),
                0..3
            ),
        )
            .prop_map(|(field, quantiles)| Aggregate::QuantileOverTime { field, quantiles }),
    ]
}

/// A pipeline stage that can stand on its own, that is, everything but the
/// numeric filter, which the grammar only accepts behind another stage.
fn arb_leading_stage() -> impl Strategy<Value = Pipeline> {
    prop_oneof![
        6 => arb_aggregate().prop_map(Pipeline::Aggregate),
        1 => prop::collection::vec(arb_field(), 1..3).prop_map(Pipeline::By),
        1 => prop::collection::vec(arb_field(), 1..3).prop_map(Pipeline::Select),
        1 => (0_usize..1000).prop_map(Pipeline::TopK),
        1 => (0_usize..1000).prop_map(Pipeline::BottomK),
        1 => Just(Pipeline::Coalesce),
        1 => (
            arb_spanset(),
            0_usize..1000,
            prop::option::of((-1_000_000_000_000_i64..1_000_000_000_000, -1_000_000_000_000_i64..1_000_000_000_000)),
        )
            .prop_map(|(selection, top_n, window)| Pipeline::Compare {
                selection: Box::new(selection),
                top_n,
                start: window.map(|(start, _)| start),
                end: window.map(|(_, end)| end),
            }),
    ]
}

/// One pipeline group: a stage, then the `by` clause the grammar lets follow
/// it, then the numeric filter the grammar lets follow that.
fn arb_pipeline_group() -> impl Strategy<Value = Vec<Pipeline>> {
    (
        arb_leading_stage(),
        prop::option::of(prop::collection::vec(arb_field(), 1..3)),
        prop::option::of((
            prop::sample::select(&[
                ComparisonOp::Eq,
                ComparisonOp::Neq,
                ComparisonOp::Lt,
                ComparisonOp::Lte,
                ComparisonOp::Gt,
                ComparisonOp::Gte,
            ]),
            arb_float(),
        )),
    )
        .prop_map(|(stage, by, filter)| {
            let is_by = matches!(stage, Pipeline::By(_));
            let mut out = vec![stage];
            if let Some(fields) = by
                && !is_by
            {
                out.push(Pipeline::By(fields));
            }
            if let Some((op, value)) = filter {
                out.push(Pipeline::Filter { op, value });
            }
            out
        })
}

fn arb_query() -> impl Strategy<Value = Query> {
    (
        arb_spanset(),
        prop::collection::vec(arb_pipeline_group(), 0..3),
        any::<bool>(),
        prop::option::of(any::<bool>()),
        prop::option::of(any::<bool>()),
    )
        .prop_map(|(root, groups, most_recent, exemplars, sample)| {
            // `QueryHints` is `pub` inside a private module that the crate root
            // never re-exports, so the type has no name out here and cannot be
            // constructed by name either. Parsing the match-all query yields a
            // `Query` whose hints are the default ones, and every field is
            // `pub`, so overwriting them afterwards builds any query at all.
            let mut query = parse("{}").expect("the match-all query parses");
            query.root = root;
            query.pipeline = groups.into_iter().flatten().collect();
            query.hints.most_recent = most_recent;
            query.hints.exemplars = exemplars;
            query.hints.sample = sample;
            query
        })
}

// -- The renderer --------------------------------------------------------

fn render_query(query: &Query) -> String {
    let mut out = String::new();
    render_spanset(&mut out, &query.root, 0);
    for stage in &query.pipeline {
        render_pipeline_stage(&mut out, stage);
    }
    let mut hints = Vec::new();
    if query.hints.most_recent {
        hints.push("most_recent=true".to_owned());
    }
    if let Some(value) = query.hints.exemplars {
        hints.push(format!("exemplars={value}"));
    }
    if let Some(value) = query.hints.sample {
        hints.push(format!("sample={value}"));
    }
    if !hints.is_empty() {
        let _ = write!(out, " with({})", hints.join(", "));
    }
    out
}

/// Spanset precedence, lowest binding first. A child that binds less tightly
/// than its position allows needs parentheses.
fn spanset_precedence(expr: &SpansetExpr) -> u8 {
    match expr {
        SpansetExpr::Or(..) => 1,
        SpansetExpr::And(..) => 2,
        SpansetExpr::Structural { .. } => 3,
        SpansetExpr::Selector(_) => 4,
    }
}

fn render_spanset(out: &mut String, expr: &SpansetExpr, parent: u8) {
    let needs_parentheses = spanset_precedence(expr) < parent;
    if needs_parentheses {
        out.push('(');
    }
    match expr {
        SpansetExpr::Selector(field_expr) => {
            out.push_str("{ ");
            render_field_expr(out, field_expr, 0);
            out.push_str(" }");
        }
        SpansetExpr::Or(lhs, rhs) => {
            render_spanset(out, lhs, 1);
            out.push_str(" || ");
            render_spanset(out, rhs, 2);
        }
        SpansetExpr::And(lhs, rhs) => {
            render_spanset(out, lhs, 2);
            out.push_str(" && ");
            render_spanset(out, rhs, 3);
        }
        SpansetExpr::Structural { op, lhs, rhs } => {
            render_spanset(out, lhs, 3);
            let _ = write!(out, " {} ", structural_text(*op));
            render_spanset(out, rhs, 4);
        }
    }
    if needs_parentheses {
        out.push(')');
    }
}

fn field_expr_precedence(expr: &FieldExpr) -> u8 {
    match expr {
        FieldExpr::Or(..) => 1,
        FieldExpr::And(..) => 2,
        FieldExpr::Not(_) => 3,
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) | FieldExpr::Const(_) => 4,
    }
}

fn render_field_expr(out: &mut String, expr: &FieldExpr, parent: u8) {
    let needs_parentheses = field_expr_precedence(expr) < parent;
    if needs_parentheses {
        out.push('(');
    }
    match expr {
        FieldExpr::Comparison { lhs, op, rhs } => {
            render_field(out, lhs);
            let _ = write!(out, " {} ", comparison_text(*op));
            render_value(out, rhs);
        }
        FieldExpr::And(lhs, rhs) => {
            render_field_expr(out, lhs, 2);
            out.push_str(" && ");
            render_field_expr(out, rhs, 3);
        }
        FieldExpr::Or(lhs, rhs) => {
            render_field_expr(out, lhs, 1);
            out.push_str(" || ");
            render_field_expr(out, rhs, 2);
        }
        FieldExpr::Not(inner) => {
            out.push('!');
            render_field_expr(out, inner, 3);
        }
        FieldExpr::Field(field) => render_field(out, field),
        FieldExpr::Const(value) => {
            let _ = write!(out, "{value}");
        }
    }
    if needs_parentheses {
        out.push(')');
    }
}

fn render_field(out: &mut String, field: &Field) {
    match &field.scope {
        Scope::Both => {
            let _ = write!(out, ".{}", field.key);
        }
        Scope::Span => {
            let _ = write!(out, "span.{}", field.key);
        }
        Scope::Resource => {
            let _ = write!(out, "resource.{}", field.key);
        }
        Scope::Parent => {
            let _ = write!(out, "parent.{}", field.key);
        }
        Scope::Event => {
            let _ = write!(out, "event.{}", field.key);
        }
        Scope::Link => {
            let _ = write!(out, "link.{}", field.key);
        }
        Scope::Instrumentation => {
            let _ = write!(out, "instrumentation.{}", field.key);
        }
        Scope::Intrinsic(intrinsic) => {
            let scope = INTRINSICS
                .iter()
                .find(|(_, key, candidate)| candidate == intrinsic && *key == field.key)
                .map(|(scope, _, _)| *scope)
                .expect("the generator only builds intrinsics from the table");
            let _ = write!(out, "{scope}:{}", field.key);
        }
    }
}

fn render_value(out: &mut String, value: &Value) {
    match value {
        // Always quote: an unquoted string would lex as an identifier, a
        // keyword, or a number depending on its spelling.
        Value::Str(text) => {
            out.push('"');
            for ch in text.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    other => out.push(other),
                }
            }
            out.push('"');
        }
        Value::Int(number) => {
            let _ = write!(out, "{number}");
        }
        // `{:?}` keeps the decimal point that tells the lexer this is a float
        // and not an integer, and it is the shortest form that reads back as
        // the same `f64`.
        Value::Float(number) => {
            let _ = write!(out, "{number:?}");
        }
        Value::Duration(nanos) => {
            let _ = write!(out, "{nanos}ns");
        }
        Value::Bool(value) => {
            let _ = write!(out, "{value}");
        }
        Value::Nil => out.push_str("nil"),
    }
}

fn render_pipeline_stage(out: &mut String, stage: &Pipeline) {
    // A numeric filter is not a stage of its own: it hangs off the stage in
    // front of it, with no pipe.
    if let Pipeline::Filter { op, value } = stage {
        let _ = write!(out, " {} {value:?}", comparison_text(*op));
        return;
    }
    out.push_str(" | ");
    match stage {
        Pipeline::Filter { .. } => unreachable!("handled above"),
        Pipeline::Aggregate(aggregate) => render_aggregate(out, aggregate),
        Pipeline::By(fields) => {
            out.push_str("by(");
            render_field_list(out, fields);
            out.push(')');
        }
        Pipeline::Select(fields) => {
            out.push_str("select(");
            render_field_list(out, fields);
            out.push(')');
        }
        Pipeline::TopK(k) => {
            let _ = write!(out, "topk({k})");
        }
        Pipeline::BottomK(k) => {
            let _ = write!(out, "bottomk({k})");
        }
        Pipeline::Coalesce => out.push_str("coalesce()"),
        Pipeline::Compare {
            selection,
            top_n,
            start,
            end,
        } => {
            out.push_str("compare(");
            render_spanset(out, selection, 0);
            let _ = write!(out, ", {top_n}");
            if let (Some(start), Some(end)) = (start, end) {
                let _ = write!(out, ", {start}, {end}");
            }
            out.push(')');
        }
        Pipeline::With(bindings) => {
            out.push_str("with(");
            for (at, binding) in bindings.iter().enumerate() {
                if at > 0 {
                    out.push_str(", ");
                }
                let _ = write!(out, "{} = ", binding.name);
                render_field_expr(out, &binding.expr, 0);
            }
            out.push(')');
        }
    }
}

fn render_field_list(out: &mut String, fields: &[Field]) {
    for (at, field) in fields.iter().enumerate() {
        if at > 0 {
            out.push_str(", ");
        }
        render_field(out, field);
    }
}

fn render_aggregate(out: &mut String, aggregate: &Aggregate) {
    match aggregate {
        Aggregate::Count => out.push_str("count()"),
        Aggregate::Rate => out.push_str("rate()"),
        Aggregate::CountOverTime => out.push_str("count_over_time()"),
        Aggregate::SumOverTime(field) => render_call(out, "sum_over_time", field),
        Aggregate::AvgOverTime(field) => render_call(out, "avg_over_time", field),
        Aggregate::MinOverTime(field) => render_call(out, "min_over_time", field),
        Aggregate::MaxOverTime(field) => render_call(out, "max_over_time", field),
        Aggregate::HistogramOverTime(field) => render_call(out, "histogram_over_time", field),
        Aggregate::Sum(field) => render_call(out, "sum", field),
        Aggregate::Avg(field) => render_call(out, "avg", field),
        Aggregate::Max(field) => render_call(out, "max", field),
        Aggregate::Min(field) => render_call(out, "min", field),
        Aggregate::QuantileOverTime { field, quantiles } => {
            out.push_str("quantile_over_time(");
            render_field(out, field);
            for quantile in quantiles {
                let _ = write!(out, ", {quantile:?}");
            }
            out.push(')');
        }
    }
}

fn render_call(out: &mut String, name: &str, field: &Field) {
    let _ = write!(out, "{name}(");
    render_field(out, field);
    out.push(')');
}

fn comparison_text(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Neq => "!=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Re => "=~",
        ComparisonOp::Nre => "!~",
    }
}

fn structural_text(op: StructuralOp) -> &'static str {
    match op {
        StructuralOp::Descendant => ">>",
        StructuralOp::Ancestor => "<<",
        StructuralOp::Child => ">",
        StructuralOp::Parent => "<",
        StructuralOp::Sibling => "~",
        StructuralOp::NegDescendant => "!>>",
        StructuralOp::NegAncestor => "!<<",
        StructuralOp::NegChild => "!>",
        StructuralOp::NegParent => "!<",
        StructuralOp::NegSibling => "!~",
        StructuralOp::UnionDescendant => "&>>",
        StructuralOp::UnionAncestor => "&<<",
        StructuralOp::UnionChild => "&>",
        StructuralOp::UnionParent => "&<",
        StructuralOp::UnionSibling => "&~",
    }
}

/// The seeds carry the mutation strategy, so a typo in one would quietly
/// narrow what the totality property covers.
#[test]
fn the_seed_queries_are_valid_traceql() {
    for query in SEED_QUERIES {
        assert!(parse(query).is_ok(), "seed query: {query}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// No string panics the lexer or the parser. `/api/search` passes `q`
    /// through untouched, so this is the whole untrusted surface.
    #[test]
    fn lex_and_parse_never_panic(query in arbitrary_query()) {
        let _ = krabka_traceql::lex(&query);
        let _ = parse(&query);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Rendering a query and reparsing it yields the same query.
    #[test]
    fn rendering_a_query_round_trips(query in arb_query()) {
        let rendered = render_query(&query);
        let reparsed = parse(&rendered)
            .map_err(|error| TestCaseError::fail(format!("{rendered} does not parse: {error}")))?;

        // proptest prints the shrunk `query` itself, so the message only has
        // to say which text it rendered to.
        prop_assert!(reparsed == query, "`{rendered}` reparsed to a different query");
    }
}
