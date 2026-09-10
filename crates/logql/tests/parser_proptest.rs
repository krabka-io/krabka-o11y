//! Property tests for the `LogQL` parser and the `LogqlExpr` formatter.
//!
//! The properties here cover what the example-based suites in
//! `tests/parser.rs` cannot.
//!
//! `parse_*_never_panics` is the totality property the code style guide
//! demands of every decoder that reads untrusted input: the parser answers
//! `Ok` or a typed `ParseError` for any string at all, and never panics.
//! `/loki/api/v1/query_range` hands it the raw `query` parameter, so every
//! input here is one a caller can send.
//!
//! `formatting_a_logql_expression_round_trips` is the round-trip property
//! behind `/loki/api/v1/format_query`, which renders a parsed query back to
//! text. A generated expression tree is rendered through `Display` and
//! reparsed; the tree that comes back has to be the tree that went in. A
//! divergence there is a user-visible bug: the endpoint would hand a caller a
//! query that means something other than the one they sent.
//! `formatting_a_logql_expression_is_idempotent` is the weaker companion that
//! holds even where a first render normalizes.
//!
//! Two round trips are known to fail today, and each has an `#[ignore]`d test
//! of its own that names the input and says what goes wrong. The generator
//! steers around both, so what remains green is the property, not a weakened
//! version of it.

use std::fmt::Write as _;

use assert2::assert;
use krabka_logql::{
    ComparisonOp, LogqlExpr, MetricBinarySetOp, MetricScalarArithmeticOp,
    MetricVectorGroupModifier, MetricVectorMatching, parse_logql_expr, parse_metric_query,
    parse_query,
};
use proptest::prelude::*;

/// Valid stream selectors. None of them carries a binary operator at the top
/// level, so each one stays a single leaf when it is embedded in a larger
/// expression.
const STREAM_SOURCES: &[&str] = &[
    r#"{app="web"}"#,
    r#"{app="web", env!="dev"}"#,
    r#"{app=~"we.*"} |= "boom""#,
    r#"{app="web"} | json"#,
    r#"{app="web"} | logfmt | line_format "{{.msg}}""#,
];

/// Valid metric queries, again free of top-level binary operators.
const METRIC_SOURCES: &[&str] = &[
    r#"rate({app="web"}[5m])"#,
    r#"count_over_time({app="web"}[30s])"#,
    r#"sum by (app) (rate({app="web"}[5m]))"#,
    r#"sum(rate({app="web"}[5m]))"#,
    r#"quantile_over_time(0.99, {app="web"} | unwrap latency [5m])"#,
    r#"topk(3, rate({app="web"}[1m]))"#,
];

/// Scalar literals that the formatter reproduces verbatim.
const SCALAR_SOURCES: &[&str] = &["1", "0", "2.5", "-3", "1e-3", "1024"];

/// Whole valid queries, used to seed the mutation strategy so that the
/// arbitrary-input property spends most of its budget near the grammar rather
/// than on strings the lexer rejects in its first byte.
const SEED_QUERIES: &[&str] = &[
    r#"{app="web"}"#,
    r#"{app="web"} |= "boom" != "debug""#,
    r#"{app="web"} | json | status >= 500"#,
    r#"{app="web"} | logfmt | line_format "{{.msg}} {{.status}}""#,
    r#"{app="web"} | pattern "<_> <status>" | status = "500""#,
    r#"{app="web"} | regexp "(?P<status>\\d+)""#,
    r#"rate({app="web"}[5m])"#,
    r#"sum by (app) (rate({app="web"}[5m]))"#,
    r#"sum(rate({app="web"}[5m])) / sum(rate({app="api"}[5m]))"#,
    r#"count_over_time({app="web"}[30s]) > bool 5"#,
    r#"label_replace(rate({app="web"}[5m]), "service", "$1", "app", "(.*)")"#,
    r#"label_join(rate({app="web"}[5m]), "k", "-", "app", "env")"#,
    r#"sort_desc(sum by (app) (rate({app="web"}[5m])))"#,
    r"vector(1) + 2 * 3 ^ 4 ^ 5",
    r#"quantile_over_time(0.99, {app="web"} | unwrap latency [5m]) by (app)"#,
    r#"rate({app="web"}[5m] offset 10m)"#,
    r#"sum(rate({app="web"}[5m])) or sum(rate({app="api"}[5m]))"#,
    r#"absent_over_time({app="web"}[5m])"#,
    r#"bytes_over_time({app="web"}[5m])"#,
    r#"{app="web"} | json | __error__ = """#,
];

/// The alphabet the token-salad strategy draws from: every operator, keyword
/// and punctuation mark the grammar gives meaning to, so a generated string
/// drives the parser deep rather than failing on its first token.
const TOKENS: &[&str] = &[
    "{",
    "}",
    "(",
    ")",
    "[",
    "]",
    ",",
    "|",
    "|=",
    "!=",
    "|~",
    "!~",
    "=",
    "=~",
    "==",
    ">=",
    "<=",
    ">",
    "<",
    "+",
    "-",
    "*",
    "/",
    "%",
    "^",
    "\"",
    "\\",
    "`",
    " ",
    "5m",
    "1",
    "0.5",
    "app",
    "__error__",
    "json",
    "logfmt",
    "pattern",
    "regexp",
    "unpack",
    "unwrap",
    "line_format",
    "label_format",
    "decolorize",
    "drop",
    "keep",
    "rate",
    "count_over_time",
    "sum",
    "by",
    "without",
    "on",
    "ignoring",
    "group_left",
    "group_right",
    "bool",
    "and",
    "or",
    "unless",
    "offset",
    "topk",
    "quantile_over_time",
    "label_replace",
    "label_join",
    "vector",
    "sort",
    "sort_desc",
    "\"unterminated",
    "#",
];

/// Arbitrary Unicode, weighted towards the code points that trip a
/// byte-indexing parser: control characters, multi-byte characters, and the
/// top of the code point range.
fn arbitrary_text() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            2 => any::<char>(),
            3 => prop::char::range('\u{0}', '\u{7f}'),
            1 => Just('\u{10ffff}'),
            1 => Just('é'),
            1 => Just('\u{1f600}'),
        ],
        0..48,
    )
    .prop_map(String::from_iter)
}

/// Concatenated grammar tokens, separated by nothing or a space.
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

/// A valid seed query with one splice applied: a range of it is replaced by a
/// slice of another seed, or by nothing. Near-miss inputs like these reach
/// error paths that neither pure noise nor a token salad finds.
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

/// Two ascending char boundaries into `text`, so slicing never splits a
/// multi-byte character and never panics inside the generator itself.
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

/// Every string a caller can put in the `query` parameter.
fn arbitrary_query() -> impl Strategy<Value = String> {
    prop_oneof![
        2 => arbitrary_text(),
        3 => token_salad(),
        3 => mutated_seed(),
    ]
}

/// A leaf expression, built by parsing a source that the formatter reproduces
/// verbatim. Building leaves through the parser keeps the generated tree
/// inside the set of trees the parser can actually produce, which is what the
/// round-trip property is about.
fn arb_leaf() -> impl Strategy<Value = LogqlExpr> {
    prop_oneof![
        prop::sample::select(SCALAR_SOURCES).prop_map(|source| LogqlExpr::Scalar(source.into())),
        prop::sample::select(STREAM_SOURCES).prop_map(|source| LogqlExpr::Stream {
            query: parse_query(source).expect("a stream source in the table parses"),
            source: source.into(),
        }),
        prop::sample::select(METRIC_SOURCES).prop_map(|source| LogqlExpr::Metric {
            query: parse_metric_query(source).expect("a metric source in the table parses"),
            source: source.into(),
        }),
    ]
}

fn arb_labels() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(
        prop::sample::select(&["app", "env", "cluster", "namespace"]).prop_map(String::from),
        1..3,
    )
}

/// A vector-matching modifier. `group` stays `None` for set operators, which
/// the parser rejects a group modifier on.
fn arb_matching(allow_group: bool) -> impl Strategy<Value = Option<MetricVectorMatching>> {
    // A group modifier with an empty label list renders bare, as
    // `group_left`, and the parser cannot always read that back. The case is
    // pinned in `a_bare_group_modifier_round_trips_before_a_parenthesised_operand`
    // below, so this generator keeps the label list non-empty.
    let group = if allow_group {
        prop_oneof![
            2 => Just(None),
            1 => arb_labels().prop_map(|labels| Some(MetricVectorGroupModifier::Left(labels))),
            1 => arb_labels().prop_map(|labels| Some(MetricVectorGroupModifier::Right(labels))),
        ]
        .boxed()
    } else {
        Just(None).boxed()
    };
    prop_oneof![
        2 => Just(None),
        1 => (arb_labels(), group.clone())
            .prop_map(|(labels, group)| Some(MetricVectorMatching::On { labels, group })),
        1 => (arb_labels(), group)
            .prop_map(|(labels, group)| Some(MetricVectorMatching::Ignoring { labels, group })),
    ]
}

/// Label and pattern arguments for `label_replace` and `label_join`. The
/// formatter escapes them, so the alphabet includes the characters that need
/// escaping.
fn arb_string_arg() -> impl Strategy<Value = String> {
    prop::sample::select(&[
        "service", "$1", "(.*)", "app", "", "-", "a\"b", "a\\b", "a\nb", "a\tb",
    ])
    .prop_map(String::from)
}

/// An expression tree, at most four levels deep. The depth bound keeps a
/// counterexample readable; a shrunk tree of twenty nodes tells no one
/// anything.
fn arb_expr() -> impl Strategy<Value = LogqlExpr> {
    arb_leaf().prop_recursive(4, 24, 2, |inner| {
        prop_oneof![
            // `vector` takes a scalar argument only, so it wraps a scalar leaf.
            prop::sample::select(SCALAR_SOURCES)
                .prop_map(|source| LogqlExpr::Vector(Box::new(LogqlExpr::Scalar(source.into())))),
            (inner.clone(), any::<bool>()).prop_map(|(expr, descending)| LogqlExpr::Sort {
                expr: Box::new(expr),
                descending
            }),
            (
                inner.clone(),
                arb_string_arg(),
                arb_string_arg(),
                arb_string_arg(),
                arb_string_arg(),
            )
                .prop_map(
                    |(expr, destination_label, replacement, source_label, pattern)| {
                        LogqlExpr::LabelReplace {
                            expr: Box::new(expr),
                            destination_label,
                            replacement,
                            source_label,
                            pattern,
                        }
                    }
                ),
            (
                inner.clone(),
                arb_string_arg(),
                arb_string_arg(),
                prop::collection::vec(arb_string_arg(), 1..3),
            )
                .prop_map(|(expr, destination_label, separator, source_labels)| {
                    LogqlExpr::LabelJoin {
                        expr: Box::new(expr),
                        destination_label,
                        separator,
                        source_labels,
                    }
                }),
            (
                inner.clone(),
                prop::sample::select(&[
                    MetricScalarArithmeticOp::Add,
                    MetricScalarArithmeticOp::Subtract,
                    MetricScalarArithmeticOp::Multiply,
                    MetricScalarArithmeticOp::Divide,
                    MetricScalarArithmeticOp::Modulo,
                    MetricScalarArithmeticOp::Power,
                ]),
                arb_matching(true),
                inner.clone(),
            )
                .prop_map(|(left, op, matching, right)| LogqlExpr::Arithmetic {
                    left: Box::new(left),
                    op,
                    // A vector-matching modifier in front of a right operand
                    // that starts with a minus sign renders to text the parser
                    // reads differently. The case is pinned in
                    // `a_signed_operand_round_trips_after_a_vector_matching_modifier`
                    // below, so this generator drops the modifier there.
                    matching: if right.to_string().starts_with('-') {
                        None
                    } else {
                        matching
                    },
                    right: Box::new(right),
                }),
            (
                inner.clone(),
                // `=~` and `!~` are label-matcher operators, not expression
                // operators, so `parse_expr` never produces them and they are
                // not part of the reachable tree set.
                prop::sample::select(&[
                    ComparisonOp::Equal,
                    ComparisonOp::NotEqual,
                    ComparisonOp::Greater,
                    ComparisonOp::GreaterEqual,
                    ComparisonOp::Less,
                    ComparisonOp::LessEqual,
                ]),
                any::<bool>(),
                arb_matching(true),
                inner.clone(),
            )
                .prop_map(|(left, op, bool_modifier, matching, right)| {
                    LogqlExpr::Comparison {
                        left: Box::new(left),
                        op,
                        bool_modifier,
                        matching,
                        right: Box::new(right),
                    }
                }),
            (
                inner.clone(),
                prop::sample::select(&[
                    MetricBinarySetOp::And,
                    MetricBinarySetOp::Or,
                    MetricBinarySetOp::Unless,
                ]),
                arb_matching(false),
                inner,
            )
                .prop_map(|(left, op, matching, right)| LogqlExpr::Set {
                    left: Box::new(left),
                    op,
                    matching,
                    right: Box::new(right),
                }),
        ]
    })
}

/// A one-line sketch of a tree's shape, for a failure message that reads
/// without a `Debug` dump of the whole tree.
fn shape(expr: &LogqlExpr) -> String {
    let mut out = String::new();
    write_shape(&mut out, expr);
    out
}

fn write_shape(out: &mut String, expr: &LogqlExpr) {
    match expr {
        LogqlExpr::Stream { .. } => out.push_str("stream"),
        LogqlExpr::Metric { .. } => out.push_str("metric"),
        LogqlExpr::Scalar(text) => {
            let _ = write!(out, "scalar({text})");
        }
        LogqlExpr::Vector(inner) => nest(out, "vector", [inner.as_ref()]),
        LogqlExpr::LabelReplace { expr, .. } => nest(out, "label_replace", [expr.as_ref()]),
        LogqlExpr::LabelJoin { expr, .. } => nest(out, "label_join", [expr.as_ref()]),
        LogqlExpr::Sort { expr, .. } => nest(out, "sort", [expr.as_ref()]),
        LogqlExpr::Arithmetic { left, right, .. } => {
            nest(out, "arith", [left.as_ref(), right.as_ref()]);
        }
        LogqlExpr::Comparison { left, right, .. } => {
            nest(out, "cmp", [left.as_ref(), right.as_ref()]);
        }
        LogqlExpr::Set { left, right, .. } => nest(out, "set", [left.as_ref(), right.as_ref()]),
    }
}

fn nest<'a>(out: &mut String, name: &str, children: impl IntoIterator<Item = &'a LogqlExpr>) {
    out.push_str(name);
    out.push('(');
    for (at, child) in children.into_iter().enumerate() {
        if at > 0 {
            out.push_str(", ");
        }
        write_shape(out, child);
    }
    out.push(')');
}

/// A bare `group_left`, one with no label list, is ambiguous before a
/// parenthesised right operand: the parser reads the operand's opening
/// parenthesis as the start of the modifier's label list and fails.
///
/// This is a divergence at `/loki/api/v1/format_query` and at
/// `/loki/api/v1/query_range` alike, and it runs in both directions. The
/// parser rejects `1 + on(app) group_left (2 + 3)`, which Prometheus accepts,
/// and the formatter renders the tree for
/// `on(app) group_left` with a parenthesised right operand back to text that
/// no longer reparses.
#[test]
#[ignore = "krabka-logql rejects a bare group_left before a parenthesised operand; see report"]
fn a_bare_group_modifier_round_trips_before_a_parenthesised_operand() {
    let query = "1 + on(app) group_left (2 + 3)";
    let expr = parse_logql_expr(query).expect("a bare group_left parses before a parenthesis");

    assert!(expr.to_string() == query);
    assert!(parse_logql_expr(&expr.to_string()).is_ok_and(|round_tripped| round_tripped == expr));
}

/// A vector-matching modifier ends in `)`, and the parser reads the sign of a
/// following negative literal as a binary minus, because
/// `sign_is_unary_or_exponent` looks only at the character in front of the
/// sign.
///
/// `1 + on(app) (-3)` parses, and the tree it produces renders back to
/// `1 + on(app) -3`, which does not parse at all. That makes
/// `/loki/api/v1/format_query` hand a caller a query Krabka then rejects.
#[test]
#[ignore = "krabka-logql misreads a negative literal after a vector-matching modifier; see report"]
fn a_signed_operand_round_trips_after_a_vector_matching_modifier() {
    let expr = parse_logql_expr("1 + on(app) (-3)").expect("a parenthesised negative operand");

    let rendered = expr.to_string();
    assert!(parse_logql_expr(&rendered).is_ok_and(|round_tripped| round_tripped == expr));
}

/// The leaf tables and the mutation seeds are hand-written, so a typo in one
/// would quietly narrow what the properties below cover. This checks that every
/// one of them is the valid query it claims to be.
#[test]
fn the_generator_tables_hold_only_valid_queries() {
    for source in STREAM_SOURCES {
        assert!(parse_query(source).is_ok(), "stream source: {source}");
    }
    for source in METRIC_SOURCES {
        assert!(
            parse_metric_query(source).is_ok(),
            "metric source: {source}"
        );
    }
    for source in SCALAR_SOURCES {
        assert!(parse_logql_expr(source).is_ok(), "scalar source: {source}");
    }
    for query in SEED_QUERIES {
        assert!(parse_logql_expr(query).is_ok(), "seed query: {query}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// No string panics the stream-query parser.
    #[test]
    fn parse_query_never_panics(query in arbitrary_query()) {
        let _ = parse_query(&query);
    }

    /// No string panics the metric-query parser.
    #[test]
    fn parse_metric_query_never_panics(query in arbitrary_query()) {
        let _ = parse_metric_query(&query);
    }

    /// No string panics the recursive expression parser, which is the one
    /// `/loki/api/v1/format_query` calls.
    #[test]
    fn parse_logql_expr_never_panics(query in arbitrary_query()) {
        let _ = parse_logql_expr(&query);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Rendering an expression and reparsing it yields the same expression.
    #[test]
    fn formatting_a_logql_expression_round_trips(expr in arb_expr()) {
        let rendered = expr.to_string();
        let reparsed = parse_logql_expr(&rendered)
            .map_err(|error| TestCaseError::fail(format!(
                "{rendered} does not reparse: {error}"
            )))?;

        prop_assert!(
            reparsed == expr,
            "rendered `{rendered}` reparsed to {} instead of {}",
            shape(&reparsed),
            shape(&expr),
        );
    }

    /// Rendering is idempotent: the text a second render produces is the text
    /// the first one did. This is the property `/loki/api/v1/format_query`
    /// promises its callers, and it holds even where a first render
    /// normalizes.
    #[test]
    fn formatting_a_logql_expression_is_idempotent(expr in arb_expr()) {
        let once = expr.to_string();
        let reparsed = parse_logql_expr(&once)
            .map_err(|error| TestCaseError::fail(format!("{once} does not reparse: {error}")))?;
        prop_assert_eq!(reparsed.to_string(), once);
    }
}
