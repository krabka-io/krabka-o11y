//! Property tests for the `PromQL` front end.
//!
//! `parse_promql_never_panics` is the totality property that the code style
//! guide requires of anything that reads untrusted input. `/api/v1/query` and
//! `/api/v1/query_range` hand the `query` parameter to [`parse_promql`]
//! unchanged, and before the upstream parser sees it, Krabka runs two
//! hand-written scanners over it: one strips the `anchored` and `smoothed`
//! selector modifiers, and one folds Prometheus duration expressions to fixed
//! durations. Both walk the string by index, so both are worth a generative
//! test.
//!
//! `parsing_a_rendered_query_returns_the_same_ast` is the round-trip property.
//! `PromQL` text goes in, the parsed AST is rendered back to text, and the
//! text is parsed again; the second AST has to equal the first. The property
//! is stated over the AST rather than the text on purpose: rendering
//! normalizes, so `render(parse(q))` is often not `q`, but it must never mean
//! something else. The generator builds queries that type check, since an
//! expression the parser rejects proves nothing about rendering.
//! `rendering_a_parsed_query_is_idempotent` is the weaker companion that
//! holds wherever rendering normalizes, and
//! `the_formatter_sorts_a_selectors_matchers` pins the one normalization the
//! generator has to steer around.

use krabka_promql::{DurationExprContext, parse_promql, parse_promql_with_duration_context};
use krabka_units::prelude::*;
use proptest::prelude::*;

/// Valid queries, used as the seeds the mutation strategy edits, and as the
/// starting points that keep the arbitrary-input property near the grammar.
const SEED_QUERIES: &[&str] = &[
    "up",
    r#"http_requests_total{job="api", code=~"5.."}"#,
    r#"{__name__="up"}"#,
    "rate(http_requests_total[5m])",
    "sum by (job) (rate(http_requests_total[5m]))",
    "sum without (instance) (up)",
    "topk(3, sum by (job) (rate(http_requests_total[5m])))",
    "histogram_quantile(0.9, rate(latency_bucket[5m]))",
    r#"label_replace(up, "svc", "$1", "job", "(.*)")"#,
    "up offset 5m",
    "up @ 1609746000",
    "up @ start()",
    "avg_over_time(rate(http_requests_total[5m])[30m:1m])",
    "sum(rate(a[5m])) / sum(rate(b[5m]))",
    "up > bool 0",
    "up and on (job) down",
    "up * on (job) group_left (env) meta",
    "-up",
    "1 + 2 * 3 ^ 4",
    "clamp_min(up, 0)",
    "up[5m] anchored",
    "rate(up[1s * 4])",
    "count_over_time(up[5m * 2])",
    "quantile(0.99, up)",
    "absent_over_time(up[5m])",
];

/// Every token the grammar gives meaning to, plus the two Krabka selector
/// modifiers, so a generated string reaches the scanners rather than dying on
/// its first byte.
const TOKENS: &[&str] = &[
    "{",
    "}",
    "(",
    ")",
    "[",
    "]",
    ":",
    ",",
    "=",
    "!=",
    "=~",
    "!~",
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
    "'",
    "`",
    "\\",
    " ",
    "@",
    "up",
    "job",
    "5m",
    "1h30m",
    "1",
    "0.5",
    "1e3",
    "rate",
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
    "quantile",
    "histogram_quantile",
    "label_replace",
    "start",
    "end",
    "step",
    "anchored",
    "smoothed",
    "\"unterminated",
    "inf",
    "nan",
    "__name__",
];

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
/// of another seed, or by nothing. Near-miss inputs like these reach error
/// paths that neither pure noise nor a token salad finds.
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

const METRICS: &[&str] = &["up", "http_requests_total", "latency_bucket", "node_cpu"];
/// Label names, in the order the formatter sorts them into. The generator
/// keeps a selector's matchers in this order so that rendering does not
/// reorder them; `the_formatter_sorts_a_selectors_matchers` below pins the
/// sorting itself.
const LABELS: &[&str] = &["code", "env", "instance", "job"];
const RANGES: &[&str] = &["5m", "1h", "30s", "1h30m", "7d"];

/// A number the parser reads and the formatter writes back unchanged.
fn arb_number() -> impl Strategy<Value = String> {
    prop::sample::select(&["0", "1", "2", "3", "0.5", "0.9", "0.99", "10", "1000"])
        .prop_map(String::from)
}

/// An instant-vector selector with no modifier on it.
///
/// Every label name in the matcher list is distinct. Two matchers on one label
/// are legal `PromQL`, but the formatter sorts them, and the AST holds the
/// matcher list in order, so a selector that repeats a label renders to text
/// that parses to an equal-but-reordered AST. That is normalization rather
/// than a round-trip failure, and
/// `repeated_matchers_on_one_label_reorder_but_survive` below pins it.
fn arb_bare_selector() -> impl Strategy<Value = String> {
    (
        prop::sample::select(METRICS),
        prop::sample::subsequence(LABELS.to_vec(), 0..4),
        prop::collection::vec(
            (
                prop::sample::select(&["=", "!=", "=~", "!~"]),
                prop::sample::select(&["api", "5..", "", "a|b"]),
            ),
            LABELS.len(),
        ),
    )
        .prop_map(|(metric, labels, specs)| {
            let mut out = String::from(metric);
            if !labels.is_empty() {
                let rendered: Vec<String> = labels
                    .iter()
                    .zip(specs)
                    .map(|(label, (op, value))| format!("{label}{op}\"{value}\""))
                    .collect();
                out.push('{');
                out.push_str(&rendered.join(", "));
                out.push('}');
            }
            out
        })
}

/// An instant-vector selector, optionally with an `offset` or `@` modifier.
fn arb_selector() -> impl Strategy<Value = String> {
    (
        arb_bare_selector(),
        prop::option::of(prop::sample::select(&[
            " offset 5m",
            " offset -5m",
            " @ 1609746000",
        ])),
    )
        .prop_map(|(selector, modifier)| format!("{selector}{}", modifier.unwrap_or("")))
}

/// A vector-matching clause, and the group modifier that only a non-set
/// operator accepts.
///
/// The matching label and the group label are always different, because
/// `PromQL` rejects a label that appears in both clauses.
fn arb_matching(allow_group: bool) -> impl Strategy<Value = String> {
    let group_kind = if allow_group {
        prop_oneof![
            3 => Just(None),
            1 => Just(Some("group_left")),
            1 => Just(Some("group_right")),
        ]
        .boxed()
    } else {
        Just(None).boxed()
    };
    prop_oneof![
        3 => Just(String::new()).boxed(),
        1 => (
            prop::sample::select(&["on", "ignoring"]),
            prop::sample::subsequence(LABELS.to_vec(), 2),
            group_kind,
        )
            .prop_map(|(kind, labels, group_kind)| {
                let group = group_kind
                    .map(|group| format!(" {group} ({})", labels[1]))
                    .unwrap_or_default();
                format!(" {kind} ({}){group}", labels[0])
            })
            .boxed(),
    ]
}

/// Joins two operands with a binary operator.
///
/// A vector-matching clause binds to one operator, so an unparenthesised
/// operand would re-associate around it and change which operands the clause
/// applies to. `up + on (code) 0 + up` reads as `(up + on (code) 0) + up`,
/// which matches a vector against a scalar, and `PromQL` rejects that. Adding
/// parentheses keeps the clause on the operands the generator chose for it.
/// An operator with no clause needs none, so those keep the plain form, which
/// is what exercises precedence.
fn binary(left: &str, op: &str, matching: &str, right: &str) -> String {
    if matching.is_empty() {
        format!("{left} {op} {right}")
    } else {
        format!("({left}) {op}{matching} ({right})")
    }
}

/// An instant-vector expression. Every branch keeps the `PromQL` type rules,
/// because an expression the parser rejects says nothing about rendering.
/// Depth is capped at four so a counterexample stays readable.
fn arb_vector() -> impl Strategy<Value = String> {
    arb_selector().prop_recursive(4, 32, 3, |inner| {
        let matrix = arb_matrix(inner.clone());
        prop_oneof![
            // Parentheses and unary minus.
            inner.clone().prop_map(|expr| format!("({expr})")),
            inner.clone().prop_map(|expr| format!("-{expr}")),
            // Instant-vector functions.
            (
                prop::sample::select(&["abs", "ceil", "floor", "sqrt", "exp", "ln", "log2"]),
                inner.clone(),
            )
                .prop_map(|(name, expr)| format!("{name}({expr})")),
            // Range-vector functions.
            (
                prop::sample::select(&[
                    "rate",
                    "irate",
                    "increase",
                    "delta",
                    "idelta",
                    "sum_over_time",
                    "avg_over_time",
                    "max_over_time",
                    "min_over_time",
                    "count_over_time",
                    "last_over_time",
                    "absent_over_time",
                ]),
                matrix,
            )
                .prop_map(|(name, range)| format!("{name}({range})")),
            // Aggregations, with and without a grouping clause.
            (
                prop::sample::select(&["sum", "avg", "min", "max", "count", "stddev", "group"]),
                prop_oneof![
                    2 => Just(String::new()),
                    1 => prop::sample::select(LABELS).prop_map(|label| format!(" by ({label})")),
                    1 => prop::sample::select(LABELS)
                        .prop_map(|label| format!(" without ({label})")),
                ],
                inner.clone(),
            )
                .prop_map(|(op, grouping, expr)| format!("{op}{grouping}({expr})")),
            // Parameterised aggregations.
            (
                prop::sample::select(&["topk", "bottomk"]),
                prop::sample::select(&["1", "3", "10"]),
                inner.clone(),
            )
                .prop_map(|(op, k, expr)| format!("{op}({k}, {expr})")),
            (
                prop::sample::select(&["quantile"]),
                prop::sample::select(&["0.5", "0.9", "0.99"]),
                inner.clone(),
            )
                .prop_map(|(op, quantile, expr)| format!("{op}({quantile}, {expr})")),
            (prop::sample::select(&["0.5", "0.9", "0.99"]), inner.clone(),)
                .prop_map(|(quantile, expr)| format!("histogram_quantile({quantile}, {expr})")),
            (
                prop::sample::select(&["clamp_min", "clamp_max"]),
                inner.clone(),
                arb_number(),
            )
                .prop_map(|(name, expr, bound)| format!("{name}({expr}, {bound})")),
            inner.clone().prop_map(|expr| format!(
                "label_replace({expr}, \"svc\", \"$1\", \"job\", \"(.*)\")"
            )),
            // Arithmetic between two vectors.
            (
                inner.clone(),
                prop::sample::select(&["+", "-", "*", "/", "%", "^"]),
                arb_matching(true),
                inner.clone(),
            )
                .prop_map(|(left, op, matching, right)| binary(&left, op, &matching, &right)),
            // Comparison between two vectors, with and without `bool`.
            (
                inner.clone(),
                prop::sample::select(&["==", "!=", ">", ">=", "<", "<="]),
                prop::sample::select(&["", " bool"]),
                arb_matching(true),
                inner.clone(),
            )
                .prop_map(|(left, op, bool_modifier, matching, right)| binary(
                    &left,
                    &format!("{op}{bool_modifier}"),
                    &matching,
                    &right
                )),
            // Set operations, which take no group modifier.
            (
                inner.clone(),
                prop::sample::select(&["and", "or", "unless"]),
                arb_matching(false),
                inner.clone(),
            )
                .prop_map(|(left, op, matching, right)| binary(&left, op, &matching, &right)),
            // Arithmetic between a vector and a scalar, both ways round.
            (
                inner.clone(),
                prop::sample::select(&["+", "-", "*", "/"]),
                arb_number(),
            )
                .prop_map(|(left, op, right)| format!("{left} {op} {right}")),
            (
                arb_number(),
                prop::sample::select(&["+", "-", "*", "/"]),
                inner,
            )
                .prop_map(|(left, op, right)| format!("{left} {op} {right}")),
        ]
    })
}

/// A range vector: a range selector, or a subquery over an instant vector.
fn arb_matrix(vector: impl Strategy<Value = String> + 'static) -> BoxedStrategy<String> {
    // A modifier goes after the range, never before it: `up offset 5m[5m]` is
    // not `PromQL`.
    let modifier = prop::option::of(prop::sample::select(&[
        " offset 5m",
        " offset -5m",
        " @ 1609746000",
    ]));
    prop_oneof![
        3 => (arb_bare_selector(), prop::sample::select(RANGES), modifier)
            .prop_map(|(selector, range, modifier)| format!(
                "{selector}[{range}]{}",
                modifier.unwrap_or("")
            )),
        1 => (vector, prop::sample::select(&["30m:1m", "1h:5m"]))
            .prop_map(|(expr, subquery)| format!("({expr})[{subquery}]")),
    ]
    .boxed()
}

/// The formatter sorts a selector's matchers, and the AST holds them in a
/// `Vec`, so a selector written out of order renders to text whose AST carries
/// the matchers in a different order. The meaning is the same, since matchers
/// are conjoined, and the text is stable once it has been through the
/// formatter. The generator above emits matchers already sorted, which is why
/// the round-trip property can compare ASTs exactly.
#[test]
fn the_formatter_sorts_a_selectors_matchers() {
    for (query, want) in [
        (r#"up{job="api", job="5.."}"#, r#"up{job="5..",job="api"}"#),
        (
            r#"up{job="api", instance="a"}"#,
            r#"up{instance="a",job="api"}"#,
        ),
    ] {
        let rendered = parse_promql(query).expect("the query parses").to_string();
        let reparsed = parse_promql(&rendered).expect("the rendered form parses");

        assert2::assert!(rendered == want);
        assert2::assert!(reparsed.to_string() == rendered);
    }
}

/// The seeds carry the mutation strategy, so a typo in one would quietly
/// narrow what the totality property covers.
#[test]
fn the_seed_queries_are_valid_promql() {
    for query in SEED_QUERIES {
        assert2::assert!(parse_promql(query).is_ok(), "seed query: {query}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// No string panics the parser, whichever duration context it runs under.
    /// A range context is the interesting one, because `step()`, `start()` and
    /// `end()` fold to real values only there.
    #[test]
    fn parse_promql_never_panics(query in arbitrary_query()) {
        let _ = parse_promql(&query);
        let _ = parse_promql_with_duration_context(&query, DurationExprContext::instant(0));
        let _ = parse_promql_with_duration_context(
            &query,
            DurationExprContext::range(1_600_000_000_000, 1_600_000_600_000, secs(15)),
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Rendering a parsed query and parsing the result yields the same AST.
    #[test]
    fn parsing_a_rendered_query_returns_the_same_ast(query in arb_vector()) {
        let parsed = parse_promql(&query)
            .map_err(|error| TestCaseError::fail(format!("{query} does not parse: {error}")))?;

        let rendered = parsed.to_string();
        let reparsed = parse_promql(&rendered).map_err(|error| {
            TestCaseError::fail(format!("{query} renders to {rendered}, which does not parse: {error}"))
        })?;

        prop_assert!(
            reparsed == parsed,
            "{query} renders to {rendered}, which parses to a different AST",
        );
    }

    /// Rendering is idempotent: a second render produces the text the first
    /// one did.
    #[test]
    fn rendering_a_parsed_query_is_idempotent(query in arb_vector()) {
        let parsed = parse_promql(&query)
            .map_err(|error| TestCaseError::fail(format!("{query} does not parse: {error}")))?;

        let once = parsed.to_string();
        let twice = parse_promql(&once)
            .map_err(|error| TestCaseError::fail(format!("{once} does not parse: {error}")))?
            .to_string();

        prop_assert_eq!(twice, once);
    }
}
