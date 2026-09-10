//! Property tests for the `LogQL` stream-query engine.
//!
//! Every other property test in this repository stops before evaluation.
//! `krabka-logql`'s `parser_proptest` checks that the parser is total, and
//! that an expression survives a trip through its own rendering. Its
//! `planner_proptest` checks that series pruning and block pruning agree with
//! a full scan over the two indexes. `fuzz/fuzz_targets/logql_parse.rs` fuzzes
//! the same parser with bytes. None of them reads a row.
//!
//! These properties read rows. Each one writes real Parquet blocks, plans a
//! query over them, and runs it. The answer goes against the rows the `LogQL`
//! specification keeps for that selector and that line-filter chain.
//!
//! The expected side never runs the engine a second time. It is a fold over
//! the generated series and lines with `str::contains`, which is what Loki
//! defines `|=` and `!=` to mean. The engine side takes a longer path: posting
//! list pruning in `plan_stream_query`, block pruning by time range, a
//! `DataFusion` scan that carries the leading line filters as `LIKE`
//! predicates, and a second row by row pass in Rust. A fault anywhere on that
//! path moves the engine side alone.
//!
//! The generators aim at four faults that path can have.
//!
//! - A literal that holds `%` or `_` becomes a `LIKE` wildcard unless
//!   `sql_like_pattern_literal` escapes it. The answer then gets wider. Both
//!   characters are in the literal alphabet.
//! - `not like` in SQL is three-valued. A negated filter that reaches the scan
//!   unguarded can drop rows instead of keeping them. The generator writes
//!   negated filters freely.
//! - Block pruning and the scan's time predicate are both inclusive at each
//!   end. The generator cuts blocks at one set of bounds and picks the query
//!   window from another, so a row on an edge is a common case.
//! - `|~` compiles the pattern with the `regex` crate on one path, and hands
//!   it to `regexp_like` on the other. Over an escaped literal both paths must
//!   agree with `str::contains`.
//!
//! Two things stay out of scope on purpose. Every generated series carries
//! both label names, so Loki's rule that a missing label matches the empty
//! string never applies, and the oracle does not model it. Timestamps are
//! unique inside a series, so the order of two entries that share a timestamp
//! never decides a comparison. Loki does not specify that order.

use std::collections::{BTreeMap, BTreeSet};

use assert2::assert;
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, write_log_block,
};
use krabka_logql::{parse_query, plan_stream_query};
use krabka_observability::execute_stream_query;
use proptest::prelude::*;
use serde_json::Value;

/// The tenant every generated block, series and query belongs to.
const TENANT: &str = "tenant-a";

/// The values the `app` label takes.
const APPS: &[&str] = &["api", "worker", "db"];

/// The values the `env` label takes.
const ENVS: &[&str] = &["prod", "dev"];

/// The characters a generated log line is built from.
///
/// `%` and `_` are the SQL `LIKE` wildcards, and the rest are `RE2`
/// metacharacters, so a literal drawn from this alphabet is exactly the kind
/// that an unescaped pushdown mistakes for a pattern. `é` is here so a
/// multi-byte character crosses the Parquet round trip and the substring
/// search.
const LINE_CHARS: &[char] = &[
    'a', 'b', '%', '_', '.', '*', '\\', '[', ']', '(', ')', '|', '^', '$', '+', '?', '-', '{', '}',
    '"', ' ', 'é',
];

/// The characters a generated line-filter literal is built from.
///
/// A narrower alphabet than the lines themselves, so that a short literal hits
/// often enough for the property to be about matching rather than about the
/// empty answer.
const LITERAL_CHARS: &[char] = &['a', 'b', '%', '_', '.', '*', '\\', 'é'];

/// The highest timestamp a generated row can carry.
const MAX_TIMESTAMP_NS: i64 = 63;

/// One generated series, identified by the two labels every series here has.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Series {
    app: String,
    env: String,
}

impl Series {
    /// The series as the label map the block store indexes it under.
    fn labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("app".to_string(), self.app.clone()),
            ("env".to_string(), self.env.clone()),
        ])
    }
}

/// One generated log line, tied to a series by index.
#[derive(Clone, Debug)]
struct Row {
    series: usize,
    timestamp_ns: i64,
    line: String,
}

/// One generated label matcher, over `app` or `env`, equal or not equal.
#[derive(Clone, Debug)]
struct Matcher {
    name: &'static str,
    negated: bool,
    value: String,
}

impl Matcher {
    /// Whether the matcher keeps `series`, by the `LogQL` definition of `=`
    /// and `!=` on a label the series has.
    fn keeps(&self, series: &Series) -> bool {
        let actual = match self.name {
            "app" => &series.app,
            _ => &series.env,
        };
        (actual == &self.value) != self.negated
    }

    /// The matcher as `LogQL` selector text.
    fn render(&self) -> String {
        let op = if self.negated { "!=" } else { "=" };
        format!("{}{op}\"{}\"", self.name, self.value)
    }
}

/// One generated line filter.
#[derive(Clone, Debug)]
struct Filter {
    negated: bool,
    literal: String,
}

impl Filter {
    /// Whether the filter keeps `line`. Loki defines `|=` as "the line
    /// contains this string" and `!=` as its negation, so this is
    /// `str::contains`.
    fn keeps(&self, line: &str) -> bool {
        line.contains(&self.literal) != self.negated
    }

    /// The filter as `LogQL` text, using the literal operators.
    ///
    /// The literal goes in backticks, which `LogQL` reads verbatim, so a
    /// backslash in it stays one backslash and no escaping layer sits between
    /// the generated literal and the one the engine filters with.
    fn render_literal(&self) -> String {
        let op = if self.negated { "!=" } else { "|=" };
        format!("{op} `{}`", self.literal)
    }

    /// The same filter written as a regex over the escaped literal.
    ///
    /// `|~` is an unanchored `RE2` match, so a pattern that escapes every
    /// metacharacter of the literal matches exactly the lines containing it.
    fn render_regex(&self) -> String {
        let op = if self.negated { "!~" } else { "|~" };
        format!("{op} `{}`", regex::escape(&self.literal))
    }
}

/// One generated scenario: the data, the query and the window to run it over.
#[derive(Clone, Debug)]
struct Case {
    series: Vec<Series>,
    rows: Vec<Row>,
    matchers: Vec<Matcher>,
    filters: Vec<Filter>,
    query_start_ns: i64,
    query_end_ns: i64,
    block_count: usize,
}

impl Case {
    /// The matcher set as a `LogQL` stream selector.
    fn selector(&self) -> String {
        let matchers = self
            .matchers
            .iter()
            .map(Matcher::render)
            .collect::<Vec<_>>()
            .join(", ");
        format!("{{{matchers}}}")
    }

    /// The query with its line filters written as literal filters.
    fn literal_query(&self) -> String {
        self.query_with(Filter::render_literal)
    }

    /// The query with its line filters written as regexes over the escaped
    /// literals.
    fn regex_query(&self) -> String {
        self.query_with(Filter::render_regex)
    }

    fn query_with(&self, render: impl Fn(&Filter) -> String) -> String {
        let mut query = self.selector();
        for filter in &self.filters {
            query.push(' ');
            query.push_str(&render(filter));
        }
        query
    }

    /// The rows the specification keeps, grouped into streams the way a Loki
    /// `streams` response groups them: one stream per series, entries in
    /// ascending timestamp order.
    ///
    /// This is the oracle. It reads the generated data directly and never
    /// calls the engine.
    fn expected_streams(&self) -> BTreeMap<BTreeMap<String, String>, Vec<(String, String)>> {
        let mut streams: BTreeMap<BTreeMap<String, String>, Vec<(String, String)>> =
            BTreeMap::new();
        for row in &self.rows {
            let series = &self.series[row.series];
            if !self.matchers.iter().all(|matcher| matcher.keeps(series)) {
                continue;
            }
            if row.timestamp_ns < self.query_start_ns || row.timestamp_ns > self.query_end_ns {
                continue;
            }
            if !self.filters.iter().all(|filter| filter.keeps(&row.line)) {
                continue;
            }
            streams
                .entry(series.labels())
                .or_default()
                .push((row.timestamp_ns.to_string(), row.line.clone()));
        }
        for entries in streams.values_mut() {
            entries.sort_by_key(|(timestamp, _)| timestamp.parse::<i64>().unwrap_or(i64::MAX));
        }
        streams
    }

    /// The time range each generated block covers, cut so the blocks partition
    /// `0..=MAX_TIMESTAMP_NS` into `block_count` contiguous pieces.
    fn block_ranges(&self) -> Vec<(i64, i64)> {
        let count = i64::try_from(self.block_count).unwrap_or(1).max(1);
        let width = (MAX_TIMESTAMP_NS + 1) / count;
        (0..count)
            .map(|index| {
                let start = index * width;
                let end = if index + 1 == count {
                    MAX_TIMESTAMP_NS
                } else {
                    start + width - 1
                };
                (start, end)
            })
            .collect()
    }
}

fn arb_line() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(LINE_CHARS), 1..=6)
        .prop_map(|chars| chars.into_iter().collect())
}

fn arb_literal() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(LITERAL_CHARS), 1..=2)
        .prop_map(|chars| chars.into_iter().collect())
}

fn arb_matchers() -> impl Strategy<Value = Vec<Matcher>> {
    (
        prop::sample::select(APPS),
        prop::option::of((prop::sample::select(ENVS), any::<bool>())),
        any::<bool>(),
    )
        .prop_map(|(app, env, app_negated)| {
            // Loki rejects a selector whose every matcher matches the empty
            // string, and `!=` does match it. At least one matcher here is
            // therefore an equality, so the parser accepts what the generator
            // writes and no case is thrown away.
            let env_negated = env.is_some_and(|(_, negated)| negated);
            let mut matchers = vec![Matcher {
                name: "app",
                negated: app_negated && env.is_some() && !env_negated,
                value: (*app).to_string(),
            }];
            if let Some((env, negated)) = env {
                matchers.push(Matcher {
                    name: "env",
                    negated,
                    value: (*env).to_string(),
                });
            }
            matchers
        })
}

fn arb_case() -> impl Strategy<Value = Case> {
    let series = prop::collection::btree_set(
        (prop::sample::select(APPS), prop::sample::select(ENVS)).prop_map(|(app, env)| Series {
            app: (*app).to_string(),
            env: (*env).to_string(),
        }),
        1..=4,
    )
    .prop_map(|set| set.into_iter().collect::<Vec<_>>());

    (
        series,
        prop::collection::vec((0usize..4, 0i64..=MAX_TIMESTAMP_NS, arb_line()), 1..=12),
        arb_matchers(),
        prop::collection::vec((any::<bool>(), arb_literal()), 0..=3),
        (
            (
                any::<bool>(),
                any::<prop::sample::Index>(),
                0i64..=MAX_TIMESTAMP_NS,
            ),
            (
                any::<bool>(),
                any::<prop::sample::Index>(),
                0i64..=MAX_TIMESTAMP_NS,
            ),
        ),
        1usize..=3,
    )
        .prop_map(
            |(series, rows, matchers, filters, (first_bound, second_bound), block_count)| {
                // A row names a series by index, so fold the index into range,
                // then drop the duplicates that would leave two entries in one
                // stream sharing a timestamp.
                let mut seen = BTreeSet::new();
                let rows: Vec<Row> = rows
                    .into_iter()
                    .map(|(series_index, timestamp_ns, line)| Row {
                        series: series_index % series.len(),
                        timestamp_ns,
                        line,
                    })
                    .filter(|row| seen.insert((row.series, row.timestamp_ns)))
                    .collect();

                // Half the window bounds land on a row's own timestamp. Both
                // ends of a query window are inclusive, so a bound that sits
                // one nanosecond off a row says nothing about the edge. Drawn
                // uniformly, a bound would hit a row only about one time in
                // sixteen.
                let snap = |(on, index, free): (bool, prop::sample::Index, i64)| {
                    if on && !rows.is_empty() {
                        rows[index.index(rows.len())].timestamp_ns
                    } else {
                        free
                    }
                };
                let first_bound = snap(first_bound);
                let second_bound = snap(second_bound);

                Case {
                    series,
                    rows,
                    matchers,
                    filters: filters
                        .into_iter()
                        .map(|(negated, literal)| Filter { negated, literal })
                        .collect(),
                    query_start_ns: first_bound.min(second_bound),
                    query_end_ns: first_bound.max(second_bound),
                    block_count,
                }
            },
        )
}

/// Writes `case` as Parquet blocks, plans `query` over them and runs it,
/// returning the Loki response the engine produced.
fn run_query(case: &Case, query: &str) -> Value {
    let dir = tempfile::tempdir().expect("temp dir");

    let mut label_index = LabelIndex::default();
    let fingerprints = case
        .series
        .iter()
        .map(|series| label_index.insert_series(TENANT, series.labels()))
        .collect::<Vec<_>>();

    let mut block_index = BlockIndex::default();
    for (block_id, (start_ns, end_ns)) in case.block_ranges().into_iter().enumerate() {
        let rows = case
            .rows
            .iter()
            .filter(|row| row.timestamp_ns >= start_ns && row.timestamp_ns <= end_ns)
            .map(|row| {
                LogRow::new(
                    fingerprints[row.series],
                    row.timestamp_ns,
                    row.line.clone(),
                    BTreeMap::new(),
                )
            })
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        let partition = i32::try_from(block_id).expect("block id fits a partition");
        let key = BlockKey::new(
            TENANT,
            partition,
            start_ns,
            end_ns,
            TimeRange::new(start_ns, end_ns).expect("block range"),
        );
        block_index.insert(write_log_block(dir.path(), &key, rows).expect("write block"));
    }

    let plan = plan_stream_query(
        TENANT,
        TimeRange::new(case.query_start_ns, case.query_end_ns).expect("query range"),
        parse_query(query).expect("generated query parses"),
        &label_index,
        &block_index,
    )
    .expect("generated query plans");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime
        .block_on(execute_stream_query(dir.path(), &plan, &label_index))
        .expect("query executes")
}

/// Reshapes a Loki `streams` response into the oracle's shape.
///
/// `detected_level` is dropped here and checked on its own, by
/// [`every_returned_stream_carries_the_unknown_detected_level`], so that a
/// change to the synthetic label does not read as a change to the rows.
fn streams_from_response(
    response: &Value,
) -> BTreeMap<BTreeMap<String, String>, Vec<(String, String)>> {
    let mut streams = BTreeMap::new();
    let results = response["data"]["result"]
        .as_array()
        .expect("a streams result array");
    for result in results {
        let labels = result["stream"]
            .as_object()
            .expect("a stream label map")
            .iter()
            .filter(|(name, _)| name.as_str() != "detected_level")
            .map(|(name, value)| {
                (
                    name.clone(),
                    value.as_str().expect("a string label value").to_string(),
                )
            })
            .collect::<BTreeMap<String, String>>();
        let entries = result["values"]
            .as_array()
            .expect("a values array")
            .iter()
            .map(|entry| {
                (
                    entry[0].as_str().expect("a timestamp").to_string(),
                    entry[1].as_str().expect("a line").to_string(),
                )
            })
            .collect::<Vec<_>>();
        streams.insert(labels, entries);
    }
    streams
}

/// The generators carry all three properties. A query shape that did not parse
/// would turn every case into a panic inside the fixture, and the properties
/// would then state nothing about the engine.
#[test]
fn the_generated_query_shapes_are_valid_logql() {
    let cases = [
        r#"{app="api"}"#,
        r#"{app!="api", env="prod"}"#,
        r#"{app="api", env!="prod"}"#,
        r#"{app="api", env!="dev"}"#,
        r#"{app="api"} |= `a%b` != `\` |= `é`"#,
        r#"{app="api"} |~ `a%b` !~ `\\`"#,
    ];
    for query in cases {
        assert!(parse_query(query).is_ok(), "query: {query}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// A stream query returns exactly the rows the selector, the query window
    /// and the line-filter chain keep, in ascending timestamp order.
    ///
    /// `LogQL` defines a matcher set as a conjunction, a query window as
    /// inclusive at both ends, `|=` as `str::contains`, and `!=` as its
    /// negation. The expected value folds those four rules over the generated
    /// series and lines. Nothing on that side runs the engine. So a pushdown
    /// that reads a `%` in a literal as a wildcard moves the engine's answer
    /// alone, and so does a `not like` that loses rows to SQL's three-valued
    /// logic, and so does a block that pruning drops one nanosecond early.
    #[test]
    fn a_stream_query_returns_exactly_the_rows_the_specification_keeps(case in arb_case()) {
        let query = case.literal_query();
        let actual = streams_from_response(&run_query(&case, &query));
        prop_assert_eq!(actual, case.expected_streams(), "query: {}", query);
    }

    /// A regex filter over the escaped literal keeps the same lines the
    /// literal filter keeps.
    ///
    /// `|~` is an unanchored `RE2` match. A pattern that escapes every
    /// metacharacter of a literal therefore matches the lines that hold that
    /// literal, and no others. `!~` is its negation.
    ///
    /// The comparison uses the same `str::contains` oracle that the previous
    /// property uses. It does not compare the regex filter against the literal
    /// filter's own answer, so each operator is pinned to the specification
    /// and not to the other one.
    ///
    /// The two operators take different paths. A literal filter becomes an
    /// escaped `LIKE` in the scan. A regex filter becomes `regexp_like` only
    /// where `regex_line_filter_is_pushdown_safe` permits it, and stays in
    /// Rust everywhere else. This property fails on an anchoring mistake, on a
    /// lost escape, and on a pushdown that rewrites the call into something
    /// that is not a regex.
    #[test]
    fn a_regex_filter_over_an_escaped_literal_keeps_the_same_lines(case in arb_case()) {
        let query = case.regex_query();
        let actual = streams_from_response(&run_query(&case, &query));
        prop_assert_eq!(actual, case.expected_streams(), "query: {}", query);
    }

    /// Every returned stream carries `detected_level`, and it is `unknown`
    /// while nothing supplies a level.
    ///
    /// Loki adds the label at query time to a stream that carries no level of
    /// its own. Grafana's logs panel groups on it, so a stream that comes back
    /// without it is a visible fault, not a cosmetic one.
    ///
    /// No generated series carries `level`, `severity`, `severity_text` or
    /// `detected_level`, and no generated query has a `keep` stage. `unknown`
    /// is therefore the only value the specification allows here. The other
    /// two properties drop this label before they compare rows, which is why
    /// it needs a property of its own.
    #[test]
    fn every_returned_stream_carries_the_unknown_detected_level(case in arb_case()) {
        let query = case.literal_query();
        let response = run_query(&case, &query);

        for result in response["data"]["result"].as_array().expect("results") {
            prop_assert_eq!(
                result["stream"]["detected_level"].as_str(),
                Some("unknown"),
                "query: {}",
                query
            );
        }
    }
}
