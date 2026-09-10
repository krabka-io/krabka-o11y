//! Property tests for the `TraceQL` search engine over stored spans.
//!
//! The other generative tests in this crate stop at the grammar.
//! `tests/parser_proptest.rs` proves that `lex` and `parse` are total and that
//! a rendered query round-trips through the parser, and
//! `fuzz/fuzz_targets/traceql_parse.rs` drives `parse` with arbitrary bytes.
//! Neither one runs a query. `tests/golden_corpus.rs` does run queries, but
//! only the fixed cases the vendored corpus spells out, and only against the
//! one hand-built fixture trace tree the testkit supplies.
//!
//! These are the first properties here that evaluate a `TraceQL` query over
//! spans. Each case builds a small store, renders a query, and checks the
//! spans the engine returns against an answer computed from the generated
//! spans in plain Rust. The engine side of that comparison runs the whole
//! path: parse, matcher lowering, the pushdown the store applies while it
//! scans, the `DataFusion` plan, and span-set assembly. The expected side
//! touches none of it.
//!
//! The generated space is deliberately small and closed -- a handful of span
//! names, services, durations, statuses, and one integer attribute -- so the
//! oracle stays a few lines of filtering and a shrunk counterexample can be
//! read.
//!
//! The condition spellings are the ones the golden corpus in
//! `tests/testdata/traceql/` uses: `span:name`, `span:duration` with a `ms`
//! suffix, `span:status` against the `unset`/`ok`/`error` enum,
//! `resource.service.name`, and an unscoped `.code` attribute.

use std::sync::Arc;

use krabka_traceql::{
    AttrValue, EngineOpts, InMemorySpanStore, InputSpan, SearchOptions, SearchResponse,
    TraceResult, TraceqlEngine, TraceqlError,
};
use krabka_units::{Time, convert::TimeExt as _};
use proptest::prelude::*;

/// Span names the generator draws from. `span:name` compares against these.
const NAMES: [&str; 3] = ["alpha", "beta", "gamma"];

/// Service names the generator draws from. The in-memory store models
/// `service.name` as a per-trace resource attribute, so a trace picks one and
/// every span in it reports that value for `resource.service.name`.
const SERVICES: [&str; 3] = ["cart", "checkout", "payments"];

/// Span durations, in whole milliseconds, so every query literal is a round
/// `<n>ms` and no rounding sits between the query and the oracle.
const DURATION_MILLIS: [i64; 4] = [1, 5, 10, 25];

/// `span:status` enum names, indexed by the status code each one names.
const STATUS_NAMES: [&str; 3] = ["unset", "ok", "error"];

/// Values of the `.code` span attribute. Every generated span carries the
/// attribute, so a `!=` condition never has to decide what an absent
/// attribute means.
const CODES: [i64; 3] = [1, 2, 3];

/// Largest span count a generated trace can hold. Span ids are laid out with
/// this stride, so no two spans in a store share one.
const SPANS_PER_TRACE: usize = 4;

/// Tenant every case writes to and reads from.
const TENANT: &str = "t";

/// A limit above the largest result any generator can produce, so a search
/// that passes it is effectively unlimited. It also stands in for the
/// spans-per-span-set cap, which defaults to 3 and would otherwise truncate a
/// four-span trace.
const NO_LIMIT: usize = 64;

/// Start of the search window.
const SEARCH_START_NS: i64 = 0;

/// End of the search window. Trace start times are one second apart, starting
/// at one second, so every generated trace falls inside it.
const SEARCH_END_NS: i64 = 1_000_000_000_000;

/// One generated span. The fields are exactly the ones a generated condition
/// can mention.
#[derive(Clone, Debug)]
struct GenSpan {
    name: &'static str,
    duration_millis: i64,
    status_code: i32,
    code: i64,
}

/// One generated trace: a service and its spans.
#[derive(Clone, Debug)]
struct GenTrace {
    service: &'static str,
    spans: Vec<GenSpan>,
}

/// The comparison operators a numeric condition can use.
#[derive(Clone, Copy, Debug)]
enum NumOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl NumOp {
    /// The `TraceQL` spelling of the operator.
    fn text(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
        }
    }

    /// Whether `lhs <op> rhs` holds, evaluated in plain Rust.
    fn holds(self, lhs: i64, rhs: i64) -> bool {
        match self {
            Self::Eq => lhs == rhs,
            Self::Ne => lhs != rhs,
            Self::Gt => lhs > rhs,
            Self::Gte => lhs >= rhs,
            Self::Lt => lhs < rhs,
            Self::Lte => lhs <= rhs,
        }
    }
}

/// One primitive condition inside a spanset filter.
#[derive(Clone, Copy, Debug)]
enum Cond {
    Name { negated: bool, value: &'static str },
    Service { negated: bool, value: &'static str },
    Status { negated: bool, code: usize },
    Duration { op: NumOp, millis: i64 },
    Code { op: NumOp, value: i64 },
}

/// The `TraceQL` spelling of `=` or `!=`.
fn eq_text(negated: bool) -> &'static str {
    if negated { "!=" } else { "=" }
}

impl Cond {
    /// The condition as `TraceQL` text, without the enclosing braces.
    fn text(self) -> String {
        match self {
            Self::Name { negated, value } => {
                format!("span:name {} \"{value}\"", eq_text(negated))
            }
            Self::Service { negated, value } => {
                format!("resource.service.name {} \"{value}\"", eq_text(negated))
            }
            Self::Status { negated, code } => {
                format!("span:status {} {}", eq_text(negated), STATUS_NAMES[code])
            }
            Self::Duration { op, millis } => format!("span:duration {} {millis}ms", op.text()),
            Self::Code { op, value } => format!(".code {} {value}", op.text()),
        }
    }

    /// Whether the condition holds for `span`, which belongs to `trace`.
    fn holds(self, trace: &GenTrace, span: &GenSpan) -> bool {
        match self {
            Self::Name { negated, value } => (span.name == value) != negated,
            Self::Service { negated, value } => (trace.service == value) != negated,
            Self::Status { negated, code } => {
                (usize::try_from(span.status_code).unwrap_or(usize::MAX) == code) != negated
            }
            Self::Duration { op, millis } => op.holds(span.duration_millis, millis),
            Self::Code { op, value } => op.holds(span.code, value),
        }
    }
}

/// Renders a conjunction of conditions as one spanset filter.
fn and_query(conds: &[Cond]) -> String {
    let body: Vec<String> = conds.iter().copied().map(Cond::text).collect();
    format!("{{ {} }}", body.join(" && "))
}

fn num_op() -> impl Strategy<Value = NumOp> {
    prop_oneof![
        Just(NumOp::Eq),
        Just(NumOp::Ne),
        Just(NumOp::Gt),
        Just(NumOp::Gte),
        Just(NumOp::Lt),
        Just(NumOp::Lte),
    ]
}

fn cond() -> impl Strategy<Value = Cond> {
    prop_oneof![
        (any::<bool>(), 0..NAMES.len()).prop_map(|(negated, i)| Cond::Name {
            negated,
            value: NAMES[i]
        }),
        (any::<bool>(), 0..SERVICES.len()).prop_map(|(negated, i)| Cond::Service {
            negated,
            value: SERVICES[i]
        }),
        (any::<bool>(), 0..STATUS_NAMES.len())
            .prop_map(|(negated, code)| Cond::Status { negated, code }),
        (num_op(), 0..DURATION_MILLIS.len()).prop_map(|(op, i)| Cond::Duration {
            op,
            millis: DURATION_MILLIS[i]
        }),
        (num_op(), 0..CODES.len()).prop_map(|(op, i)| Cond::Code {
            op,
            value: CODES[i]
        }),
    ]
}

fn conds() -> impl Strategy<Value = Vec<Cond>> {
    prop::collection::vec(cond(), 1..=3)
}

fn gen_span() -> impl Strategy<Value = GenSpan> {
    (
        0..NAMES.len(),
        0..DURATION_MILLIS.len(),
        0..STATUS_NAMES.len(),
        0..CODES.len(),
    )
        .prop_map(|(name, duration, status, code)| GenSpan {
            name: NAMES[name],
            duration_millis: DURATION_MILLIS[duration],
            status_code: i32::try_from(status).unwrap_or(0),
            code: CODES[code],
        })
}

fn gen_trace() -> impl Strategy<Value = GenTrace> {
    (
        0..SERVICES.len(),
        prop::collection::vec(gen_span(), 1..=SPANS_PER_TRACE),
    )
        .prop_map(|(service, spans)| GenTrace {
            service: SERVICES[service],
            spans,
        })
}

fn gen_traces() -> impl Strategy<Value = Vec<GenTrace>> {
    prop::collection::vec(gen_trace(), 2..=6)
}

/// Trace ids are `[n; 16]` with `n` one-based, so a trace is identified by its
/// first byte and no two generated traces collide.
fn trace_id(trace_ix: usize) -> [u8; 16] {
    [u8::try_from(trace_ix + 1).unwrap_or(u8::MAX); 16]
}

/// Span ids are `[n; 8]`, laid out with a per-trace stride, so a span is
/// identified by its first byte across the whole store.
fn span_id(trace_ix: usize, span_ix: usize) -> [u8; 8] {
    [u8::try_from(trace_ix * SPANS_PER_TRACE + span_ix + 1).unwrap_or(u8::MAX); 8]
}

/// Builds an engine over an in-memory store holding the generated traces.
///
/// Span 0 is the trace root and the rest are its children, which is the shape
/// the nested-set assignment expects. None of the properties here use a
/// structural operator, but the store still has to be handed a well-formed
/// tree.
fn engine_over(traces: &[GenTrace]) -> TraceqlEngine<InMemorySpanStore> {
    let mut store = InMemorySpanStore::new();
    for (t, trace) in traces.iter().enumerate() {
        let base_nano = (i64::try_from(t).unwrap_or(0) + 1) * 1_000_000_000;
        let spans: Vec<InputSpan> = trace
            .spans
            .iter()
            .enumerate()
            .map(|(s, span)| InputSpan {
                trace_id: trace_id(t),
                span_id: span_id(t, s),
                parent_span_id: if s == 0 { None } else { Some(span_id(t, 0)) },
                name: span.name.to_owned(),
                kind: 0,
                start_unix_nano: base_nano + i64::try_from(s).unwrap_or(0),
                duration: Time::from_millis(span.duration_millis),
                status_code: span.status_code,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                attrs: vec![("code".to_owned(), AttrValue::Int(span.code))],
                events: Vec::new(),
                links: Vec::new(),
            })
            .collect();
        store.push_trace(TENANT, trace.service, trace.spans[0].name, spans);
    }
    TraceqlEngine::new(Arc::new(store), EngineOpts::default())
}

/// Every `(trace_id, span_id)` in a response, as the first byte of each,
/// sorted. The store writes both ids as a repeated byte, so the first byte
/// names the span.
fn span_keys(resp: &SearchResponse) -> Vec<(u8, u8)> {
    let mut keys: Vec<(u8, u8)> = resp
        .traces
        .iter()
        .flat_map(|trace| {
            trace.span_sets.iter().flat_map(move |set| {
                set.spans
                    .iter()
                    .map(move |span| (trace.trace_id[0], span.span_id[0]))
            })
        })
        .collect();
    keys.sort_unstable();
    keys
}

/// Runs a search with the limits raised above anything a generated store can
/// produce, so nothing is dropped by truncation.
async fn search_unlimited(
    engine: &TraceqlEngine<InMemorySpanStore>,
    query: &str,
) -> Result<SearchResponse, TraceqlError> {
    engine
        .search_with_options(
            TENANT,
            query,
            SEARCH_START_NS,
            SEARCH_END_NS,
            SearchOptions {
                limit: NO_LIMIT,
                spss: NO_LIMIT,
                ..SearchOptions::default()
            },
        )
        .await
}

/// Runs a search and reduces it to the sorted set of spans it matched.
async fn matched_spans(
    engine: &TraceqlEngine<InMemorySpanStore>,
    query: &str,
) -> Result<Vec<(u8, u8)>, TraceqlError> {
    Ok(span_keys(&search_unlimited(engine, query).await?))
}

/// The spans that satisfy every condition, computed from the generated
/// structs alone.
fn expected_spans(traces: &[GenTrace], conds: &[Cond]) -> Vec<(u8, u8)> {
    let mut keys: Vec<(u8, u8)> = Vec::new();
    for (t, trace) in traces.iter().enumerate() {
        for (s, span) in trace.spans.iter().enumerate() {
            if conds.iter().all(|c| c.holds(trace, span)) {
                keys.push((trace_id(t)[0], span_id(t, s)[0]));
            }
        }
    }
    keys.sort_unstable();
    keys
}

/// Turns a query failure into a property failure that names the query.
fn or_fail<T>(result: Result<T, TraceqlError>, query: &str) -> Result<T, TestCaseError> {
    result.map_err(|err| TestCaseError::fail(format!("query {query} failed: {err}")))
}

/// A single-threaded runtime for one property case.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// A spanset filter returns exactly the spans that satisfy its conditions.
    ///
    /// This is the defining behaviour of `{ ... }` in `TraceQL`: the filter is
    /// a per-span predicate, and `&&` inside one filter requires a single span
    /// to satisfy every operand. `tests/golden_queries.rs` pins that for one
    /// pair of conditions in `single_span_and_differs_from_inter_brace_and`;
    /// this generalises it over the operand set.
    ///
    /// It is not a tautology. The expected side filters the generated span
    /// structs directly and never enters the crate, while the engine side
    /// parses the rendered query, lowers each condition to a `SpanMatcher`,
    /// lets the store apply those matchers as pushdown while it scans, runs a
    /// `DataFusion` plan over the surviving rows, and assembles span sets from
    /// the result batches. A pushdown that widens or narrows a comparison, an
    /// operator mapped to its inverse, or a `ms` literal read as nanoseconds
    /// moves only the engine side.
    #[test]
    fn a_spanset_filter_selects_exactly_the_matching_spans(
        (traces, conds) in (gen_traces(), conds()),
    ) {
        let engine = engine_over(&traces);
        let query = and_query(&conds);
        let want = expected_spans(&traces, &conds);

        let got = or_fail(runtime().block_on(matched_spans(&engine, &query)), &query)?;
        prop_assert_eq!(got, want, "query: {}", query);
    }

    /// `&&` between two conditions is set intersection, and `||` is set union.
    ///
    /// This is the specified meaning of the two boolean operators inside a
    /// spanset filter: both operands are evaluated against the same span, so
    /// a conjunction matches the spans both operands match and a disjunction
    /// matches the spans either operand matches.
    ///
    /// Unlike the property above, this one is relational. It constrains four
    /// engine runs against each other rather than against an independent
    /// oracle, and it is worth stating why that still has content. Nothing in
    /// the engine computes `{ A && B }` by intersecting the results of `{ A }`
    /// and `{ B }`: the two conditions are lowered together into one matcher
    /// list and one filter expression, so the equality is a claim about that
    /// joint lowering rather than a restatement of it. A second condition
    /// whose pushdown replaced the first instead of narrowing it, or a
    /// disjunction whose operands were pushed down conjunctively and so
    /// dropped rows that only one side wanted, would break the property while
    /// leaving each single-condition run correct.
    #[test]
    fn conjunction_of_two_filters_is_the_intersection_of_each_alone(
        (traces, a, b) in (gen_traces(), cond(), cond()),
    ) {
        let engine = engine_over(&traces);
        let (query_a, query_b) = (and_query(&[a]), and_query(&[b]));
        let query_and = format!("{{ {} && {} }}", a.text(), b.text());
        let query_or = format!("{{ {} || {} }}", a.text(), b.text());

        let runs = runtime().block_on(async {
            Ok::<_, TraceqlError>((
                matched_spans(&engine, &query_a).await?,
                matched_spans(&engine, &query_b).await?,
                matched_spans(&engine, &query_and).await?,
                matched_spans(&engine, &query_or).await?,
            ))
        });
        let (only_a, only_b, both_and, both_or) = or_fail(runs, &query_or)?;

        let intersection: Vec<(u8, u8)> = only_a
            .iter()
            .filter(|key| only_b.contains(key))
            .copied()
            .collect();
        let mut union: Vec<(u8, u8)> = only_a.clone();
        union.extend(only_b.iter().filter(|key| !only_a.contains(key)).copied());
        union.sort_unstable();

        prop_assert_eq!(both_and, intersection, "query: {}", query_and);
        prop_assert_eq!(both_or, union, "query: {}", query_or);
    }

    /// A search limit truncates the unlimited result; it does not reorder it.
    ///
    /// Tempo's `/api/search` takes a `limit` on the number of traces returned,
    /// and the traces come back in a defined order, so a limited search has to
    /// hand back a prefix of the unlimited one. The engine's order is total
    /// and fixed: `assemble_search_response` sorts by
    /// `(start_time_unix_nano, trace_id)` and truncates afterwards, and the
    /// generator gives every trace a distinct start time and a distinct id, so
    /// the prefix is well defined for every case.
    ///
    /// It is not a tautology: the limit is threaded through `SearchOptions`,
    /// defaulted when zero, clamped against `EngineOpts::max_traces`, and
    /// applied only after assembly, and any of those steps could keep the
    /// wrong traces or return them in another order. The comparison is over
    /// whole `TraceResult` values, so a limit that also changed a trace's span
    /// set would fail it.
    #[test]
    fn a_search_limit_returns_a_prefix_of_the_unlimited_result(
        (traces, conds, limit) in (gen_traces(), conds(), 1_usize..=6),
    ) {
        let engine = engine_over(&traces);
        let query = and_query(&conds);

        let responses = runtime().block_on(async {
            let unlimited = search_unlimited(&engine, &query).await?;
            let limited = engine
                .search_with_options(
                    TENANT,
                    &query,
                    SEARCH_START_NS,
                    SEARCH_END_NS,
                    SearchOptions { limit, spss: NO_LIMIT, ..SearchOptions::default() },
                )
                .await?;
            Ok::<_, TraceqlError>((unlimited, limited))
        });
        let (unlimited, limited) = or_fail(responses, &query)?;

        let want: Vec<TraceResult> = unlimited.traces.into_iter().take(limit).collect();
        prop_assert_eq!(limited.traces, want, "query: {}, limit: {}", query, limit);
    }
}
