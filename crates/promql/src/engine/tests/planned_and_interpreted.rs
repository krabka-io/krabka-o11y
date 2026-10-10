use std::cell::RefCell;

use super::*;
use crate::Annotations;

/// The tenant the parity tests store their series under.
const PARITY_TENANT: &str = "t";

/// Evaluates `query` for tenant `t` at `time_ms` on both paths of the engine, and returns the
/// operator path's result and then the interpreter's.
///
/// Panics when the query does not parse, when the recursive planner does not
/// claim it, or when either path fails, naming the query.
pub(crate) async fn planned_and_interpreted<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    time_ms: i64,
) -> (QueryResult, QueryResult) {
    let parsed = ParsedQuery::at(query, time_ms);
    (
        via_operators(engine, &parsed).await,
        via_interpreter(engine, &parsed).await,
    )
}

/// Like [`planned_and_interpreted`], with the annotations each path raised.
pub(crate) async fn annotated_planned_and_interpreted<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    time_ms: i64,
) -> ((QueryResult, Annotations), (QueryResult, Annotations)) {
    let parsed = ParsedQuery::at(query, time_ms);
    (
        with_annotations(via_operators(engine, &parsed)).await,
        with_annotations(via_interpreter(engine, &parsed)).await,
    )
}

/// Runs `future` inside a fresh annotation sink and returns what it raised.
async fn with_annotations<T>(future: impl Future<Output = T>) -> (T, Annotations) {
    super::super::ANNOTATIONS
        .scope(RefCell::new(Annotations::new()), async {
            let future_output = future.await;
            let annotations = super::super::ANNOTATIONS.with(|sink| sink.borrow().clone());
            (future_output, annotations)
        })
        .await
}

/// The vector samples of `query_result`, sorted by series fingerprint.
pub(crate) fn fingerprint_sorted(
    query_result: QueryResult,
    query: &str,
) -> Vec<crate::InstantSample> {
    let QueryResult::InstantVector(mut samples) = query_result else {
        panic!("expected vector for `{query}`");
    };
    samples.sort_by_key(|sample| sample.labels.fingerprint());
    samples
}

/// A query's text and its parse at the instant it is evaluated.
struct ParsedQuery<'a> {
    text: &'a str,
    expr: promql_parser::parser::Expr,
    time_ms: i64,
}

impl<'a> ParsedQuery<'a> {
    fn at(text: &'a str, time_ms: i64) -> Self {
        let expr = crate::parse_promql_with_duration_context(
            text,
            crate::DurationExprContext::instant(time_ms),
        )
        .unwrap_or_else(|error| panic!("parse `{text}`: {error}"));
        Self {
            text,
            expr,
            time_ms,
        }
    }
}

/// Operator path: the recursive planner must claim this query.
async fn via_operators<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    parsed: &ParsedQuery<'_>,
) -> QueryResult {
    let ParsedQuery {
        text: query,
        expr,
        time_ms,
    } = parsed;
    let time_ms = *time_ms;
    let plan = engine
        .plan_instant_expr(PARITY_TENANT, expr, time_ms)
        .await
        .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
        .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
    engine
        .assemble_planned_instant(plan, time_ms)
        .await
        .unwrap_or_else(|error| panic!("operator `{query}`: {error}"))
}

/// Interpreter path: evaluate the same expression directly.
async fn via_interpreter<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    parsed: &ParsedQuery<'_>,
) -> QueryResult {
    let ParsedQuery {
        text: query,
        expr,
        time_ms,
    } = parsed;
    let time_ms = *time_ms;
    engine
        .eval_instant_expr(PARITY_TENANT, expr, time_ms)
        .await
        .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"))
}
