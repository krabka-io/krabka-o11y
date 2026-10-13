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
pub(crate) struct QueryPathResults {
    pub(crate) via_operators: QueryResult,
    pub(crate) via_interpreter: QueryResult,
}

pub(crate) async fn planned_and_interpreted<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    time_ms: i64,
) -> QueryPathResults {
    let parsed = ParsedQuery::at(query, time_ms);
    QueryPathResults {
        via_operators: via_operators(engine, &parsed).await,
        via_interpreter: via_interpreter(engine, &parsed).await,
    }
}

/// Both paths' vector samples for one query, each sorted by series fingerprint.
pub(crate) struct SortedParity {
    pub(crate) via_operators: Vec<crate::InstantSample>,
    pub(crate) via_interpreter: Vec<crate::InstantSample>,
}

pub(crate) struct PathEvaluation {
    pub result: QueryResult,
    pub annotations: Annotations,
}

pub(crate) struct AnnotatedParity {
    pub via_operators: PathEvaluation,
    pub via_interpreter: PathEvaluation,
}

/// Like [`planned_and_interpreted`], with each path's vector sorted by
/// [`fingerprint_sorted`].
pub(crate) async fn sorted_planned_and_interpreted<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    time_ms: i64,
) -> SortedParity {
    let parity = planned_and_interpreted(engine, query, time_ms).await;
    let via_operators = parity.via_operators;
    let via_interpreter = parity.via_interpreter;
    SortedParity {
        via_operators: fingerprint_sorted(via_operators, query),
        via_interpreter: fingerprint_sorted(via_interpreter, query),
    }
}

/// Like [`planned_and_interpreted`], with the annotations each path raised.
pub(crate) async fn annotated_planned_and_interpreted<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    time_ms: i64,
) -> AnnotatedParity {
    let parsed = ParsedQuery::at(query, time_ms);
    AnnotatedParity {
        via_operators: with_annotations(via_operators(engine, &parsed)).await,
        via_interpreter: with_annotations(via_interpreter(engine, &parsed)).await,
    }
}

/// Runs `future` inside a fresh annotation sink and returns what it raised.
async fn with_annotations<T>(future: impl Future<Output = T>) -> PathEvaluation
where
    T: Into<QueryResult>,
{
    super::super::ANNOTATIONS
        .scope(RefCell::new(Annotations::new()), async {
            let future_output = future.await;
            let annotations = super::super::ANNOTATIONS.with(|sink| sink.borrow().clone());
            PathEvaluation {
                result: future_output.into(),
                annotations,
            }
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
