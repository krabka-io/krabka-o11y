//! Typed execution generation: the oracle reads original values, never the
//! parser, projected columns, planner, or the engine's comparison helpers.

use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};

use krabka_traceql::{
    AttrValue, Attribute, EngineOpts, InMemorySpanStore, InputSpan, TraceMetricSeries,
    TraceqlEngine,
};
use krabka_units::nanos;
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Sample {
    Missing,
    Integer(i16),
    Float(i16),
    Boolean(bool),
    Text(&'static str),
}

impl Sample {
    fn attribute(&self) -> Option<AttrValue> {
        match self {
            Self::Missing => None,
            Self::Integer(value) => Some(AttrValue::Int(i64::from(*value))),
            Self::Float(value) => Some(AttrValue::Float(f64::from(*value) / 2.0)),
            Self::Boolean(value) => Some(AttrValue::Bool(*value)),
            Self::Text(value) => Some(AttrValue::Str((*value).into())),
        }
    }
}

fn sample() -> impl Strategy<Value = Sample> {
    prop_oneof![
        Just(Sample::Missing),
        (-8_i16..9).prop_map(Sample::Integer),
        (-16_i16..17).prop_map(Sample::Float),
        any::<bool>().prop_map(Sample::Boolean),
        prop::sample::select(&["", "0", "1", "false", "true", "a", "b"]).prop_map(Sample::Text),
    ]
}

#[derive(Clone, Debug)]
struct SamplePair {
    lhs: Sample,
    rhs: Sample,
}

struct SearchAndMetrics {
    search: krabka_traceql::SearchResponse,
    metrics: krabka_traceql::TraceMetricsResponse,
}

fn sample_pair() -> impl Strategy<Value = SamplePair> {
    sample().prop_flat_map(|lhs| {
        sample().prop_map(move |rhs| SamplePair {
            lhs: lhs.clone(),
            rhs,
        })
    })
}

#[derive(Clone, Copy, Debug)]
enum Operator {
    Equal,
    Different,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl Operator {
    fn text(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::Different => "!=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
        }
    }

    fn model(self, lhs: &Sample, rhs: &Sample) -> bool {
        let ordering = match lhs {
            Sample::Integer(lhs) => match rhs {
                Sample::Integer(rhs) => Some(lhs.cmp(rhs)),
                Sample::Float(rhs) => (f64::from(*lhs) * 2.0).partial_cmp(&f64::from(*rhs)),
                _ => None,
            },
            Sample::Float(lhs) => match rhs {
                Sample::Float(rhs) => Some(lhs.cmp(rhs)),
                Sample::Integer(rhs) => f64::from(*lhs).partial_cmp(&(f64::from(*rhs) * 2.0)),
                _ => None,
            },
            Sample::Text(lhs) => match rhs {
                Sample::Text(rhs) => Some(lhs.cmp(rhs)),
                _ => None,
            },
            Sample::Boolean(lhs) if matches!(self, Self::Equal | Self::Different) => match rhs {
                Sample::Boolean(rhs) => Some(lhs.cmp(rhs)),
                _ => None,
            },
            _ => None,
        };
        let Some(ordering) = ordering else {
            return false;
        };
        match self {
            Self::Equal => ordering == Ordering::Equal,
            Self::Different => ordering != Ordering::Equal,
            Self::Less => ordering == Ordering::Less,
            Self::LessOrEqual => ordering != Ordering::Greater,
            Self::Greater => ordering == Ordering::Greater,
            Self::GreaterOrEqual => ordering != Ordering::Less,
        }
    }
}

fn input(id: u8, lhs: &Sample, rhs: &Sample) -> InputSpan {
    let mut attrs = Vec::new();
    if let Some(value) = lhs.attribute() {
        attrs.push(Attribute::new("lhs", value));
    }
    if let Some(value) = rhs.attribute() {
        attrs.push(Attribute::new("rhs", value));
    }
    InputSpan {
        trace_id: [1; 16],
        span_id: [id; 8],
        parent_span_id: None,
        name: format!("span-{id}"),
        kind: 0,
        start_unix_nano: i64::from(id),
        duration: nanos(10),
        status_code: 0,
        status_message: String::new(),
        instrumentation_name: String::new(),
        instrumentation_version: String::new(),
        attrs,
        events: Vec::new(),
        links: Vec::new(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn typed_comparison_and_boolean_trees_match_original_value_ledger(
        rows in prop::collection::vec(sample_pair(),1..9),
        operator in prop::sample::select(&[Operator::Equal,Operator::Different,Operator::Less,Operator::LessOrEqual,Operator::Greater,Operator::GreaterOrEqual]),
        negate in any::<bool>(),
        require_present in any::<bool>(),
    ) {
        let mut store = InMemorySpanStore::new();
        store.push_trace("t","service","root",rows.iter().enumerate()
            .map(|indexed_row| input(u8::try_from(indexed_row.0+1).unwrap(), &indexed_row.1.lhs, &indexed_row.1.rhs)).collect());
        let engine=TraceqlEngine::new(Arc::new(store),EngineOpts::default());
        let comparison=format!("span.lhs {} span.rhs",operator.text());
        let comparison=if negate {format!("!({comparison})")} else {comparison};
        let expression=if require_present {format!("({comparison}) && span.lhs != nil")} else {comparison};
        let expected=rows.iter().enumerate().filter(|indexed_row| {
            (operator.model(&indexed_row.1.lhs,&indexed_row.1.rhs) != negate) && (!require_present || !matches!(indexed_row.1.lhs,Sample::Missing))
        }).map(|indexed_row|[u8::try_from(indexed_row.0+1).unwrap();8]).collect::<Vec<_>>();
        let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let SearchAndMetrics { search, metrics } = runtime.block_on(async {
            let search=engine.search_with_spss("t",&format!("{{ {expression} }}"),0,100,100,100).await.unwrap();
            let metrics=engine.query_range("t",&format!("{{ {expression} }} | count_over_time()"),0,100,100).await.unwrap();
            SearchAndMetrics { search, metrics }
        });
        let mut actual=search.traces.iter().flat_map(|trace|trace.span_sets.iter())
            .flat_map(|set|set.spans.iter()).map(|span|span.span_id).collect::<Vec<_>>();
        actual.sort_unstable();
        prop_assert_eq!(actual,expected.clone(),"query: {}",expression);
        let count=f64::from(u32::try_from(expected.len()).unwrap());
        let expected_series = if expected.is_empty() { Vec::new() } else { vec![TraceMetricSeries { labels: Vec::new(), label_types: BTreeMap::new(), points: vec![krabka_traceql::MetricPoint::new(0,count),krabka_traceql::MetricPoint::new(100,0.0)], exemplars: Vec::new() }] };
        prop_assert_eq!(metrics.series, expected_series, "query: {}", expression);
    }
}
