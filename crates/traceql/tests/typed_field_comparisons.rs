//! Typed execution generation: the oracle reads original values, never the
//! parser, projected columns, planner, or the engine's comparison helpers.

use std::{cmp::Ordering, sync::Arc};

use krabka_traceql::{AttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine};
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
        let ordering = match (lhs, rhs) {
            (Sample::Integer(lhs), Sample::Integer(rhs)) => Some(lhs.cmp(rhs)),
            (Sample::Float(lhs), Sample::Float(rhs)) => Some(lhs.cmp(rhs)),
            (Sample::Integer(lhs), Sample::Float(rhs)) => {
                (f64::from(*lhs) * 2.0).partial_cmp(&f64::from(*rhs))
            }
            (Sample::Float(lhs), Sample::Integer(rhs)) => {
                f64::from(*lhs).partial_cmp(&(f64::from(*rhs) * 2.0))
            }
            (Sample::Text(lhs), Sample::Text(rhs)) => Some(lhs.cmp(rhs)),
            (Sample::Boolean(lhs), Sample::Boolean(rhs))
                if matches!(self, Self::Equal | Self::Different) =>
            {
                Some(lhs.cmp(rhs))
            }
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
        attrs: [("lhs", lhs.attribute()), ("rhs", rhs.attribute())]
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key.into(), value)))
            .collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn typed_comparison_and_boolean_trees_match_original_value_ledger(
        rows in prop::collection::vec((sample(),sample()),1..9),
        operator in prop::sample::select(&[Operator::Equal,Operator::Different,Operator::Less,Operator::LessOrEqual,Operator::Greater,Operator::GreaterOrEqual]),
        negate in any::<bool>(),
        require_present in any::<bool>(),
    ) {
        let mut store = InMemorySpanStore::new();
        store.push_trace("t","service","root",rows.iter().enumerate()
            .map(|(index,(lhs,rhs))|input(u8::try_from(index+1).unwrap(),lhs,rhs)).collect());
        let engine=TraceqlEngine::new(Arc::new(store),EngineOpts::default());
        let comparison=format!("span.lhs {} span.rhs",operator.text());
        let comparison=if negate {format!("!({comparison})")} else {comparison};
        let expression=if require_present {format!("({comparison}) && span.lhs != nil")} else {comparison};
        let expected=rows.iter().enumerate().filter(|(_, (lhs,rhs))| {
            (operator.model(lhs,rhs) != negate) && (!require_present || !matches!(lhs,Sample::Missing))
        }).map(|(index,_)|[u8::try_from(index+1).unwrap();8]).collect::<Vec<_>>();
        let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let (search,metrics)=runtime.block_on(async {
            let search=engine.search_with_spss("t",&format!("{{ {expression} }}"),0,100,100,100).await.unwrap();
            let metrics=engine.query_range("t",&format!("{{ {expression} }} | count_over_time()"),0,100,100).await.unwrap();
            (search,metrics)
        });
        let mut actual=search.traces.iter().flat_map(|trace|trace.span_sets.iter())
            .flat_map(|set|set.spans.iter()).map(|span|span.span_id).collect::<Vec<_>>();
        actual.sort_unstable();
        prop_assert_eq!(actual,expected.clone(),"query: {}",expression);
        let count=f64::from(u32::try_from(expected.len()).unwrap());
        prop_assert_eq!(&metrics.series[0].points,&vec![(0,count),(100,0.0)]);
    }
}
