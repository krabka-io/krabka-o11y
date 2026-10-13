use super::*;

#[derive(Clone)]
struct ComparisonInput {
    metric: &'static str,
    job: &'static str,
    extra_label: Option<InputLabel>,
    value: f64,
}

#[derive(Clone)]
struct InputLabel {
    name: &'static str,
    value: &'static str,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedLabel {
    name: String,
    value: String,
}

impl ExpectedLabel {
    fn new(name: &str, value: &str) -> Self {
        Self {
            name: name.to_owned(),
            value: value.to_owned(),
        }
    }
}

struct QueryExpectation {
    query: &'static str,
    labels: Vec<Vec<ExpectedLabel>>,
}

/// One-to-one `on` projects comparison labels to the matching set. Grouped
/// comparisons preserve operand metadata while arithmetic drops metric names.
/// Each query pair exercises matched and filled paths against explicit labels.
#[tokio::test]
pub(crate) async fn a_comparison_keeps_the_metric_name_that_arithmetic_drops() {
    let mut store = InMemoryMetricStore::new();
    for input in [
        ComparisonInput {
            metric: "a",
            job: "x",
            extra_label: Some(InputLabel {
                name: "extra",
                value: "1",
            }),
            value: 5.0,
        },
        ComparisonInput {
            metric: "a",
            job: "y",
            extra_label: Some(InputLabel {
                name: "extra",
                value: "2",
            }),
            value: 5.0,
        },
        ComparisonInput {
            metric: "b",
            job: "x",
            extra_label: None,
            value: 3.0,
        },
        ComparisonInput {
            metric: "m",
            job: "x",
            extra_label: Some(InputLabel {
                name: "inst",
                value: "1",
            }),
            value: 7.0,
        },
        ComparisonInput {
            metric: "m",
            job: "y",
            extra_label: Some(InputLabel {
                name: "inst",
                value: "9",
            }),
            value: 7.0,
        },
    ] {
        let mut pairs = vec![
            krabka_metrics::MetricLabelPair::new("__name__", input.metric),
            krabka_metrics::MetricLabelPair::new("job", input.job),
        ];
        if let Some(extra) = input.extra_label {
            pairs.push(krabka_metrics::MetricLabelPair::new(
                extra.name,
                extra.value,
            ));
        }
        store.push_float(
            "tenant-a",
            crate::PromqlLabels::from_pairs(pairs),
            10_000,
            input.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        // One-to-one: `x` matches, `y` takes the right-fill path.
        QueryExpectation {
            query: "a > on (job) fill_right(0) b",
            labels: vec![
                vec![ExpectedLabel::new("job", "x")],
                vec![ExpectedLabel::new("job", "y")],
            ],
        },
        QueryExpectation {
            query: "a + on (job) fill_right(0) b",
            labels: vec![
                vec![ExpectedLabel::new("job", "x")],
                vec![ExpectedLabel::new("job", "y")],
            ],
        },
        // The mirror of the pair above: here the *left* side is filled, so the
        // synthetic left operand has the matching labels and no metric name.
        // Only a left-fill query reaches that branch.
        QueryExpectation {
            query: "b < on (job) fill_left(0) a",
            labels: vec![
                vec![ExpectedLabel::new("job", "x")],
                vec![ExpectedLabel::new("job", "y")],
            ],
        },
        QueryExpectation {
            query: "b + on (job) fill_left(0) a",
            labels: vec![
                vec![ExpectedLabel::new("job", "x")],
                vec![ExpectedLabel::new("job", "y")],
            ],
        },
        // `bool` puts a comparison back on the arithmetic rule.
        QueryExpectation {
            query: "a > bool on (job) b",
            labels: vec![vec![ExpectedLabel::new("job", "x")]],
        },
        // Many-to-one: `x` matches, `y` takes the right-fill path.
        QueryExpectation {
            query: "m > on (job) group_left fill_right(0) b",
            labels: vec![
                vec![
                    ExpectedLabel::new("__name__", "m"),
                    ExpectedLabel::new("inst", "1"),
                    ExpectedLabel::new("job", "x"),
                ],
                vec![
                    ExpectedLabel::new("__name__", "m"),
                    ExpectedLabel::new("inst", "9"),
                    ExpectedLabel::new("job", "y"),
                ],
            ],
        },
        QueryExpectation {
            query: "m + on (job) group_left fill_right(0) b",
            labels: vec![
                vec![
                    ExpectedLabel::new("inst", "1"),
                    ExpectedLabel::new("job", "x"),
                ],
                vec![
                    ExpectedLabel::new("inst", "9"),
                    ExpectedLabel::new("job", "y"),
                ],
            ],
        },
        // One-to-many: `x` matches, `y` takes the left-fill path.
        QueryExpectation {
            query: "b < on (job) group_right fill_right(0) m",
            labels: vec![
                vec![
                    ExpectedLabel::new("__name__", "m"),
                    ExpectedLabel::new("inst", "1"),
                    ExpectedLabel::new("job", "x"),
                ],
                vec![
                    ExpectedLabel::new("__name__", "m"),
                    ExpectedLabel::new("inst", "9"),
                    ExpectedLabel::new("job", "y"),
                ],
            ],
        },
        QueryExpectation {
            query: "b + on (job) group_right fill_right(0) m",
            labels: vec![
                vec![
                    ExpectedLabel::new("inst", "1"),
                    ExpectedLabel::new("job", "x"),
                ],
                vec![
                    ExpectedLabel::new("inst", "9"),
                    ExpectedLabel::new("job", "y"),
                ],
            ],
        },
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), case.query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", case.query));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {}", case.query);
        };
        let mut got = samples
            .iter()
            .map(|sample| {
                sample
                    .labels
                    .iter()
                    .map(|pair| ExpectedLabel {
                        name: pair.name.clone(),
                        value: pair.value.to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        got.sort();
        let want = case.labels.iter().cloned().collect::<Vec<_>>();
        assert2::assert!(got == want, "{}", case.query);
    }
}
