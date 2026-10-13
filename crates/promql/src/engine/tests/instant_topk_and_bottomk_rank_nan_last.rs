use super::*;

#[derive(bon::Builder)]
struct RankInput {
    instance: &'static str,
    value: f64,
}

#[derive(Debug, Eq, PartialEq)]
struct RankedInstance {
    instance: String,
    value: Option<OrderedF64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrderedF64(u64);

struct TopKCase {
    query: &'static str,
    expected: Vec<RankedInstance>,
}

/// Prometheus ranks a `NaN` below every number under both operators. `topk`
/// and `bottomk` therefore select the `NaN` series only after they select
/// every number. `None` stands for the `NaN` sample in the table below,
/// because no float compares equal to a `NaN`.
#[tokio::test]
pub(crate) async fn instant_topk_and_bottomk_rank_nan_last() {
    let mut store = InMemoryMetricStore::new();
    for input in [
        RankInput::builder().instance("a").value(1.0).build(),
        RankInput::builder().instance("c").value(2.0).build(),
        RankInput::builder().instance("n").value(f64::NAN).build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "memory_bytes"),
                crate::test_support::TestLabelPair::new("instance", input.instance),
            ]),
            10_000,
            input.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        TopKCase {
            query: "topk(1, memory_bytes)",
            expected: vec![ranked("c", Some(2.0))],
        },
        TopKCase {
            query: "bottomk(1, memory_bytes)",
            expected: vec![ranked("a", Some(1.0))],
        },
        TopKCase {
            query: "topk(2, memory_bytes)",
            expected: vec![ranked("c", Some(2.0)), ranked("a", Some(1.0))],
        },
        TopKCase {
            query: "bottomk(2, memory_bytes)",
            expected: vec![ranked("a", Some(1.0)), ranked("c", Some(2.0))],
        },
        TopKCase {
            query: "topk(3, memory_bytes)",
            expected: vec![
                ranked("c", Some(2.0)),
                ranked("a", Some(1.0)),
                ranked("n", None),
            ],
        },
        TopKCase {
            query: "bottomk(3, memory_bytes)",
            expected: vec![
                ranked("a", Some(1.0)),
                ranked("c", Some(2.0)),
                ranked("n", None),
            ],
        },
    ] {
        let samples = instant_vector(&engine, case.query, 10_000).await;
        let selected = samples
            .iter()
            .map(|sample| {
                let value = float_value(&sample.value);
                RankedInstance {
                    instance: sample.labels.get("instance").unwrap().to_owned(),
                    value: (!value.is_nan())
                        .then_some(value)
                        .map(|value| OrderedF64(value.to_bits())),
                }
            })
            .collect::<Vec<_>>();
        assert2::assert!(selected == case.expected);
    }
}

fn ranked(instance: &'static str, value: Option<f64>) -> RankedInstance {
    RankedInstance {
        instance: instance.to_owned(),
        value: value.map(|value| OrderedF64(value.to_bits())),
    }
}
