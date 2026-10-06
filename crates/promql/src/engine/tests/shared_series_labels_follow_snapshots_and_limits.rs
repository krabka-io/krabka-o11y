use krabka_blockstore::MatchOp;

use super::*;
use crate::{MergedMetricStore, MetricStore, PromqlMatcher as LabelMatcher, WalHead};

fn labels(name: &str, job: &str) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    labels.insert("job", job);
    labels
}

#[tokio::test]
async fn shared_series_labels_follow_snapshots_and_limits() {
    let api = labels("up", "api");
    let latency = labels("latency", "api");
    let mut cold = InMemoryMetricStore::new();
    cold.push_float("t", api.clone(), 1_000, 1.0);
    let cold_labels = Arc::clone(&cold.floats["t"].iter().next().unwrap().labels);
    let mut hot = InMemoryMetricStore::with_retention(secs(1));
    hot.push_float("t", api.clone(), 2_000, 2.0);
    hot.push_float("t", labels("up", "worker"), 999, 3.0);
    hot.push_histogram("t", latency.clone(), 2_000, native_histogram(2.0, 3.0));
    hot.push_float("other", labels("outside", "api"), 2_000, 4.0);
    let hot_labels = Arc::clone(&hot.hists["t"].iter().next().unwrap().labels);
    let head = WalHead::from_store(hot);
    let store = Arc::new(MergedMetricStore::new(cold, head.clone()));
    let engine = PromqlEngine::new(Arc::clone(&store), EngineOpts::default());
    let matchers = vec![LabelMatcher::new("job", MatchOp::Eq, "api")];
    let resolved = engine
        .labels_by_fingerprint_sets("t", &[matchers.clone()], 1_000, 2_000)
        .await
        .unwrap();
    let expected: BTreeMap<_, crate::PromqlLabels> = BTreeMap::from([
        (api.fingerprint(), api.clone().into()),
        (latency.fingerprint(), latency.clone().into()),
    ]);
    let hot_series = head
        .series_shared("t", &matchers, 1_000, 2_000)
        .await
        .unwrap();
    check!(
        hot_series.iter().map(Arc::as_ref).collect::<Vec<_>>()
            == expected.values().collect::<Vec<_>>()
    );
    check!(
        resolved
            .iter()
            .map(|(fp, labels)| (*fp, labels.as_ref().clone()))
            .collect::<BTreeMap<_, _>>()
            == expected
    );
    check!(Arc::ptr_eq(&resolved[&api.fingerprint()], &cold_labels));
    check!(Arc::ptr_eq(&resolved[&latency.fingerprint()], &hot_labels));
    let owned = store.series("t", &matchers, 1_000, 2_000).await.unwrap();
    check!(
        owned
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect::<BTreeMap<_, _>>()
            == expected
    );
    let limited = PromqlEngine::new(
        Arc::clone(&store),
        EngineOpts {
            max_fetched_series: 1,
            ..EngineOpts::default()
        },
    );
    check!(
        limited
            .labels_by_fingerprint_sets("t", &[matchers.clone()], 1_000, 2_000)
            .await
            .is_err()
    );
    let pruned = head.prune(4_000);
    check!((pruned.samples_dropped, pruned.series_dropped) == (4, 4));
    head.delete_tenant("t");
    let current = engine
        .labels_by_fingerprint_sets("t", &[matchers], 1_000, 2_000)
        .await
        .unwrap();
    check!(current.len() == 1);
    check!(resolved[&latency.fingerprint()].as_ref() == &latency);
    check!(Arc::ptr_eq(&resolved[&latency.fingerprint()], &hot_labels));
}
