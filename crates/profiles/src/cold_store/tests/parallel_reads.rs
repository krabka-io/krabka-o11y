use std::time::Duration;

use object_store::throttle::{ThrottleConfig, ThrottledStore};

use super::*;
use crate::hot_store::WalTailProfileStore;

#[tokio::test]
async fn many_cold_blocks_keep_their_symbols_and_fail_as_a_whole_if_one_is_missing() {
    // Awaiting reads makes block loads interleave. More than four blocks
    // exercise several waves, with the same local partition in every block.
    let store: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig {
            wait_get_per_call: Duration::from_millis(1),
            ..ThrottleConfig::default()
        },
    ));
    let records = (1..=9)
        .map(|value| {
            let mut record = record("t", "api", vec![0], value);
            record.symbols.strings[1] = format!("frame-{value}");
            record
        })
        .collect::<Vec<_>>();
    let hot = Arc::new(WalTailProfileStore::new());
    hot.append_records(records.clone()).unwrap();
    let control = FlameEngine::new(hot, EngineOpts::default())
        .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
        .await
        .unwrap();
    check!(control.total == 45);
    let mut index = ProfileIndex::new();
    let labels = Labels::from_pairs(records[0].labels.iter().cloned());
    index
        .add_series("t", labels.fingerprint(), &labels)
        .unwrap();
    let mut keys = Vec::new();
    for (offset, record) in records.iter().enumerate() {
        let offset = i64::try_from(offset).unwrap();
        let meta = build_block(
            &store,
            "t",
            0,
            std::slice::from_ref(record),
            (offset, offset),
            &krabka_blockstore::ObjectStoreMetrics::unregistered(),
        )
        .await
        .unwrap()
        .remove(0);
        keys.push(meta.object_key.clone());
        index.add_block(&meta);
        index.add_profile_block("t", &meta.object_key, vec![STACKTRACE_PARTITION]);
    }
    let cold = Arc::new(ColdProfileStore::new(Arc::clone(&store), Arc::new(index)));
    let engine = FlameEngine::new(cold, EngineOpts::default());
    let result = engine
        .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
        .await
        .unwrap();
    check!(result == control);

    // A failed load must not publish a flamegraph from the other eight blocks.
    store.delete(&Path::from(keys[4].clone())).await.unwrap();
    check!(
        engine
            .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
            .await
            .is_err()
    );
}
