use super::*;

/// Four one-row `app="api"` blocks for `tenant-a`, covering 0..=39 ns ten
/// nanoseconds apiece, in a store that delays every get by 25 ms.
pub(crate) struct FourColdApiBlocks {
    pub(crate) store: RecordingObjectStore,
    pub(crate) prefix: ObjectPath,
    pub(crate) label_index: LabelIndex,
    pub(crate) block_index: BlockIndex,
}

impl FourColdApiBlocks {
    pub(crate) async fn write() -> Self {
        let store = RecordingObjectStore::new().with_get_delay(Duration::from_millis(25));
        let prefix = ObjectPath::from("observability/logs");
        let mut label_index = LabelIndex::default();
        let api =
            label_index.insert_series("tenant-a", krabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();
        for block_id in 0_i64..4 {
            let start_ns = block_id * 10;
            let end_ns = start_ns + 9;
            let block = write_log_block_to_object_store(
                &store,
                &prefix,
                &BlockKey::new(
                    "tenant-a",
                    0,
                    start_ns,
                    end_ns,
                    TimeRange::new(start_ns, end_ns).unwrap(),
                ),
                vec![LogRow::new(
                    api,
                    end_ns,
                    format!("api error {block_id}"),
                    BTreeMap::new(),
                )],
            )
            .await
            .unwrap();
            block_index.insert(block);
        }
        Self {
            store,
            prefix,
            label_index,
            block_index,
        }
    }
}
