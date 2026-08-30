use super::{
    Arc, BTreeMap, LabelIndex, Labels, LokiDirection, ObjectPath, ObjectStore,
    ObjectStoreStreamScan, QueryError, QueryHotTail, StreamPlan, StreamScanOptions,
    append_matching_hot_log_record, append_matching_log_batches,
    collect_object_store_stream_log_batches, loki_streams_response,
    loki_streams_response_with_warnings, object_store_stream_blocks_in_scan_order,
    sort_loki_stream_values,
};

pub(crate) async fn execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_>,
    options: StreamScanOptions,
) -> Result<ObjectStoreStreamScan, QueryError> {
    if plan.blocks.is_empty() || plan.fingerprints.is_empty() {
        let mut streams = BTreeMap::new();
        for record in hot_tail.records {
            append_matching_hot_log_record(
                &mut streams,
                plan,
                record,
                hot_tail.frontier,
                hot_tail.delete_filters,
            );
        }
        sort_loki_stream_values(&mut streams);
        return Ok(ObjectStoreStreamScan {
            value: loki_streams_response(streams),
            scanned_blocks: Vec::new(),
        });
    }

    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut scanned_blocks = Vec::new();

    if matches!(options.direction, LokiDirection::Backward) {
        for record in hot_tail.records {
            append_matching_hot_log_record(
                &mut streams,
                plan,
                record,
                hot_tail.frontier,
                hot_tail.delete_filters,
            );
        }
    }

    if !options.reached_limit(&streams) {
        let ordered_blocks =
            object_store_stream_blocks_in_scan_order(&plan.blocks, options.direction);
        for block_batch in ordered_blocks.chunks(options.block_fetch_concurrency()) {
            if options.reached_limit(&streams) {
                break;
            }
            let results = futures_util::future::join_all(block_batch.iter().map(|block| {
                let store = Arc::clone(&store);
                let block = *block;
                async move {
                    let result =
                        collect_object_store_stream_log_batches(store, prefix, block, plan).await;
                    (block, result)
                }
            }))
            .await;

            for (block, result) in results {
                scanned_blocks.push(block.clone());
                let Ok(batches) = result else {
                    warnings.push(format!("failed to read block {}", block.key.object_key()));
                    continue;
                };
                append_matching_log_batches(
                    &mut streams,
                    plan,
                    label_index,
                    &batches,
                    hot_tail.delete_filters,
                )?;
            }
        }
    }

    if matches!(options.direction, LokiDirection::Forward) && !options.reached_limit(&streams) {
        for record in hot_tail.records {
            append_matching_hot_log_record(
                &mut streams,
                plan,
                record,
                hot_tail.frontier,
                hot_tail.delete_filters,
            );
        }
    }
    sort_loki_stream_values(&mut streams);

    Ok(ObjectStoreStreamScan {
        value: loki_streams_response_with_warnings(streams, &warnings),
        scanned_blocks,
    })
}
