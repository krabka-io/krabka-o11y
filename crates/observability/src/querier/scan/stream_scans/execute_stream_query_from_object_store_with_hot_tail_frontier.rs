use std::borrow::Borrow;

use super::{
    Arc, LabelIndex, ObjectPath, ObjectStore, QueryError, QueryHotTail, StreamPlan,
    StreamScanOptions, Value,
    execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options,
};
use crate::WalLogRecord;

pub(crate) async fn execute_stream_query_from_object_store_with_hot_tail_frontier<
    R: Borrow<WalLogRecord> + Sync,
>(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_, R>,
) -> Result<Value, QueryError> {
    Ok(
        execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            store,
            prefix,
            plan,
            label_index,
            hot_tail,
            StreamScanOptions::exhaustive(),
        )
        .await?
        .value,
    )
}
