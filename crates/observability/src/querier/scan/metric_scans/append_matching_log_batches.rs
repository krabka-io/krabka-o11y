use super::{
    ActiveLogDeleteFilter, BTreeMap, LabelIndex, Labels, LokiStreamEntry, QueryError, RecordBatch,
    StreamPlan, append_matching_log_row, for_each_query_row,
};

pub(crate) fn append_matching_log_batches(
    streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    batches: &[RecordBatch],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    for_each_query_row(batches, |row| {
        append_matching_log_row(streams, plan, label_index, row, delete_filters)
    })
}
