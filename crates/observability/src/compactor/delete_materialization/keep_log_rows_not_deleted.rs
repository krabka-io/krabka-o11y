use super::{CompactorRunError, LabelIndex, LogRow, is_deleted_log_entry};
use crate::ActiveLogDeleteFilter;

pub(crate) fn keep_log_rows_not_deleted(
    rows: Vec<LogRow>,
    tenant: &str,
    label_index: &LabelIndex,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Vec<LogRow>, CompactorRunError> {
    rows.into_iter().try_fold(Vec::new(), |mut kept, row| {
        let labels = label_index
            .labels_for(tenant, row.series_fingerprint)
            .ok_or_else(|| CompactorRunError::MissingSeriesLabels {
                tenant: tenant.to_owned(),
                fingerprint: row.series_fingerprint,
            })?;
        if !is_deleted_log_entry(
            delete_filters,
            labels,
            &row.line,
            &row.structured_metadata,
            row.timestamp_ns,
        ) {
            kept.push(row);
        }
        Ok(kept)
    })
}
