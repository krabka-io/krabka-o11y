use super::*;

/// Deterministic object plan plus row-count evidence for one encoded block kind.
#[must_use]
pub fn compaction_object_plan_for_rows(
    rows: &TenantCompactionRows,
    kind: MetricBlockKind,
    first_offset: i64,
    last_offset: i64,
) -> CompactionObjectPlan {
    let mut plan = compaction_object_plan(&rows.tenant, kind, first_offset, last_offset);
    plan.row_count = match kind {
        MetricBlockKind::Float => rows.float_rows.len(),
        MetricBlockKind::NativeHistograms => rows.histogram_rows.len(),
        MetricBlockKind::Exemplars => rows.exemplar_rows.len(),
        MetricBlockKind::Metadata => rows.metadata_rows.len(),
        MetricBlockKind::ClockReadings => rows.clock_rows.len(),
    };
    plan
}
