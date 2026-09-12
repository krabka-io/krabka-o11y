use super::{BTreeMap, BlockTimestampUnit, CompactionCandidate, ExpiredBlock, RetentionWindows};

/// Names the blocks that end before their tenant's retention cutoff.
///
/// A block expires when its `max_ts` is strictly older than
/// `now_ts - retention`, both counted in `unit`. The newest sample decides,
/// not the oldest: a block that still holds one sample inside the window is
/// still readable data, and deleting it would answer a query that the window
/// covers with a hole.
///
/// A tenant whose window is `Time::ZERO` never expires a block. See
/// [`RetentionWindows::block_retention`].
///
/// The result is ordered by tenant and then by object key. Two passes over one
/// index therefore produce the same plan, which is what lets a caller compare
/// them, log them, or apply them in batches.
///
/// The arithmetic saturates, so a block or a clock at either end of the `i64`
/// range does not panic the compactor.
#[must_use]
pub fn plan_expired_blocks(
    candidates: &[CompactionCandidate],
    now_ts: i64,
    unit: BlockTimestampUnit,
    windows: &dyn RetentionWindows,
) -> Vec<ExpiredBlock> {
    // One lookup per tenant rather than one per block: a tenant's window is
    // configuration, and a large index holds far more blocks than tenants.
    let mut cutoffs: BTreeMap<&str, Option<i64>> = BTreeMap::new();
    let mut expired: Vec<ExpiredBlock> = candidates
        .iter()
        .filter(|candidate| {
            let cutoff = *cutoffs.entry(candidate.tenant.as_str()).or_insert_with(|| {
                let ticks = unit.ticks(windows.block_retention(&candidate.tenant));
                // No window is "keep forever", and so no cutoff at all.
                (ticks > 0).then(|| now_ts.saturating_sub(ticks))
            });
            cutoff.is_some_and(|cutoff| candidate.max_ts < cutoff)
        })
        .map(|candidate| ExpiredBlock {
            tenant: candidate.tenant.clone(),
            object_key: candidate.object_key.clone(),
        })
        .collect();
    expired.sort();
    expired
}
