use super::{BTreeMap, BTreeSet, IndexShardRange, TenantIndex, index_shard_width_for_span};

/// Assigns a tenant's live blocks to shards on a grid.
///
/// A block belongs to every shard its own span crosses. A block that crosses a
/// boundary is therefore written more than once, and a query that reads two
/// shards can see it twice. The load merges blocks by object key, so the
/// duplicate costs nothing but the bytes. The alternative is to cut shards on
/// block boundaries instead of on a grid, and that is what would make an
/// append rewrite its neighbours.
pub(crate) fn plan_tenant_index_shards(
    tenant_index: &TenantIndex,
    requested_width: i64,
) -> Vec<(IndexShardRange, BTreeSet<u32>)> {
    let ordinals = tenant_index.blocks.ordinals_overlapping(i64::MIN, i64::MAX);
    if ordinals.is_empty() {
        return Vec::new();
    }

    let mut span_min = i64::MAX;
    let mut span_max = i64::MIN;
    for ordinal in &ordinals {
        let entry = tenant_index.blocks.entry(*ordinal);
        span_min = span_min.min(entry.min_ts);
        span_max = span_max.max(entry.max_ts);
    }
    let width = index_shard_width_for_span(requested_width, span_min, span_max);

    let mut shards: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();
    for ordinal in ordinals {
        let entry = tenant_index.blocks.entry(ordinal);
        let first = entry.min_ts.div_euclid(width);
        let last = entry.max_ts.div_euclid(width);
        for slot in first..=last {
            shards.entry(slot).or_default().insert(ordinal);
        }
    }

    shards
        .into_iter()
        .map(|(slot, ordinals)| (shard_range(slot, width), ordinals))
        .collect()
}

/// The span of grid slot `slot`, clamped where the grid runs off the ends of
/// an `i64`.
fn shard_range(slot: i64, width: i64) -> IndexShardRange {
    let start = slot.checked_mul(width).unwrap_or(i64::MIN);
    let end = start.saturating_add(width.saturating_sub(1));
    IndexShardRange::new(start, end)
}
