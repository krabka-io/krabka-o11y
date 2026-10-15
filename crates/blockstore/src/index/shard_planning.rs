use super::*;

use krabka_o11y_verified::shard_range;

/// The shard width to cut a tenant spanning `[min_ts, max_ts]` with.
///
/// `requested` is the caller's width. This function doubles it until the span
/// fits [`MAX_INDEX_SHARDS_PER_TENANT`] shards. A width that would overflow is
/// clamped, and the tenant then collapses to a single shard. That is coarse,
/// but it is a correct index and a bounded number of objects.
#[must_use]
pub(crate) fn index_shard_width_for_span(requested: i64, min_ts: i64, max_ts: i64) -> i64 {
    let mut width = requested.max(1);
    loop {
        let first = min_ts.div_euclid(width);
        let last = max_ts.div_euclid(width);
        let shards = u128::from(last.abs_diff(first)).saturating_add(1);
        let ceiling =
            u128::try_from(MAX_INDEX_SHARDS_PER_TENANT).expect("a shard ceiling fits a u128");
        if shards <= ceiling {
            return width;
        }
        let Some(doubled) = width.checked_mul(2) else {
            return i64::MAX;
        };
        width = doubled;
    }
}

pub(crate) struct PlannedIndexShard {
    pub range: IndexShardRange,
    pub ordinals: BTreeSet<u32>,
}

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
) -> Vec<PlannedIndexShard> {
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
        .map(|(slot, ordinals)| {
            let range = shard_range(slot, width);
            PlannedIndexShard {
                range: IndexShardRange::new(range.start, range.end),
                ordinals,
            }
        })
        .collect()
}
