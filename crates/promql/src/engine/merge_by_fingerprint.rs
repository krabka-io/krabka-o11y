use std::collections::{BTreeMap, btree_map::Entry};

use krabka_blockstore::SeriesFingerprint;

use crate::planner::LabeledSeries;

/// Folds runs of the same series into one entry, in fingerprint order.
///
/// A selector with `or` branches resolves to more than one matcher set, and a
/// series can match several of them. Each branch contributes its own run, and
/// `SeriesDivide` splits its input wherever the label columns change, so two
/// runs of one series would become two series. Merging them here keeps the leaf
/// to one contiguous run per series — the shape the single-matcher-set case
/// already has, and the one a flat scan produced by concatenating every branch
/// and re-sorting.
pub(super) fn merge_by_fingerprint(series: Vec<LabeledSeries>) -> Vec<LabeledSeries> {
    let mut by_fp: BTreeMap<SeriesFingerprint, LabeledSeries> = BTreeMap::new();
    for one in series {
        match by_fp.entry(one.fp) {
            Entry::Occupied(mut held) => held.get_mut().samples.extend(one.samples),
            Entry::Vacant(free) => {
                free.insert(one);
            }
        }
    }
    by_fp
        .into_values()
        .map(|mut one| {
            // Stable, so two branches contributing the same instant keep the
            // branch order a flat concatenation would have given them.
            one.samples.sort_by_key(|sample| sample.ts_ms);
            one
        })
        .collect()
}
