#[cfg(creusot)]
use creusot_std::prelude::*;

/// Returns the exclusive end of every compaction run.
///
/// The caller has already removed sealed blocks and grouped and sorted the
/// remaining candidates. Every row count is therefore below `target_rows`.
/// A returned run contains at least two blocks, never exceeds `max_blocks`,
/// and the only input omitted is a possible final singleton.
#[cfg_attr(creusot, requires(max_blocks@ >= 2))]
#[cfg_attr(creusot, requires(target_rows@ >= 1))]
#[cfg_attr(creusot, requires(forall<i: Int> 0 <= i && i < row_counts@.len()
    ==> row_counts@[i]@ < target_rows@))]
#[cfg_attr(creusot, ensures(forall<i: Int> 0 <= i && i < result@.len()
    ==> result@[i]@ <= row_counts@.len()))]
#[cfg_attr(creusot, ensures(forall<i: Int> 0 <= i && i < result@.len()
    ==> (if i == 0 { result@[i]@ } else { result@[i]@ - result@[i - 1]@ }) >= 2))]
#[cfg_attr(creusot, ensures(forall<i: Int> 0 <= i && i < result@.len()
    ==> (if i == 0 { result@[i]@ } else { result@[i]@ - result@[i - 1]@ }) <= max_blocks@))]
#[cfg_attr(creusot, ensures(result@.len() == 0 ==> row_counts@.len() <= 1))]
#[cfg_attr(creusot, ensures(result@.len() > 0
    ==> row_counts@.len() - result@[result@.len() - 1]@ <= 1))]
#[must_use]
pub fn compaction_run_ends(
    row_counts: &[usize],
    max_blocks: usize,
    target_rows: usize,
) -> Vec<usize> {
    let mut ends: Vec<usize> = Vec::new();
    let mut start = 0_usize;
    let mut rows = 0_usize;
    let mut index = 0_usize;
    #[cfg_attr(creusot, invariant(index@ <= row_counts@.len()))]
    #[cfg_attr(creusot, invariant(start@ <= index@))]
    #[cfg_attr(creusot, invariant(index@ - start@ < max_blocks@))]
    #[cfg_attr(creusot, invariant(index@ == start@ ==> rows@ == 0))]
    #[cfg_attr(creusot, invariant(index@ - start@ == 1 ==> rows@ < target_rows@))]
    #[cfg_attr(creusot, invariant(forall<i: Int> 0 <= i && i < ends@.len()
        ==> ends@[i]@ <= row_counts@.len()))]
    #[cfg_attr(creusot, invariant(forall<i: Int> 0 <= i && i < ends@.len()
        ==> (if i == 0 { ends@[i]@ } else { ends@[i]@ - ends@[i - 1]@ }) >= 2))]
    #[cfg_attr(creusot, invariant(forall<i: Int> 0 <= i && i < ends@.len()
        ==> (if i == 0 { ends@[i]@ } else { ends@[i]@ - ends@[i - 1]@ }) <= max_blocks@))]
    #[cfg_attr(creusot, invariant(ends@.len() == 0 ==> start@ == 0))]
    #[cfg_attr(creusot, invariant(ends@.len() > 0
        ==> start@ == ends@[ends@.len() - 1]@))]
    #[cfg_attr(creusot, variant(row_counts@.len() - index@))]
    while index < row_counts.len() {
        rows = rows.saturating_add(row_counts[index]);
        index += 1;
        if index - start >= max_blocks || rows >= target_rows {
            ends.push(index);
            start = index;
            rows = 0;
        }
    }
    if index - start >= 2 {
        ends.push(index);
    }
    ends
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::compaction_run_ends;

    fn reference(row_counts: &[usize], max_blocks: usize, target_rows: usize) -> Vec<usize> {
        let mut ends = Vec::new();
        let mut start = 0;
        let mut rows = 0_usize;
        for (index, row_count) in row_counts.iter().enumerate() {
            rows = rows.saturating_add(*row_count);
            if index + 1 - start >= max_blocks || rows >= target_rows {
                ends.push(index + 1);
                start = index + 1;
                rows = 0;
            }
        }
        if row_counts.len() - start >= 2 {
            ends.push(row_counts.len());
        }
        ends
    }

    #[test]
    fn closes_at_caps_and_omits_only_a_singleton() {
        check!(compaction_run_ends(&[1, 1, 1, 1, 1], 2, 100) == std::vec![2, 4]);
        check!(compaction_run_ends(&[60, 60, 60, 60], 8, 100) == std::vec![2, 4]);
        check!(compaction_run_ends(&[1], 8, 100).is_empty());
    }

    #[test]
    fn saturating_rows_still_close_the_run() {
        check!(compaction_run_ends(&[usize::MAX - 1, 1], 8, usize::MAX) == std::vec![2]);
    }

    #[test]
    fn matches_the_previous_planner_for_bounded_inputs() {
        for target_rows in 1_usize..=4 {
            for max_blocks in 2..=5 {
                for len in 0_u32..=6 {
                    for mut encoded in 0..target_rows.pow(len) {
                        let mut row_counts = Vec::with_capacity(len as usize);
                        for _ in 0..len {
                            row_counts.push(encoded % target_rows);
                            encoded /= target_rows;
                        }
                        check!(
                            compaction_run_ends(&row_counts, max_blocks, target_rows)
                                == reference(&row_counts, max_blocks, target_rows)
                        );
                    }
                }
            }
        }
    }
}
