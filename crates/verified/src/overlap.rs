#[cfg(creusot)]
use creusot_std::prelude::*;

/// Returns the half-open window that can overlap a query range.
///
/// `starts` is sorted by block start time. `prefix_ends[i]` is the greatest
/// block end through `i`, so it is sorted too. Everything before the returned
/// window ends before `min_ts`; everything after it starts after `max_ts`.
#[cfg_attr(creusot, requires(starts@.len() == prefix_ends@.len()))]
#[cfg_attr(creusot, requires(forall<i: Int, j: Int>
    0 <= i && i < j && j < starts@.len() ==> starts@[i]@ <= starts@[j]@))]
#[cfg_attr(creusot, requires(forall<i: Int, j: Int>
    0 <= i && i < j && j < prefix_ends@.len()
        ==> prefix_ends@[i]@ <= prefix_ends@[j]@))]
#[cfg_attr(creusot, ensures(result.0@ <= result.1@))]
#[cfg_attr(creusot, ensures(result.1@ <= starts@.len()))]
#[cfg_attr(creusot, ensures(forall<i: Int> 0 <= i && i < result.0@
    ==> prefix_ends@[i]@ < min_ts@))]
#[cfg_attr(creusot, ensures(forall<i: Int> result.0@ <= i && i < result.1@
    ==> prefix_ends@[i]@ >= min_ts@ && starts@[i]@ <= max_ts@))]
#[cfg_attr(creusot, ensures(forall<i: Int> result.1@ <= i && i < starts@.len()
    ==> starts@[i]@ > max_ts@))]
#[must_use]
pub fn overlap_window(
    starts: &[i64],
    prefix_ends: &[i64],
    min_ts: i64,
    max_ts: i64,
) -> (usize, usize) {
    let mut lower = 0_usize;
    let mut upper = starts.len();
    #[cfg_attr(creusot, invariant(lower@ <= upper@ && upper@ <= starts@.len()))]
    #[cfg_attr(creusot, invariant(forall<i: Int> 0 <= i && i < lower@
        ==> starts@[i]@ <= max_ts@))]
    #[cfg_attr(creusot, invariant(forall<i: Int> upper@ <= i && i < starts@.len()
        ==> starts@[i]@ > max_ts@))]
    #[cfg_attr(creusot, variant(upper@ - lower@))]
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if starts[middle] <= max_ts {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    let hi = lower;

    lower = 0;
    upper = hi;
    #[cfg_attr(creusot, invariant(lower@ <= upper@ && upper@ <= hi@))]
    #[cfg_attr(creusot, invariant(hi@ <= prefix_ends@.len()))]
    #[cfg_attr(creusot, invariant(forall<i: Int> 0 <= i && i < lower@
        ==> prefix_ends@[i]@ < min_ts@))]
    #[cfg_attr(creusot, invariant(forall<i: Int> upper@ <= i && i < hi@
        ==> prefix_ends@[i]@ >= min_ts@))]
    #[cfg_attr(creusot, variant(upper@ - lower@))]
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if prefix_ends[middle] < min_ts {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    (lower, hi)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::overlap_window;

    #[test]
    fn matches_partition_points_for_bounded_inputs() {
        for len in 0_u32..=6 {
            for mut encoded in 0_u32..5_u32.pow(len) {
                let mut starts = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    starts.push(i64::from(encoded % 5) - 2);
                    encoded /= 5;
                }
                starts.sort_unstable();
                let mut prefix_ends = starts
                    .iter()
                    .enumerate()
                    .map(|(index, start)| *start + i64::try_from(index % 3).unwrap())
                    .collect::<Vec<_>>();
                for index in 1..prefix_ends.len() {
                    prefix_ends[index] = prefix_ends[index].max(prefix_ends[index - 1]);
                }

                for min_ts in -3_i64..=3 {
                    for max_ts in -3_i64..=3 {
                        let hi = starts.partition_point(|start| *start <= max_ts);
                        let lo = prefix_ends[..hi].partition_point(|end| *end < min_ts);
                        check!(overlap_window(&starts, &prefix_ends, min_ts, max_ts) == (lo, hi));
                    }
                }
            }
        }
    }
}
