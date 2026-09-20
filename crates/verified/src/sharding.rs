#[cfg(creusot)]
use creusot_std::prelude::*;

/// Enumerates every inclusive grid slot from `first` through `last`.
///
/// An empty result means the span meets or exceeds the caller's slot cap. The
/// loop increments only while below `last`, so even a span ending at
/// [`i64::MAX`] cannot overflow.
#[cfg_attr(creusot, requires(first@ <= last@))]
#[cfg_attr(creusot, requires(max_slots@ > 0))]
#[cfg_attr(creusot, ensures((result@.len() == 0)
    == (last@ - first@ >= max_slots@)))]
#[cfg_attr(creusot, ensures(result@.len() > 0
    ==> result@.len() == last@ - first@ + 1))]
#[cfg_attr(creusot, ensures(forall<i: Int> 0 <= i && i < result@.len()
    ==> result@[i]@ == first@ + i))]
#[must_use]
pub fn bounded_shard_slots(first: i64, last: i64, max_slots: i64) -> Vec<i64> {
    if last.saturating_sub(first) >= max_slots {
        return Vec::new();
    }

    let mut slots: Vec<i64> = Vec::new();
    let mut slot = first;
    #[cfg_attr(creusot, invariant(first@ <= slot@ && slot@ <= last@))]
    #[cfg_attr(creusot, invariant(slots@.len() == slot@ - first@))]
    #[cfg_attr(creusot, invariant(forall<i: Int> 0 <= i && i < slots@.len()
        ==> slots@[i]@ == first@ + i))]
    #[cfg_attr(creusot, variant(last@ - slot@))]
    while slot < last {
        slots.push(slot);
        slot += 1;
    }
    slots.push(last);
    slots
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::bounded_shard_slots;

    #[test]
    fn enumerates_exactly_up_to_the_cap() {
        for first in -8_i64..=8 {
            for last in first..=8 {
                for max_slots in 1_i64..=8 {
                    let got = bounded_shard_slots(first, last, max_slots);
                    let count = last - first + 1;
                    if count > max_slots {
                        check!(got.is_empty());
                    } else {
                        check!(got == (first..=last).collect::<Vec<_>>());
                    }
                }
            }
        }
    }

    #[test]
    fn rejects_an_unrepresentably_wide_span_without_overflow() {
        check!(bounded_shard_slots(i64::MIN, i64::MAX, 32).is_empty());
        check!(bounded_shard_slots(i64::MAX, i64::MAX, 32) == vec![i64::MAX]);
    }
}
