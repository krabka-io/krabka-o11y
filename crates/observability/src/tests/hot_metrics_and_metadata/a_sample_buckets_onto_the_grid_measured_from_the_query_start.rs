use super::*;

/// `sample_time_bucket` floors a sample onto the step grid measured FROM
/// the query's start, not from the epoch. A start that is not itself a
/// multiple of the step is what shows that: with start 0 the two are the
/// same arithmetic, and every bucket would look right.
///
/// A sample before the start clamps to the start rather than producing a
/// bucket before the window began. The `<=` in that guard could be `<`
/// without changing any answer -- at exactly the start the arithmetic
/// yields the start anyway -- so relaxing it is an equivalent mutation.
/// The guard as a whole is not: a sample below the start would otherwise
/// floor to a negative offset.
#[test]
pub(crate) fn a_sample_buckets_onto_the_grid_measured_from_the_query_start() {
    let bucket = super::super::prelude::sample_time_bucket;
    // 1_000 is deliberately not a multiple of 300.
    let (start, step) = (1_000_i64, 300_i64);

    // The grid runs 1000, 1300, 1600 -- not 900, 1200, 1500, which is what
    // flooring from the epoch would give.
    check!(
        bucket(1_000, start, step) == 1_000,
        "the start is its own bucket"
    );
    check!(bucket(1_001, start, step) == 1_000);
    check!(bucket(1_299, start, step) == 1_000, "one short of the next");
    check!(bucket(1_300, start, step) == 1_300, "exactly on the next");
    check!(bucket(1_301, start, step) == 1_300);
    check!(bucket(1_900, start, step) == 1_900, "three steps along");
    check!(bucket(2_000, start, step) == 1_900);

    // At or before the start, clamped.
    check!(bucket(999, start, step) == 1_000);
    check!(bucket(0, start, step) == 1_000);
    check!(bucket(-1_000, start, step) == 1_000);

    // A start of zero is the degenerate case where flooring from the start
    // and from the epoch agree -- pinned so the distinction above is not
    // mistaken for the only behaviour.
    check!(bucket(700, 0, step) == 600);
    check!(bucket(600, 0, step) == 600);
}
