use super::*;

/// `eligible_tail_record_count` holds a tail back by `delay_for`, so a
/// consumer sees only records old enough that nothing earlier can still
/// arrive. It counts with `take_while` rather than `filter`: the WAL is
/// ordered, so the first record too new to send BLOCKS the ones after it
/// even if those happen to be older. Sending past it would emit records
/// out of order, which is worse than sending them late.
#[test]
pub(crate) fn a_tail_holds_back_records_newer_than_its_delay() {
    let record = |timestamp_ns| super::super::prelude::WalLogRecord {
        tenant: "tenant".to_string(),
        labels: Labels::default(),
        timestamp_ns,
        line: "line".to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };
    let count = super::super::prelude::eligible_tail_record_count;
    // Comfortably either side of now, so the wall clock cannot straddle
    // them however long the test takes to reach this line.
    let old = 1_000_000_000_000_i64;
    let future = i64::MAX / 2;

    // No delay means no holding back, whatever the timestamps.
    check!(count(&[record(old), record(future)], 0) == 2);
    check!(
        count(&[record(future)], -1) == 1,
        "a negative delay is not a delay"
    );

    // With a delay, old records are eligible and future ones are not.
    check!(count(&[record(old), record(old)], 1) == 2);
    check!(count(&[record(future)], 1) == 0);

    // The cutoff is `now - delay`, and only a record BETWEEN the two
    // possible cutoffs shows that: with an hour's delay a record stamped
    // now is held back, where `now + delay` would have released it. A
    // one-nanosecond delay moves the cutoff too little to tell.
    let hour_ns = 3_600 * 1_000_000_000_i64;
    check!(
        count(&[record(super::prelude::current_unix_time_ns())], hour_ns) == 0,
        "a record stamped now is newer than an hour ago"
    );

    // The first ineligible record stops the count: the second record here
    // is old enough on its own, and is still held back.
    check!(
        count(&[record(future), record(old)], 1) == 0,
        "take_while, not filter"
    );
    check!(count(&[record(old), record(future), record(old)], 1) == 1);

    check!(count(&[], 1) == 0);
    check!(count(&[], 0) == 0);
}
