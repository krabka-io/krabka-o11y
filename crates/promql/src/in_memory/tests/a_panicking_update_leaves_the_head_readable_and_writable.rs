use super::*;

/// The hot head is the one piece of shared state a query and the WAL tail both
/// touch, and it used to be written in place under a `std::sync` write lock.
/// A record that panicked part-way through applying would have left a
/// half-applied store behind a poisoned lock, and from that instant every
/// query and every later record on this querier would have panicked on the
/// `expect`. One bad record, one permanently broken role, still listening.
///
/// So: an update that panics leaves the head exactly as the last successful
/// one left it, and the head goes on serving reads and taking writes.
#[tokio::test]
pub(crate) async fn a_panicking_update_leaves_the_head_readable_and_writable() {
    let head = WalHead::new();
    let series = lbls(&[("__name__", "up"), ("job", "api")]);
    head.apply_wal_record(&float_record("t", &series, 1_000, 1.0));

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        head.update(|store| {
            store.apply_wal_record(&float_record("t", &series, 2_000, 2.0));
            panic!("one bad record");
        });
    }));

    // A later record still lands, rather than meeting a poisoned lock.
    head.apply_wal_record(&float_record("t", &series, 3_000, 3.0));

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let scan = head
        .scan("t", &matchers, i64::MIN, i64::MAX)
        .await
        .expect("the head still answers a scan");
    let table = scan.float_table.clone().expect("the series is present");

    assert2::assert!(panicked.is_err());
    // Two rows, not three: the record the panic was part-way through writing
    // was never published, so nothing observes a half-applied update.
    assert2::assert!(count_rows(&scan, &table).await == 2);
}
