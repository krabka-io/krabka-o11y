//! Krabka's response to the faults an object-store provider injects.
//!
//! Each case runs over an in-memory store here. The provider contract runs the
//! same cases over a real provider.

mod faults;

use std::sync::Arc;

use assert2::check;
use object_store::{ObjectStore, memory::InMemory};

use self::faults::{
    ChecksumOutcome, StaleListingOutcome, ThrottlingOutcome, checksum_mismatch, stale_listing,
    throttling,
};

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// A throttled put that clears inside the budget writes its payload once. One
/// that outlasts the budget reports the provider's error and writes nothing.
#[tokio::test]
async fn retry_absorbs_throttling_within_its_budget() {
    check!(throttling(store()).await == ThrottlingOutcome::absorbed());
}

/// A rejected upload is resent from the intact payload, then reported. A
/// corrupted read fails the block decode and is not retried as an outage.
#[tokio::test]
async fn a_checksum_mismatch_surfaces_as_a_permanent_error() {
    check!(checksum_mismatch(store()).await == ChecksumOutcome::surfaced());
}

/// The sweep deletes only what it listed and the index does not name, so an
/// object missing from a stale listing is kept until the listing converges.
#[tokio::test]
async fn a_stale_listing_costs_no_live_or_unlisted_block() {
    check!(stale_listing(store()).await == StaleListingOutcome::nothing_lost());
}
