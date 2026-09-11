//! The order `--target all` stops its roles in, asserted by watching the
//! tokens fire.
//!
//! Roles that share a process share a stop, and stopping them all at once is
//! the bug this order exists to prevent. A distributor that stopped after the
//! block builder would go on accepting pushes and writing them to a WAL that
//! nothing was reading, and every one of those records then waits for a block
//! builder to come back: on a laptop, or on the one-replica deployment
//! `--target all` exists for, nothing comes back.
//!
//! The assertion is on observed behaviour rather than on the list: each stage
//! records the moment its own token fires, and the recorded sequence is what
//! is checked. A drain that cancelled every token at once would produce the
//! right list and the wrong sequence, and only this catches that.

use std::sync::{Arc, Mutex};

use assert2::{assert, check};
use krabka_observability::{RoleKind, StagedDrain};
use krabka_profiles::all::DRAIN_ORDER;
use krabka_units::secs;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_roles_stop_one_at_a_time_in_the_order_the_pipeline_needs() {
    let stopped: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    let mut drain = StagedDrain::new(secs(10));
    for role in DRAIN_ORDER {
        let stopped = Arc::clone(&stopped);
        drain.stage(role.as_str(), move |token| async move {
            token.cancelled().await;
            stopped
                .lock()
                .expect("the recorded stop order")
                .push(role.as_str());
        });
    }
    let registered = drain.order();

    let overran = drain.drain().await;

    assert!(overran.is_empty(), "every stage finished inside its budget");
    let observed = stopped.lock().expect("the recorded stop order").clone();
    let expected: Vec<&str> = DRAIN_ORDER.iter().map(|role| role.as_str()).collect();
    check!(observed == expected);
    check!(registered == expected);
}

/// The two orderings the pipeline actually depends on, named rather than left
/// implicit in a list comparison, so that a change to either one fails with a
/// message that says what was broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_distributor_stops_before_the_block_builder_and_the_compactor_stops_last() {
    let stopped: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    let mut drain = StagedDrain::new(secs(10));
    for role in DRAIN_ORDER {
        let stopped = Arc::clone(&stopped);
        drain.stage(role.as_str(), move |token| async move {
            token.cancelled().await;
            stopped
                .lock()
                .expect("the recorded stop order")
                .push(role.as_str());
        });
    }

    drain.drain().await;
    let observed = stopped.lock().expect("the recorded stop order").clone();

    let distributor = position_of(&observed, RoleKind::Distributor);
    let block_builder = position_of(&observed, RoleKind::BlockBuilder);
    check!(
        distributor < block_builder,
        "nothing may enter the WAL while the block builder is still draining it"
    );
    check!(
        observed.last() == Some(&RoleKind::Compactor.as_str()),
        "the compactor has the least to lose and so goes last"
    );
}

/// `--target all` runs every profiles role and no more, so the order has to
/// name every one of them exactly once.
///
/// A role missing here is a role that never starts: `run_all` takes its stages
/// from this list. A repeated one is a role whose stage is asked for twice and
/// found once.
#[test]
fn the_order_names_every_profiles_role_exactly_once() {
    let mut named: Vec<RoleKind> = DRAIN_ORDER.to_vec();
    named.sort_unstable();

    let mut expected = vec![
        RoleKind::Distributor,
        RoleKind::BlockBuilder,
        RoleKind::Querier,
        RoleKind::QueryFrontend,
        RoleKind::Compactor,
        RoleKind::Symbolizer,
    ];
    expected.sort_unstable();

    check!(named == expected);
    check!(
        !DRAIN_ORDER.iter().any(|role| role.is_composite()),
        "`all` is the composition, not one of the things composed"
    );
}

fn position_of(observed: &[&'static str], role: RoleKind) -> usize {
    observed
        .iter()
        .position(|name| *name == role.as_str())
        .unwrap_or_else(|| panic!("{role} never stopped"))
}
