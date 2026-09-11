//! The order `--target all` stops its roles in.
//!
//! The order is a data-loss argument, not a preference. The distributor
//! acknowledges a push only once its WAL append has been acknowledged, so
//! every span it has answered `200` for is in the WAL; the block builder is
//! what turns those records into a block. Cancel both at the same instant and
//! the records accepted in the last second sit in the WAL, in no block,
//! waiting for a restart that on one machine never comes. That loss is silent:
//! the client got its `200`, the logs say a clean shutdown, and the span is
//! gone.
//!
//! So the order is checkable, and these tests check it two ways: that the list
//! the process is built from says what the argument requires, and that a
//! [`StagedDrain`] built from that list really does fire its tokens one at a
//! time, in that order, waiting for each stage before asking the next to stop.

use std::sync::{Arc, Mutex};

use assert2::{assert, check};
use krabka_observability::{RoleKind, StagedDrain};
use krabka_traces::all_in_one::DRAIN_ORDER;
use krabka_units::secs;

/// Every role exactly once, and no composite in the list.
///
/// `RoleKind::All` names the composition rather than a stage of it, and a
/// process that staged itself would never finish stopping.
#[test]
fn the_drain_order_names_each_role_of_the_composition_once() {
    let mut sorted = DRAIN_ORDER.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    check!(sorted.len() == DRAIN_ORDER.len(), "a role appears twice");
    check!(
        !DRAIN_ORDER.iter().any(|role| role.is_composite()),
        "a composite role cannot be a stage of itself"
    );
}

/// The distributor stops before the block builder, and the compactor last.
///
/// These are the two ends of the argument. Nothing may still be entering the
/// WAL when the role that drains it is asked to stop, and the one role whose
/// work is pure housekeeping is the one that can be interrupted for free.
#[test]
fn the_write_path_stops_before_the_role_that_drains_it() {
    assert!(let Some(distributor) = position(RoleKind::Distributor));
    assert!(let Some(block_builder) = position(RoleKind::BlockBuilder));
    assert!(let Some(metrics_generator) = position(RoleKind::MetricsGenerator));
    assert!(let Some(live_store) = position(RoleKind::LiveStore));
    assert!(let Some(querier) = position(RoleKind::Querier));
    assert!(let Some(query_frontend) = position(RoleKind::QueryFrontend));
    assert!(let Some(compactor) = position(RoleKind::Compactor));

    check!(distributor < block_builder, "nothing may still be writing");
    check!(
        block_builder < live_store && metrics_generator < live_store,
        "the WAL consumers drain before the read path goes"
    );
    check!(
        live_store < querier && querier < query_frontend,
        "the read path stops from the bottom up"
    );
    check!(
        compactor == DRAIN_ORDER.len() - 1,
        "the only role that loses nothing when interrupted goes last"
    );
}

/// A drain built from the order really stops stages one at a time, in it.
///
/// Each stage records the moment its own token fires and then takes a visible
/// amount of time to finish. If `StagedDrain` cancelled them all at once, or
/// cancelled the next before the previous had returned, the recording would
/// come out in some other order -- or in this order but with the stages
/// overlapping, which the "stopped" marks below would show. This is the
/// property `--target all` is built on: the block builder's token fires only
/// after the distributor's stage has actually returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_over_the_order_cancels_one_stage_at_a_time() {
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let mut drain = StagedDrain::new(secs(5));
    for role in DRAIN_ORDER {
        let log = Arc::clone(&log);
        drain.stage(role.as_str(), move |token| async move {
            token.cancelled().await;
            log.lock()
                .expect("the log mutex")
                .push(format!("{role} cancelled"));
            // Long enough that a drain cancelling the next stage early would
            // interleave the marks below rather than keeping them in pairs.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            log.lock()
                .expect("the log mutex")
                .push(format!("{role} stopped"));
        });
    }
    check!(drain.order() == DRAIN_ORDER.map(RoleKind::as_str).to_vec());

    let overran = drain.drain().await;
    check!(overran.is_empty(), "no stage needed more than its budget");

    let expected: Vec<String> = DRAIN_ORDER
        .iter()
        .flat_map(|role| [format!("{role} cancelled"), format!("{role} stopped")])
        .collect();
    let recorded = log.lock().expect("the log mutex").clone();
    check!(recorded == expected);
}

fn position(role: RoleKind) -> Option<usize> {
    DRAIN_ORDER.iter().position(|entry| *entry == role)
}
