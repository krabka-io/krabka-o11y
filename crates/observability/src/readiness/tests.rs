use assert2::{assert, check};

use super::{DRAINING_GATE, RoleKind, RoleReadiness};

/// A role with nothing to wait for is ready from construction. That is the
/// honest answer for a role whose whole startup runs before its listener
/// binds, and getting it wrong would leave such a role permanently out of
/// rotation.
#[test]
fn a_role_with_no_gates_is_ready() {
    let readiness = RoleReadiness::new();

    check!(readiness.is_ready());
    check!(readiness.pending().is_empty());
}

/// An all-in-one process is ready only when every role it runs is, and
/// `/ready` has to say which one is holding it back. Three roles that each
/// register an `object-store` gate would otherwise report `object-store,
/// object-store, object-store` and name none of them.
#[test]
fn one_process_reports_every_role_it_runs_and_names_each_gate() {
    let process = RoleReadiness::new();
    let block_builder = process
        .for_role(RoleKind::BlockBuilder)
        .gate("object-store");
    let querier = process.for_role(RoleKind::Querier).gate("object-store");

    check!(!process.is_ready());
    check!(process.pending() == ["block-builder/object-store", "querier/object-store"]);

    block_builder.mark_ready();
    check!(process.pending() == ["querier/object-store"]);
    check!(!process.is_ready());

    querier.mark_ready();
    assert!(process.is_ready());
}

/// A role-scoped view shares the process's gates rather than copying them.
/// A view with a list of its own would leave `/ready` reporting whichever
/// subset the probe happened to hold.
#[test]
fn a_role_view_registers_into_the_process_readiness() {
    let process = RoleReadiness::new();
    let distributor = process.for_role(RoleKind::Distributor);
    let gate = distributor.gate("wal-broker");

    check!(process.pending() == ["distributor/wal-broker"]);
    check!(distributor.pending() == ["distributor/wal-broker"]);

    gate.mark_ready();
    check!(process.is_ready());
    check!(distributor.is_ready());
}

/// A drain is asked about by name, and in an all-in-one that name arrives
/// with a role in front of it. A check that compared the whole string would
/// read a draining process as merely starting, and send a runbook the wrong
/// way at the one moment it is being read.
#[test]
fn the_drain_gate_is_found_whichever_role_registered_it() {
    let process = RoleReadiness::new();
    let gate = process.for_role(RoleKind::Distributor).gate(DRAINING_GATE);
    gate.mark_ready();

    check!(!process.is_pending(DRAINING_GATE));

    gate.mark_unready();

    check!(process.is_pending(DRAINING_GATE));
    check!(!process.is_pending("wal-consumer"));
}

/// A gate goes back down when what it stood for goes away. A background task
/// that dies takes its role out of rotation without stopping the process,
/// which is the difference between readiness and liveness.
#[test]
fn a_gate_that_loses_what_it_had_takes_the_role_out_of_rotation() {
    let readiness = RoleReadiness::new();
    let gate = readiness.gate("wal-consumer");
    gate.mark_ready();
    assert!(readiness.is_ready());

    gate.mark_unready();

    check!(!readiness.is_ready());
    check!(readiness.pending() == ["wal-consumer"]);
}
