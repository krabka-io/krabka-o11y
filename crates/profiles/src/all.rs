//! What a single-process profiles stack is made of, and the order it stops in.
//!
//! `krabka-profiles --target all` runs every profiles role in one process.
//! The composition itself lives in the binary, but the stop order does not:
//! it is a property of the signal's pipeline rather than of one `main`, and a
//! test that cannot see the binary's private modules still has to be able to
//! pin it.

use krabka_observability::RoleKind;

/// The roles `--target all` runs, in the order a stop walks them.
///
/// The distributor stops first so that nothing new enters the WAL. The block
/// builder follows and commits the records it has already read. The read path
/// follows, and the compactor stops last because its pass is the longest unit
/// of work and can be replanned on the next start.
pub const DRAIN_ORDER: [RoleKind; 6] = [
    RoleKind::Distributor,
    RoleKind::BlockBuilder,
    RoleKind::Querier,
    RoleKind::QueryFrontend,
    RoleKind::Symbolizer,
    RoleKind::Compactor,
];
