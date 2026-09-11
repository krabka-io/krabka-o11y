use super::RoleKind;

/// The roles `--target all` runs, in the order a stop walks them.
///
/// Roles that share a process share a stop, and the order is the whole point.
/// The distributor goes first, so that nothing new enters the WAL. The block
/// builder goes next, and flushes and commits the records it has already read;
/// whatever the distributor wrote that it never got to stays in the WAL,
/// uncommitted, for the next start to replay. That set stops growing only
/// because the distributor stopped first. Stop them the other way round and
/// the window between the two stops is a WAL that is still being written to
/// and no longer being read, and every record in it waits for a block builder
/// to come back -- which, on a laptop or on the one-replica deployment this
/// composition exists for, is nothing.
///
/// The read path follows, having nothing to lose by stopping: a querier holds
/// no unflushed state. The compactor is last because its pass is the longest
/// single unit of work in the process and it is the one role whose work is
/// pure rearrangement -- a pass abandoned halfway costs nothing but the pass,
/// and the next process to start replans it from the index.
pub const DRAIN_ORDER: [RoleKind; 6] = [
    RoleKind::Distributor,
    RoleKind::BlockBuilder,
    RoleKind::Querier,
    RoleKind::QueryFrontend,
    RoleKind::Symbolizer,
    RoleKind::Compactor,
];
