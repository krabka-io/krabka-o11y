use super::RoleKind;

/// The order `--target all` stops its roles in, first to last.
///
/// A single token cancelled for all seven roles at once would be wrong, and
/// wrong in a way nothing reports. The distributor acknowledges a push only
/// after its WAL append has been acknowledged, so every span it has answered
/// `200` for is in the WAL. The block builder is what turns those records into
/// a block. Stop both at the same instant and the records the distributor
/// accepted in its last second are in the WAL, in no block, waiting for
/// something to restart and read them -- which in the deployment this target
/// exists for, one process on one machine, is nothing at all. The push was
/// acknowledged and the span is gone.
///
/// So the order is a data-loss argument, read top to bottom:
///
/// 1. **`distributor`** -- stops accepting, so nothing new enters the WAL.
///    Its graceful shutdown lets the pushes already in flight finish their
///    appends first, which is why it is a stop rather than an abort.
/// 2. **`block-builder`** -- now has a WAL nobody is writing to. It finishes
///    the window it is polling, flushes what it has buffered, and commits the
///    offsets, so what step 1 accepted is in a block.
/// 3. **`metrics-generator`** -- the other WAL consumer, and the other thing
///    with unflushed state: its final collection remote-writes the span
///    metrics it had accumulated. It stops beside the block builder because it
///    reads the same records, and after it because losing derived metrics
///    costs less than losing the spans they were derived from.
/// 4. **`live-store`**, 5. **`querier`**, 6. **`query-frontend`** -- the read
///    path, stopped from the bottom up so a query in flight is never handed to
///    a tier that has already gone. None of them holds anything that is not
///    also in the WAL or in a block, so none of them can lose a write.
/// 7. **`compactor`** -- last, because it is the only role whose work is pure
///    housekeeping. A merge abandoned halfway publishes nothing: the index is
///    saved after the replacement blocks are durable, so an interrupted pass
///    leaves the pre-merge blocks exactly as they were and the next start
///    replans from them.
///
/// [`RoleKind::All`] is not in the list. It names the composition, not a stage
/// of it, and a process that staged itself would never finish stopping.
pub const DRAIN_ORDER: [RoleKind; 7] = [
    RoleKind::Distributor,
    RoleKind::BlockBuilder,
    RoleKind::MetricsGenerator,
    RoleKind::LiveStore,
    RoleKind::Querier,
    RoleKind::QueryFrontend,
    RoleKind::Compactor,
];
