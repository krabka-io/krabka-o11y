# Formal verification

Krabka observability uses formal verification only where it strengthens an
actual production boundary. Creusot proves pure storage-lifecycle kernels;
Stateright exhaustively enumerates a bounded durable-publication protocol.

## Creusot kernels

`krabka-o11y-verified` contains the code called by `krabka-blockstore`:

| Kernel | Proved contract |
| --- | --- |
| `compaction_run_ends` | Every run is contiguous, contains at least two blocks, stays within the fan-in cap, and leaves at most one trailing block. All arithmetic and the loop are safe and terminating. |
| `retention_cutoff` | Non-positive retention keeps data forever; positive retention subtracts without overflowing the timestamp range. |
| `overlap_window` | Two binary searches return exactly the slice that can overlap a query: every earlier prefix ends too soon and every later block starts too late. The searches are safe and terminating. |
| `bounded_shard_slots` | A bounded grid span produces every consecutive shard slot exactly once; an oversized span produces the unbounded fallback without overflowing at either end of `i64`. |
| `shard_range` | A positive-width grid slot produces an ordered inclusive range whose span is at most the requested width, with both timestamp edges clamped safely. |

The blockstore remains responsible for grouping compaction candidates and for
maintaining the index's sorted start times and prefix maximum end times. Those
are adapter preconditions, not claims made by the kernels. The existing
blockstore tests cover those adapters and compare their observable plans.

Creusot is pinned by `.creusot-version`; checked-in sessions live under
`verif/krabka_o11y_verified_rlib`. Replay them with:

```console
bash tools/creusot-prove.sh
```

## Stateright models

`storage_protocol_model` enumerates two- and three-partition compaction batches
through block write, index publication, assignment commit, transient failure,
crash, and replay. Its safety properties require published blocks to be
durable, committed offsets to be published, and assignment commits to be
atomic. Reachability properties ensure crash, failure, and replay paths are
not vacuous. Exact reachable-state counts are pinned.

The model assumes a stable consumer-group assignment and object-store puts
that report durability accurately. Partition revocation is excluded: the
traces block-builder documents the current buffered-window limitation, and
GitHub issue #266 owns that larger handoff redesign.

`index_snapshot_protocol_model` enumerates two concurrent snapshot writers
through an add and a removal, generation reads, immutable payload writes,
conditional manifest publication, retained generations, acknowledgement,
crashes, retries, aging, and version-conditional orphan reclamation. Its safety
properties require every retained manifest and unexpired publication payload
to remain durable and require an acknowledgeable writer's effect never to
disappear. Reachability covers conflict merging, publication expiry, stale
sweep invalidation, post-publication removal replay, and reclamation; the exact
113,426-state graph is pinned.

`block_deletion_protocol_model` enumerates sidecar deletion, block deletion,
failures, crashes, and later sweep replay. Its safety property requires that a
deleted block has no remaining sidecars; reachability covers completion after
failure and crash and idempotent replay of an already absent sidecar. Exact
two- and three-sidecar state counts are pinned.
