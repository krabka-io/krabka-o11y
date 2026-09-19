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

The blockstore remains responsible for grouping candidates by tenant, level,
and time bucket and for sorting each group. Those are adapter preconditions,
not claims made by the kernels. The existing blockstore tests cover that
adapter and compare its observable plans.

Creusot is pinned by `.creusot-version`; checked-in sessions live under
`verif/krabka_o11y_verified_rlib`. Replay them with:

```console
bash tools/creusot-prove.sh
```

## Stateright model

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
