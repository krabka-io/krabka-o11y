# Operating envelope

Krabka does not publish a numeric production limit until the complete workload
has reached saturation on the stable qualification runner. Configured tenant
limits are safety controls, not performance claims.

## Fixed qualification shape

The candidate deployment is the checked-in Kubernetes topology: one replica
per role, one broker, one object store, one partition per WAL/state topic,
replication factor one, 15-minute WAL retention, and the CPU/memory requests
and limits in `deploy/kustomization.yaml`. The HA phase renders two WAL
partitions so two block builders and two queriers per signal can own work; it
leaves state topics and replication at one. A report is comparable only when it
names the exact Krabka and broker commits, container digests, Kubernetes
version, node CPU and memory, object-store provider, retention, replication,
dataset seed, warm-up, measurement duration, and command.

The workload must run steady ingest, a burst to saturation, increasing label
cardinality, hot and cold queries, compaction, deletion, restart catch-up, and
one noisy tenant beside a quiet tenant for metrics, logs, traces, and profiles.
For each phase and signal it records accepted and rejected ingest rate,
p50/p95/p99 query latency, errors, RSS, WAL lag, object-store requests and
bytes, and recovery time. The first supported point is the highest load below
which three complete runs remain inside the error and latency objectives; a
configured maximum or a partial run is never promoted.

## Evidence and gates

The scheduled scale soak writes `soak-report.json` with warm-up, duration,
throughput, query quantiles, errors, RSS, and object-store cost. Criterion
writes raw estimates, confidence intervals, runner metadata, duration, and
`SHA256SUMS` below `benches/target/criterion`. Manual mutation sweeps preserve every
shard log plus commit, toolchain, host, command, duration, and checksums.

Object-store cost is counted in requests and bytes. `MeteredObjectStore` in
`krabka-blockstore` counts each request attempt and its payload bytes per
operation. A report writes those counts as the `operations` and
`transferred_bytes` maps, plus their sums as `requests_total` and
`transferred_bytes_total`. The `object-store contract` workflow writes these
fields for the provider suite and for one lifecycle test per signal.
`tools/object-store-evidence.py` rejects a report whose totals are zero or
differ from the sums of their maps. See
[`object_store_contract.md`](object_store_contract.md).

`tools/bench-ratchet.py` rejects missing or noisy measurements and applies
numeric baselines only after a quiet stable runner produces them.
`tools/mutants-ratchet.py` rejects missing, silent, timed-out, or internally
inconsistent shards before comparing survivor counts. A deliberate benchmark
regression or new mutation survivor must fail before either baseline is
reviewed. `tools/mutants-record.py --check` runs on every pull request. It
fails when a baseline number has no recorded run behind it, or when the record
does not name its commit, toolchain, host shape, command, duration, and
checksums.

Until the stable-runner report covers every workload and all four signals,
the supported numeric envelope remains unpublished. Raw shared-runner results
are diagnostic evidence, not a capacity promise.
