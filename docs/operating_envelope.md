# Operating envelope

Krabka does not publish a numeric production limit until the complete workload
has reached saturation on the stable qualification runner. Configured tenant
limits are safety controls, not performance claims.

## Fixed qualification shape

The candidate deployment is the checked-in Kubernetes topology: one replica
per role, one broker, one object store, one partition per WAL/state topic,
replication factor one, 15-minute WAL retention, and the CPU/memory requests
and limits in `deploy/kustomization.yaml`. A report is comparable only when it
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
`SHA256SUMS` below `benches/target/criterion`. Mutation sweeps preserve every
shard log plus commit, toolchain, host, command, duration, and checksums.

`tools/bench-ratchet.py` rejects missing or noisy measurements and applies
numeric baselines only after a quiet stable runner produces them.
`tools/mutants-ratchet.py` rejects missing, silent, timed-out, or internally
inconsistent shards before comparing survivor counts. A deliberate benchmark
regression or new mutation survivor must fail before either baseline is
reviewed.

Until the stable-runner report covers every workload and all four signals,
the supported numeric envelope remains unpublished. Raw shared-runner results
are diagnostic evidence, not a capacity promise.

