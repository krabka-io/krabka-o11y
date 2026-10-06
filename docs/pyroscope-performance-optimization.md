# Pyroscope correctness and performance

The historical [Grafana comparison](grafana-performance-comparison.md) disqualified
profiles because hot WAL samples and their published cold copies were both
counted. The original performance ratios remain unqualified.

The earlier image (`23e8b1ed7162055b86974064be52d450ef3cdbf5`) reproduces this after the querier refreshes its cold
index: an immutable seed with 1,000 expected samples returns 2,000. The immediate
seed query returns 1,000, so checking only that first response misses the bug.
The comparison harness now checks the seed again after an untimed 20-second
wait, on both backends, before warm-up and measurement.

## Handoff

Published profile blocks carry inclusive WAL partition and offset ranges in
the profile index. Compaction preserves those ranges, and shard publication
and loading preserve them with the block metadata. Cold rows and coverage come
from the same captured index snapshot, including when retired blocks require
a retry. The hot scan excludes only records covered by that snapshot.

Distinct uploads with identical labels, timestamps, and stacks remain distinct.
Unpublished offsets in gaps and offsets from other partitions remain queryable.
The profile shard encoding is version 2; readers accept that exact version.

The regression test compares the complete flamegraph before and after block
publication, snapshot reload, and compaction. Its control using a hot snapshot
without source positions reproduces the double count. It also checks that a
captured cold scan and its coverage remain consistent after index replacement.
The encoding test checks exact large offsets and rejects invalid versions and
malformed offsets.

## Query sessions

Profile queries initialize DataFusion contexts for hot, cold, and combined
scans. A diagnostic CPU profile identified repeated function and session setup;
its small sample count only supports choosing a change to measure. The query
context factory initializes default functions and the runtime once, then clones
the state with a fresh catalog and schema for each query. Concurrent queries
can each register `samples` without replacing another query's table.

The concurrency test registers eight different tables under that same name
and checks each complete result, including a default SQL function.

## Measurement contract

Cold queries additionally overlap up to four block reads in deterministic
order. A nine-block regression compares complete symbols and totals with an
independent hot-store flamegraph. Removing one block must fail the query. All
33 scoped pprof/profiles test and Clippy targets and all eight real
Pyroscope/Grafana differential tests passed with this change.

The [private GCP performance record](grafana-performance-gcp.md) covers the
latest source and deployment layouts.

Use the [reusable harness](grafana-performance-comparison.md#provenance-and-reproduction)
with `--signal profiles`. The immutable seed is checked before and after the
cold handoff. Three fresh deployments per backend alternate order, with 15
seconds of warm-up and 60 seconds of measured steady load. Both backends have
the same aggregate service budget and a separate MinIO budget.

The seed has 100 uploads with 10 samples each. Measured traffic uses 100
series, two writers sending one upload/s each, and one reader capped at four
queries/s. The seed stays immutable until both exact checks complete.

CPU includes all application roles, the broker where present, and MinIO. RSS
is the sampled peak simultaneous sum across those containers. API write
acknowledgements have different durability contracts. Local measurements on the shared development host are diagnostic. Qualification
uses the private Google Cloud workflow. It can measure the split and all
layouts sequentially on one VM; the GCP record states the layout of each result.
These API measurements do not establish equivalent durable throughput.

## Validation

The handoff fix passed 35 scoped Bazel test and Clippy targets across pprof,
profiles, and blockstore. All eight Docker differential tests against pinned
Pyroscope 2.3.1 and Grafana passed, covering render values, profile types,
metadata, ingest formats, OTLP, symbol upload, and Grafana datasource use.

The session optimization passed all 33 scoped pprof and profiles test and
Clippy targets, including concurrent catalog isolation and the handoff test.
The real Pyroscope render comparison passed again with that optimization.
Harness self-tests, Python compilation, scoped formatting, and diff checks passed.

The corrected baseline is source
`49e79adb1c37fec4b9bace44a5e6c903e89d8398`. The session candidate is
`43cb538f10d06ee04fcc1c4b8024101363f3bd92`. Both use the same harness file
hashes and pinned upstream image.
