# Krabka versus Loki, Mimir, Tempo and Pyroscope

For subsequent changes on this branch, see the
[local optimization measurements](grafana-performance-optimization.md) and
[Pyroscope handoff and performance work](pyroscope-performance-optimization.md).
The [private GCP performance record](grafana-performance-gcp.md) describes the
subsequent source changes and measurement contract.
The tables below retain the historical issue 267 results.

Measured on 2026-10-04 using the installed [Cyclenerd Google Cloud GitHub runner](https://github.com/Cyclenerd/google-cloud-github-runner). This compares accepted API work in fixed, single-node deployment shapes, with one active signal at a time. The backends acknowledge writes at different durability boundaries; these are not equivalent durable-throughput results. The issue 267 [operating envelope](operating_envelope.md) and its durability qualification remain separate.

## Steady workload

Each backend has three paired 60-second measurements after 15 seconds of warm-up. Two writers each send up to one request per second. One reader sends up to four queries/s and waits for each response. Accepted rows/s and query counts show the actual completed load. Different query response times therefore produce different completed query loads. Values below are the median of three measurements; brackets show minimum–maximum. CPU includes the application containers, broker when present, and MinIO. RSS is the peak simultaneous sum of process RSS, sampled approximately once per second.

The profiles comparison is correctness-disqualified by double-counted data. Its timing/resource rows are diagnostic only; no ratios are qualified.

| Signal | Backend | Accepted rows/s | Average vCPU | Peak RSS MiB | Query p99 ms | Query attempts |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| metrics | Krabka | 1983 [1967–1989] | 1.153 [1.145–1.158] | 717.3 [716.9–721.3] | 878.5 [849.0–935.2] | 122–124 |
| metrics | Mimir 3.2.1 | 1997 [1997–1997] | 0.285 [0.233–0.291] | 300.4 [291.9–305.4] | 21.0 [16.6–21.4] | 240–240 |
| logs | Krabka | 1993 [1992–1995] | 0.379 [0.340–0.408] | 406.4 [401.2–479.1] | 117.7 [109.8–121.2] | 239–240 |
| logs | Loki 3.7.8 | 1974 [1973–1974] | 0.337 [0.336–0.343] | 365.4 [352.5–368.9] | 81.4 [81.3–81.9] | 239–239 |
| traces | Krabka | 198 [198–199] | 0.685 [0.675–0.686] | 719.7 [717.6–724.9] | 170.1 [168.5–172.3] | 234–235 |
| traces | Tempo 3.1.0 | 198 [198–198] | 0.539 [0.533–0.544] | 541.5 [537.0–543.6] | 101.1 [100.6–104.1] | 231–232 |
| profiles | Krabka | 20 [20–20] | 0.130 [0.124–0.134] | 443.1 [440.2–446.7] | 21.9 [21.7–23.6] | 240–240 |
| profiles | Pyroscope 2.3.1 | 20 [20–20] | 0.329 [0.322–0.331] | 404.8 [391.9–407.0] | 21.8 [21.5–22.9] | 240–240 |

| Krabka / native median | Container CPU | Peak RSS | Query p99 |
| --- | ---: | ---: | ---: |
| metrics | 4.05× | 2.39× | 41.79× |
| logs | 1.12× | 1.11× | 1.45× |
| traces | 1.27× | 1.33× | 1.68× |
| profiles | unqualified | unqualified | unqualified |

8/8 backends met the steady API latency/error objective in all three repetitions. Ratios compare this schedule and deployment layout, including each backend's monitoring overhead. They do not hold completed query throughput constant. A smaller CPU or RSS ratio favors Krabka; a larger query-p99 ratio means slower Krabka queries.

## Writer and cardinality ramps

Each step has 7.5 seconds of warm-up and 30 seconds of measurement. Writers are closed-loop and paced at one request/s; they slow when response time exceeds that interval. Earlier tenants remain in each phase, so later steps include their background work. A fresh deployment and fresh volumes separate phases. Ramps stop at the first missed objective. The table reports the highest passing tested step in each repetition, not an absolute capacity. A value 256 means the configured top step passed; no higher step was tested.

| Signal | Backend | Passing writers, repetitions 1/2/3 | Passing cardinality, repetitions 1/2/3 |
| --- | --- | --- | --- |
| metrics | Krabka | 4/4/4 | 20000/20000/20000 |
| metrics | Mimir | 256/256/256 | 20000/20000/20000 |
| logs | Krabka | 64/64/64 | 5000/1000/1000 |
| logs | Loki | 64/2/32 | 20000/5000/5000 |
| traces | Krabka | 8/8/8 | 1000/1000/1000 |
| traces | Tempo | 256/256/256 | 20000/20000/20000 |
| profiles | Krabka | 256/128/32 | none/none/none |
| profiles | Pyroscope | 128/128/128 | 20000/20000/20000 |

Loki missed query-tail objectives at 128, 4 and 64 writers, respectively, with no HTTP errors or OOMs at those points. This variation does not establish a clean ingestion-saturation limit. In those 30-second steps, query counts were 14, 86 and 41; nearest-rank p99 is the maximum with fewer than 100 queries. Its 20,000-cardinality phase passed once and missed query p99 twice.

Krabka logs passed 64 writers in every repetition and missed 128 on ingest p99. Its querier was OOM-killed at 20,000 cardinality in repetition 1 and 5,000 in repetitions 2/3. The 1 GiB per-role limit is material even though the deployment has spare aggregate memory. Other failure points, query counts, telemetry gaps, role CPU and OOM evidence are preserved in the machine-readable summary and raw archives.

Krabka profiles failed the exact seed-value check. In repetition 1, 1,000 requests with 10 samples each returned 20,000 rather than 10,000; repetitions 2/3 returned 18,870 and 19,010 for the same expected 10,000. Its [union query store](https://github.com/krabka-io/krabka-o11y/blob/47ddda37177501c43c823f17db59ee149340b436/crates/pprof/src/union_store/union_profile_store.rs#L34-L46) concatenates overlapping hot and cold batches. The flamegraph sums both contributions. The third burst phase also failed at the 64-writer seed: 100 uploads returned 1,010 samples instead of 1,000. `sampleRate=1e9` converts each sample by factor 1, so the oracle is correct. Profile CPU and timing numbers above are diagnostic observations of this behavior. Positive responses in steady/burst phases do not establish correct aggregation; no profile performance winner or supported cardinality is qualified by this run.

Krabka metrics passed 4 writers in all three repetitions and missed 8 on query p99. Mimir passed the top tested 256 writers in all three, at a median 248,495 accepted samples/s; both backends passed 20,000 series. These are API results against Mimir classic-head storage.

Krabka traces passed 8 writers in all three repetitions and was OOM-killed at 16 in its 1 GiB live-store. The same role was OOM-killed during the 5,000-series seed in each repetition. Tempo passed 256 writers and 20,000 series in all three; median accepted throughput at 256 was 24,580 spans/s versus 785 at the last consistently passing Krabka step.

## Measurement contract

Both implementations for a signal run sequentially on the same fresh GCP e2-standard-16 VM: 16 vCPU, 64 GiB RAM and 600 GB SSD. Different signals use different VMs. Repetitions alternate which backend starts first. Exact CPU models, kernels, Compose configurations, container states, operations and telemetry are archived.

| Signal | Application budget, each backend | Common additional MinIO | Request contents |
| --- | --- | --- | --- |
| metrics | 6 vCPU / 6 GiB | 2 vCPU / 2 GiB | 1,000 series × 1 sample, Remote Write v1/literal Snappy blocks |
| logs | 5 vCPU / 5 GiB | 2 vCPU / 2 GiB | 100 streams × 10 rows |
| traces | 7 vCPU / 7 GiB | 2 vCPU / 2 GiB | 10 series × 10 spans, OTLP protobuf |
| profiles | 6 vCPU / 6 GiB | 2 vCPU / 2 GiB | 1 profile × 10 folded-stack samples, nanoseconds |

Native single binaries pool the entire application budget. Krabka divides it among 1 vCPU/1 GiB application roles and a 2 vCPU/2 GiB broker. Equal aggregate budgets therefore do not mean equally provisioned query engines. Docker imposes these [per-container resource ceilings](https://docs.docker.com/engine/containers/resource_constraints/).

The random seed is 267. Burst cardinality is 1,000 for metrics and 100 for other signals. Cardinality ramps test 1,000, 5,000 and 20,000 labels at two writers. Seed traffic uses 64 threads before measurement; Krabka seed WAL is drained and both backends must return nonempty queries. Exact independent seed totals are checked for metrics and profiles. Measured queries require nonempty results; this experiment does not measure complete write-to-query visibility latency.

The shared objective requires zero ingest/query HTTP errors, no empty queries, ingest/query p99 ≤ 2 seconds and no telemetry scrape errors. Krabka also checks its observable maintenance error counters. The metrics rerun additionally rejects insufficient CPU/S3 samples and sample coverage below 90%; the offline analysis checks that coverage for all cost comparisons. RSS excludes kernel and page-cache charges and can miss spikes between samples. CPU comes from container cgroup counters, including all threads. Driver CPU is reported separately in the summary.

Native storage contracts and buffering differ:

- Loki uses RF1, TSDB v13, a persistent local WAL and S3 chunks. Its [WAL durability contract](https://grafana.com/docs/loki/latest/operations/storage/wal/) differs from broker-backed acceptance. Chunk max age is 30 seconds, idle period 5 seconds and flush checks 2 seconds.
- Mimir uses classic ingesters with local TSDB WAL, two-hour blocks and 13-hour head retention. Its Kafka ingest-storage path is disabled. This window mainly measures in-memory head queries; it does not qualify the [Kafka production path](https://grafana.com/docs/mimir/latest/configure/configure-kafka-backend/).
- Tempo uses monolithic `target=all`, which [bypasses Kafka](https://grafana.com/docs/tempo/latest/reference-tempo-architecture/deployment-modes/). Distributed Kafka ingestion is unmeasured.
- Pyroscope v2 writes segments directly to S3 and publishes metadata through a single-peer Raft metastore. Segment duration is 500 ms. Its [write acknowledgement](https://grafana.com/docs/pyroscope/latest/reference-pyroscope-v2-architecture/about-pyroscope-v2-architecture/) includes object and metadata work; it is not Kafka-only acceptance.
- Krabka uses the previously qualified release image and digest-pinned broker. It acknowledges its broker WAL and builds objects asynchronously.

MinIO S3 request/read/write counters cover the measurement window only. Native buffers may defer uploads until later; these counters are not lifetime storage costs. No cold-read, HA, retention, long-running maintenance or distributed-production qualification is claimed. The high-cardinality query window is 30 minutes while writes continue; it is not a cold-read proof. The results cannot be summed to estimate a combined four-signal deployment sharing one broker. Profile aggregation remains disqualified for correct aggregation even though the earlier operating-envelope checks accepted nonempty responses.

## Provenance and reproduction

Metrics were measured at `67a30ac`; logs/traces/profiles at `ed6c3a4`. The earlier metrics job was discarded after Python payload encoding starved telemetry. The corrected encoder caches immutable label/value prefixes; an independent uncached encoder checks exact payload bytes for three cardinalities and two sequences. Both members of each valid pair use the same harness commit. Report identities were retained.

| Signal | Successful measurement job | Harness commit | Verified raw files |
| --- | --- | --- | ---: |
| metrics | [111474566237](https://github.com/krabka-io/krabka-o11y/actions/runs/37215333087/job/111474566237) | `67a30ac579bbf7a5f9db79641cb49337949ed0b0` | 890 |
| logs | [111469137964](https://github.com/krabka-io/krabka-o11y/actions/runs/37213464194/job/111469137964) | `ed6c3a479405169eba1008055f1bbed90f1a0a58` | 901 |
| traces | [111474254858](https://github.com/krabka-io/krabka-o11y/actions/runs/37213464194/job/111474254858) | `ed6c3a479405169eba1008055f1bbed90f1a0a58` | 881 |
| profiles | [111479462459](https://github.com/krabka-io/krabka-o11y/actions/runs/37213464194/job/111479462459) | `ed6c3a479405169eba1008055f1bbed90f1a0a58` | 917 |

The original multi-signal workflow has an overall failure because the invalid metrics job was interrupted. Its other three measurement jobs succeeded; the corrected metrics job succeeded in a separate run. Job conclusions indicate completed evidence collection, not that every ramp level passed. Archive SHA-256 digests were checked against GitHub artifact metadata and every file against the archived checksum list.

Full run IDs, archive digests, image manifests, source commits and expiry dates are in [the provenance record](../qualification/grafana-comparison-provenance.json). Medians, ranges, per-repetition values, all failed points and resource coverage are in [the summary](../qualification/grafana-comparison-summary.json). The qualified Krabka image producer is [run 37183330836](https://github.com/krabka-io/krabka-o11y/actions/runs/37183330836); its source is `47ddda37177501c43c823f17db59ee149340b436` and OCI manifest is `sha256:d9a83da975c23d56f2bcdb4f555978badcdf8687c2d3a4bf4079bbc2d34cddd3`.

The comparison workflow builds the selected branch's optimized image by default.
It preserves the image, source commit, manifest digest, and raw measurements.
Use `image_artifact_run` to compare an image preserved by the operating-envelope workflow.
For an image produced by this comparison workflow, also set
`image_artifact_name` to its exact `comparison-image-SIGNAL-SHA` artifact name.
All matrix jobs then load that image after verifying its file checksums and
Docker configuration digest, skipping the build. The report records its source
commit independently of the harness commit.
Use run `37183330836` to reproduce the historical measurements above.

The Loki comparator uses Grafana's original registry with the same pinned
manifest digest; Google's mirror returned a missing configuration descriptor
in a later run. The native version and configuration did not change.

The workflow is registered on the default branch. Dispatch the branch you want
to measure:

```sh
gh workflow run grafana-comparison.yml --repo krabka-io/krabka-o11y \
  --ref codex/grafana-performance \
  -f signal=metrics -f phases=steady -f phase_seconds=60 -f repetitions=3
```

Set `signal=all` and `phases=all` for the complete comparison.
Run metrics, logs, traces, and profiles in that order for performance optimization.
For profiles, `profiles_target=both` measures the separate role containers and
the shipped `all` target sequentially on one private Google Cloud runner, using
the same image and aggregate resource budget. Each layout gets three fresh
pairs. Their reports are in the artifact's `split/` and `all/` directories.
The workflow waits for low background CPU before warm-up. The harness records
host CPU and rejects runs averaging more than two external cores or exceeding
four external cores over ten sample intervals; the summarizer suppresses
qualified ratios for failed steady objectives.
Use `deployment_target=all` for the shipped single-process logs, traces and
profiles services, or `deployment_target=both` for paired measurements of both
layouts. Metrics always uses separate roles. Each layout retains the same
aggregate resource ceilings; single-process layouts pool the application
roles' allocations, with the broker remaining separate. Local invocation uses
`--deployment-target all` or `--deployment-target split`.

To reuse a newly built comparison image for all four signals:

```sh
gh workflow run grafana-comparison.yml --repo krabka-io/krabka-o11y \
  --ref codex/grafana-performance \
  -f image_artifact_run="$IMAGE_BUILD_RUN" \
  -f image_artifact_name="$IMAGE_ARTIFACT_NAME" \
  -f signal=all -f phases=steady -f phase_seconds=60 -f repetitions=3 \
  -f deployment_target=all
```

Select the producer run's exact artifact name. Its source commit and digest
remain visible in every report, even if the harness branch has since changed.
Use a quiet qualification host for published ratios. A local diagnostic run
uses the same payloads and reports its actual host:

```sh
python3 tools/compare-grafana.py --signal metrics \
  --image "$MEASURED_IMAGE" --image-digest "$MEASURED_MANIFEST_DIGEST" \
  --image-commit "$MEASURED_SOURCE_COMMIT" \
  --seconds 60 --repetitions 3 --phases steady \
  --output qualification/evidence/metrics-candidate
```

The default measures both backends in alternating order. `--backends krabka`
measures only Krabka for a diagnostic iteration. Keep the same measurement
length, dataset, host, and harness when comparing a baseline and candidate.
A faster query completes more requests in this closed-loop workload.
Check completed query counts when interpreting CPU ratios.

Each run records the harness file hashes and writes `SHA256SUMS` for its raw
evidence. Summarize fresh reports directly after three paired repetitions:

```sh
python3 tools/summarize-grafana.py \
  --reports qualification/evidence/metrics-candidate/comparison-report.json \
  --output qualification/evidence/metrics-candidate-summary.json
```

Pass additional signal reports to summarize them together. The summarizer
checks file hashes, telemetry coverage, and matching image identities.

The harness self-test checks the payload encoder against independent bytes
and checks CPU, S3, and RSS accounting. CI runs it on each pull request:

```sh
python3 tools/compare-grafana.py --self-test
python3 tools/deployment-envelope.py --self-test
```

For CPU stacks, live heap, and allocation traces on the private GCP runners,
see [profiling the same release image and workload](grafana-performance-profiling.md).
Those diagnostic runs are separate from qualified comparisons.

Download the historical artifacts into one evidence directory as `metrics.zip`,
`logs.zip`, `traces.zip`, and `profiles.zip`. Copy the provenance record there as
`provenance.json`. The offline summarizer verifies each archive digest before
reading it:

```sh
python3 tools/summarize-grafana.py --evidence qualification/evidence/grafana-comparison \
  --output qualification/evidence/grafana-comparison/summary.json
```
