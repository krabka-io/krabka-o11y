# Object-store contract

Krabka accepts `s3://`, `gs://`, and Azure `az://`/`abfs://` object-store URLs.
The configuration is parsed before a role starts accepting data; an unknown
scheme, missing bucket/container, malformed explicit credential, or unsupported
option stops startup.

| Provider | URL | Credential environment | Integrity |
| --- | --- | --- | --- |
| AWS S3 | `s3://bucket/prefix` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Set `AWS_CHECKSUM_ALGORITHM=sha256` |
| Google Cloud Storage | `gs://bucket/prefix` | `GOOGLE_SERVICE_ACCOUNT_KEY` or workload identity | Provider CRC/checksum validation |
| Azure Blob Storage | `az://container/prefix` | `AZURE_STORAGE_ACCOUNT_NAME` plus account key, SAS, or workload identity | Provider Content-MD5 validation |

All providers must supply atomic create-if-absent and versioned update,
multipart upload, ordered paginated listing, ranged reads, server-side copy,
and idempotent deletion. Krabka retries indeterminate transport and 5xx
failures under a bounded budget. Authentication, authorization, not-found,
checksum/corruption, and failed-precondition errors are permanent. Configure a
provider lifecycle rule to abort incomplete multipart uploads after a day.

Every role exports bounded-cardinality object-store request, failure, retry,
latency, and transferred-byte metrics. Multipart setup, parts, completion, and
abort each count as requests; part payloads count as transferred bytes.

## Startup probe

Each role probes its store before it accepts data. The metrics and profiles
roles probe in `build_object_store`, traces in `SharedObjectStore::get`, and
logs in `build_configured_object_store`. A failed probe stops the role before
its readiness gate opens.

The probe writes one object below `<prefix>/.krabka-probe/` and deletes it
after the probe, pass or fail. The write credential therefore needs put, get,
list and delete on that sub-prefix. The probe checks these semantics in order:

| Check | Error when it fails | What to change |
| --- | --- | --- |
| Create-if-absent is implemented | `CreateIfAbsentUnsupported` | Enable conditional puts. For S3, remove `AWS_CONDITIONAL_PUT=disabled`. |
| A second create to one key fails | `CreateIfAbsentIgnored` | Use a store that honours `If-None-Match`. |
| A bounded ranged read returns its bytes | `RangedReadMismatch` | Use a store that honours `Range`. |
| A listing names the object just written | `ListingMissedWrite` | Use a store with list-after-write consistency. |
| Conditional update is implemented | `ConditionalUpdateUnsupported` | Enable conditional puts, as for create-if-absent. |
| An update on a stale version fails | `ConditionalUpdateIgnored` | Use a store that honours `If-Match`. |
| A second delete succeeds or is not-found | `DeleteNotIdempotent` | Use a store with idempotent deletes. |

A request that fails for another reason gives `ObjectStore` with the probe
step. A bad credential, a missing bucket and an unreachable endpoint give this
error. A conditional update is optional for `file://`, `memory://` and bare
paths, because one process owns those stores. Every other scheme needs it.

The probe also tries a suffix range read and logs the result as
`suffix_range_read`. A store without suffix ranges passes. Block reads then
fall back to a `HEAD` for the size and a bounded range for the footer.

## Provider consistency

| Provider | Consistency Krabka relies on | Conditional writes | Notes |
| --- | --- | --- | --- |
| AWS S3 | Strong read-after-write and list-after-write on every object | `If-None-Match: *` create, `If-Match` ETag update | Conditional puts are on by default. S3-compatible stores must implement both headers. |
| Google Cloud Storage | Strong read-after-write and list-after-write | `ifGenerationMatch=0` create, generation match update | The probe sends the ETag and the generation, so a generation match passes. |
| Azure Blob Storage | Strong read-after-write and list-after-write | `If-None-Match: *` create, `If-Match` ETag update | The client refuses suffix range reads before it sends a request. Block reads use the fallback above. |

The orphan sweep deletes only an object that it listed and that no index
names. An object that a stale listing omits is kept until a later listing
shows it. The stale-listing case in the provider suite checks this.

## Endpoints

The provider URL names the bucket or container, not the host. Set the host
with an environment variable:

| Target | Variables |
| --- | --- |
| MinIO or another S3-compatible store over HTTP | `AWS_ENDPOINT=http://host:port`, `AWS_ALLOW_HTTP=true`, `AWS_REGION` (any value the store accepts) |
| Cloudflare R2 | `AWS_ENDPOINT=https://<account>.r2.cloudflarestorage.com`, `AWS_REGION=auto` |
| Azurite | `AZURE_STORAGE_USE_EMULATOR=true`. Set `AZURITE_BLOB_STORAGE_URL` when Azurite is not at `http://127.0.0.1:10000`. |
| Another Azure endpoint | `AZURE_STORAGE_ENDPOINT=https://host` |
| A GCS emulator | `GOOGLE_BASE_URL=http://host:port` |

`AWS_ENDPOINT_URL` and `AWS_ENDPOINT_URL_S3` are also accepted. The latter
takes precedence. A lifecycle report records the endpoint host as
`endpoint_host`, or `null` for the provider default.

## Retry and throttling

Two retry layers apply, and each has its own bound:

1. The `object_store` HTTP client retries a 5xx, a 429, a connection error,
   and a 200 response whose body has `InternalError` or `SlowDown`. It makes up
   to 10 retries inside 3 minutes, with exponential backoff.
2. `RetryingObjectStore` retries a failure that survives the client budget.
   Only `Generic` errors are retried, with 4 attempts and 250 ms to 4 s of
   backoff. The block builders and the metrics compactor use this layer.

A 401, 403, 404, 409 or 412 maps to its own error and is never retried. A
provider checksum rejection, such as S3 `BadDigest`, is a 400. The upload is
resent from the intact payload and then reported. A corrupted read fails the
block decode and is not retried as an outage.

Throttling that outlasts both budgets fails the write. The WAL offsets stay
uncommitted, so the block builder writes the same window again after a
restart. Raise the provider request quota, or spread tenants across prefixes,
when throttling failures show in the `objstore_operation_failures_total`
metric of a role, such as `krabka_metrics_objstore_operation_failures_total`.

## Authoritative provider qualification

The `object-store contract` workflow runs against the selected provider's
GitHub Environment: `object-store-aws`, `object-store-gcs`, or
`object-store-azure`. Each environment supplies
`KRABKA_OBJECT_STORE_CONTRACT_URL` and the credentials above. The URL must use
a dedicated `krabka-contract/<run>` prefix. The suite refuses any other prefix
and deletes every object below the accepted one.

The provider suite covers seven cases:

| Case | What it checks |
| --- | --- |
| `conditional_writes` | Create and update conflicts give `AlreadyExists` and `Precondition`. |
| `multipart` | A multipart object is invisible until completion, and readable after it. |
| `pagination` | A listing of more than one provider page names every object once. |
| `stale_listing` | The orphan sweep keeps an object that a stale listing omits. |
| `checksum_mismatch` | A rejected upload is resent, then reported. A corrupted read is permanent. |
| `throttling` | Throttling inside the retry budget writes once. Beyond it, nothing is written. |
| `deletion` | A repeated delete succeeds, and a deleted object leaves the listing. |

It writes `object-store-contract.json` with the cases it ran and their request
and byte costs.

The workflow then runs one lifecycle test per signal against the same
provider. Each test writes below
`<prefix>/lifecycle/<signal>/<test>/<nanos>-<pid>`, deletes that sub-prefix at
the end, and writes `object-store-lifecycle-<signal>-<test>.json`:

| Signal | Test | Steps |
| --- | --- | --- |
| Metrics | `//crates/metrics:level_compaction_test` | Flush, query, compaction, retention, orphan reconciliation, restart |
| Logs | `//crates/observability:compactor_test` | Flush, query, retention, restart |
| Traces | `//crates/traces:block_lifecycle_test` | Flush, query, compaction, retention, orphan reconciliation, restart |
| Profiles | `//crates/profiles:lifecycle_test` | Flush, query, compaction, retention, orphan reconciliation, restart |

The test in each target is
`a_block_survives_its_whole_lifecycle_on_the_configured_store`. Without
`KRABKA_OBJECT_STORE_CONTRACT_URL` it runs against an in-memory store. Logs
has no block merge and no orphan sweep, so its compaction step is the
WAL-to-block flush.

These suites cover other lifecycle paths in memory only:

| Signal | Other lifecycle evidence |
| --- | --- |
| Metrics | `//crates/metrics:ingest_roundtrip_test` |
| Logs | `//crates/observability:ingest_roundtrip_test`, `//crates/observability:wal_live_broker_test` |
| Traces | `//crates/traces:blockbuilder_test`, `//crates/traces:compactor_test` |
| Profiles | `//crates/profiles:block_builder_drain_test`, `//crates/profiles:block_builder_object_store_retry_test` |

The workflow copies the reports and test logs into one directory and writes
`SHA256SUMS` over them. `tools/object-store-evidence.py` then fails the run
when a file does not match its checksum, a case or signal is missing, a
lifecycle step did not run, or a request or byte count is zero. The directory
is kept as one artifact named with provider and commit.

## Run against a provider

Use a disposable bucket and a `krabka-contract/` prefix. The provider suite:

```bash
KRABKA_OBJECT_STORE_CONTRACT_URL=s3://bucket/krabka-contract/manual \
  cargo test -p krabka-blockstore --test object_store_provider_contract \
  -- --ignored --nocapture
```

One lifecycle test. Set `KRABKA_OBJECT_STORE_LIFECYCLE_REPORT_DIR` to keep the
report. Without it, the report is printed only:

```bash
export KRABKA_OBJECT_STORE_CONTRACT_URL=s3://bucket/krabka-contract/manual
export KRABKA_OBJECT_STORE_LIFECYCLE_REPORT_DIR=/tmp/object-store-evidence
cargo test -p krabka-metrics --test level_compaction \
  a_block_survives_its_whole_lifecycle_on_the_configured_store -- --exact
```

Use `-p krabka-observability --test compactor`, `-p krabka-traces --test
block_lifecycle` and `-p krabka-profiles --test lifecycle` for the other
signals. Under Bazel, pass each variable with `--test_env`, and add
`--nozip_undeclared_test_outputs` to keep the reports as plain files under
`bazel-testlogs/`. See `.github/workflows/object-store-contract.yml`.

Against MinIO, add the endpoint variables:

```bash
export AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
```

To check a collected directory, write `SHA256SUMS` and run the checker:

```bash
(cd /tmp/object-store-evidence && sha256sum ./* >SHA256SUMS)
tools/object-store-evidence.py /tmp/object-store-evidence --provider aws
```
