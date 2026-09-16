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

## Authoritative provider qualification

The `object-store contract` workflow runs against the selected provider's
GitHub Environment: `object-store-aws`, `object-store-gcs`, or
`object-store-azure`. Each environment supplies
`KRABKA_OBJECT_STORE_CONTRACT_URL` and the credentials above. The URL must use
a dedicated `krabka-contract/<run>` prefix. The suite refuses any other prefix
and deletes every object below the accepted one.

The suite covers conditional create/update conflicts, multipart visibility,
ranged reads, copy, deletion, immediate listing convergence, listing beyond
one S3 page, and post-delete convergence. It records request and byte costs in
`object-store-contract.json`; the test log and report are retained as one
artifact named with provider and commit.

The provider suite qualifies the shared `ObjectStore` boundary. The signal
lifecycle suites qualify flush, query, compaction, retention, orphan cleanup,
and restart above that boundary:

| Signal | Lifecycle evidence |
| --- | --- |
| Metrics | `//crates/metrics:ingest_roundtrip_test`, `//crates/metrics:level_compaction_test` |
| Logs | `//crates/observability:ingest_roundtrip_test`, `//crates/observability:compactor_test`, `//crates/observability:wal_live_broker_docker_test` |
| Traces | `//crates/traces:blockbuilder_test`, `//crates/traces:compactor_test` |
| Profiles | `//crates/profiles:block_builder_test`, `//crates/profiles:lifecycle_test` |

Run a provider locally only with a disposable prefix:

```bash
KRABKA_OBJECT_STORE_CONTRACT_URL=s3://bucket/krabka-contract/manual \
  cargo test -p krabka-blockstore --test object_store_provider_contract \
  -- --ignored --nocapture
```
