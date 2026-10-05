# Tempo deployment scenarios

Run the suite against the locally built application and pinned broker and `MinIO` images:

```bash
bazel test --config=docker //crates/traces:tempo_deployment_docker_test
```

The scenarios adapt Grafana Tempo's [integration tests](https://github.com/grafana/tempo/tree/85248acb9df2ddf12221cc1a5c7db076cec033e7/integration), surveyed at commit `85248acb9df2ddf12221cc1a5c7db076cec033e7`.
Each case owns its Docker network, broker, object store, and local state.

| Upstream scenario | Krabka test |
| --- | --- |
| `operations/receivers_test.go::TestReceivers` | OTLP HTTP, OTLP gRPC, Zipkin JSON, and Jaeger gRPC retain trace IDs, span IDs, timestamps, attributes, and service names across storage and restart |
| OTLP HTTP receiver extension | Gzip protobuf retains the complete two-span trace across storage and restart |
| `api/tracebyid/trace_by_id_test.go::TestTraceByIDandTraceQL` | A trace assembles across separate persisted batches; repeated spans collapse and distinct child spans remain |
| `api/search/search_test.go::TestSearchTraceQL` | Selectors, ancestor relationships, and spanset count filters find the expected trace before and after storage; an unmatched selector returns no trace |
| `api/search/search_tags_test.go::TestTagEndpoints` | Span and resource scopes, typed V2 values, and V1 service values remain available after restart |
| `api/search/search_tags_test.go::TestTagValuesWithSpecialCharacters` | An encoded raw attribute name with a slash, dot, and hyphen retains its tag name and value |
| `api/search/multi_tenant_test.go::TestSingleTenantSearch` | Two tenants can store different content under the same trace ID; reads and tag values stay isolated, and a third tenant sees no trace |
| `limits/limits_test.go::TestOTLPLimits` | Adapted to `max_spans_per_trace`: HTTP and gRPC reject an oversized trace before WAL append, a valid trace succeeds, and an unrestricted tenant retains the oversized trace |
| `operations/graceful_shutdown_test.go::TestShutdownSingleBinary` and `operations/single_binary_flush_test.go::TestSingleBinaryIngestsAndFlushesToBackend` | The all-in-one role serves an accepted trace through its query frontend, then drains it to storage during shutdown |

Expected complete span records come from fixed inputs, including resource and instrumentation scope, parent IDs, status, and attributes. Array order is normalized for comparison. Tests never use Krabka output as the expected result.

Ordinary persistence checks first assert that a querier without a live store cannot find the trace. The block builder then consumes the real WAL and publishes blocks to `MinIO`. After both builder and cold querier stop, a fresh cold querier must return the same expected records. The split-batch case proves the first batch is persisted before the second arrives. The shutdown case uses a long flush age and asserts the cold trace is absent before shutdown, so an earlier periodic flush cannot satisfy the drain check.

The tag cases exposed a cold-query bug: the index merges tag names from all attribute scopes, but span-only queries treated that list as span tags. The fix reads the existing scoped block attributes for span-only requests, as it already does for resource, event, link, and instrumentation requests. `querier::store::tests::cold_span_tag_discovery_keeps_other_scopes_out` checks resource, event, link, and intrinsic exclusions with a real block. This regression fails before the fix.

These cases complement `tempo_differential.rs`, which runs a pinned Tempo as a live API oracle, and the in-process trace lifecycle, frontend, metrics-generator, and WAL suites. The deployment suite uses fixed expected records and does not claim a live differential comparison.

Tempo-specific storage encodings, storage schedulers, memberlist rings, zone downscale, Jaeger query plugins, and cloud backends other than S3 are outside this adaptation. Tempo quotes special-character tag identifiers. This adaptation uses Krabka's existing raw scoped tag lookup; quoted tag identifiers remain an API compatibility gap.

OTLP JSON ingest, gzip query responses, trace-by-ID filtering and span pruning, streaming gRPC search, and partial-success ingest are not covered by this suite. The ingest-limit adaptation checks Krabka's span-count cap rather than Tempo's byte cap or partial-success policy.
