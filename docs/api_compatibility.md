# API Compatibility

This matrix states the supported, implemented, stubbed, and excluded surfaces of the four signals.

“Supported” means that a differential suite compares the behavior with the named upstream product.

The generated [route inventory](api/routes.json) is the exhaustive method and path list.

## Metrics

| Surface | Status | Evidence |
| --- | --- | --- |
| Prometheus remote write v1/v2 and Mimir push aliases | Supported | [`diff_prometheus::prometheus_compliance_corpus_matches_krabka`](../crates/metrics-service/tests/diff_prometheus.rs), [`diff_mimir::mimir_compliance_corpus_matches_krabka`](../crates/metrics-service/tests/diff_mimir.rs) |
| PromQL instant and range query APIs | Supported | [`diff_prometheus::prometheus_compliance_corpus_matches_krabka`](../crates/metrics-service/tests/diff_prometheus.rs), [`diff_mimir::mimir_compliance_corpus_matches_krabka`](../crates/metrics-service/tests/diff_mimir.rs) |
| Grafana Prometheus datasource resources | Supported | [`grafana_integration::grafana_e2e_covers_all_api_surfaces_and_query_shapes`](../crates/metrics-service/tests/grafana_integration.rs) |
| OTLP metrics and Krabka clocks | Implemented | [`metrics::ingest_roundtrip`](../crates/metrics/tests/ingest_roundtrip.rs), [`metrics::clock_ingest`](../crates/metrics/tests/clock_ingest.rs) |
| Rules, alerts, cardinality, metadata, status, and admin routes | Implemented | [`promql::http_api`](../crates/promql/tests/http_api.rs) |
| Targets, scrape pools, and alertmanager discovery | Stubbed as empty | [`promql::http_api`](../crates/promql/tests/http_api.rs) |
| Target scraping and service discovery | Out of scope | Use Prometheus agent mode or Grafana Alloy |

## Logs

| Surface | Status | Evidence |
| --- | --- | --- |
| Loki JSON, snappy-protobuf, and OTLP push | Supported | [`loki_differential::loki_corpus_matches_krabka`](../crates/observability/tests/loki_differential.rs) |
| LogQL query, range, label, series, pattern, field, and index APIs | Supported | [`loki_differential::loki_corpus_matches_krabka`](../crates/observability/tests/loki_differential.rs) |
| Grafana Loki datasource proxy and backend calls | Supported | [`grafana_integration::a_grafana_loki_datasource_reads_the_querier_over_the_proxy_and_the_backend_path`](../crates/observability/tests/grafana_integration.rs), [`grafana_e2e::grafana_reads_the_same_answer_from_krabka_and_from_loki`](../crates/observability/tests/grafana_e2e.rs) |
| Ruler and delete-request routes | Implemented | [`observability::ruler`](../crates/observability/tests/ruler.rs), [`observability::deletes`](../crates/observability/tests/deletes.rs) |
| Readiness, ring, build, config, service, and shutdown routes | Krabka extension | [`observability::status_endpoints`](../crates/observability/tests/status_endpoints.rs) |

## Traces

| Surface | Status | Evidence |
| --- | --- | --- |
| Tempo trace by ID, search, tags, and TraceQL metrics APIs | Supported | [`tempo_differential::real_tempo_and_krabka_match_basic_by_id_and_search`](../crates/traces/tests/tempo_differential.rs), [`tempo_differential::real_tempo_and_krabka_match_traceql_metrics_query_range`](../crates/traces/tests/tempo_differential.rs) |
| Grafana Tempo datasource and service graphs | Supported | [`tempo_differential::grafana_accepts_tempo_datasource_pointing_at_krabka`](../crates/traces/tests/tempo_differential.rs), [`grafana_e2e::grafana_e2e_full_surface`](../crates/traces/tests/grafana_e2e.rs) |
| OTLP, Tempo push, Zipkin, and Jaeger ingest | Implemented | [`grafana_e2e::ingest_all_doors_decode_correctly`](../crates/traces/tests/grafana_e2e.rs) |
| Jaeger query API | Out of scope | Jaeger is ingest-only; use Tempo or TraceQL for reads |
| Distributor head or tail sampling | Out of scope | Apply sampling in an OpenTelemetry Collector before ingest |

## Profiles

| Surface | Status | Evidence |
| --- | --- | --- |
| Legacy pprof ingest and render | Supported | [`pyroscope_differential::real_pyroscope_render_matches_krabka_after_identical_ingest`](../crates/profiles/tests/pyroscope_differential.rs), [`pyroscope_differential::real_pyroscope_legacy_ingest_formats_match_krabka`](../crates/profiles/tests/pyroscope_differential.rs) |
| Pyroscope Connect labels, series, merge, diff, and profile stats | Supported | [`pyroscope_differential::real_pyroscope_series_and_stats_match_krabka_after_identical_ingest`](../crates/profiles/tests/pyroscope_differential.rs) |
| Grafana Pyroscope datasource | Supported | [`pyroscope_differential::grafana_accepts_pyroscope_datasource_pointing_at_krabka`](../crates/profiles/tests/pyroscope_differential.rs) |
| OTLP profiles | Supported | [`pyroscope_differential::real_pyroscope_otlp_export_matches_krabka`](../crates/profiles/tests/pyroscope_differential.rs) |
| Legacy `/pyroscope/labels` and `/pyroscope/label-values` | Out of scope | Grafana uses the Connect label methods |

## Common contract

All data APIs are tenant-scoped with `X-Scope-OrgID` or the equivalent gRPC metadata.

Admin routes are not upstream compatibility claims unless a row above names them.
