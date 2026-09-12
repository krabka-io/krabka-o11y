# krabka-metrics

Prometheus/Grafana-Mimir-equivalent metrics backend for Krabka.

This crate starts with the metrics data layer: Arrow block schemas, native
histogram encoding, float samples, exemplars, and the remote_write v2 symbol
table. It also owns the distributor ingest path, WAL append wiring, and
the block and index writes of both the block builder and the compactor. Query execution lives in `krabka-promql`.

## The two binaries

The metrics path is one write binary and one read binary, and each owns its own
roles. They are not interchangeable, and neither has a role the other also has.

| Binary | `--target` | What it does |
| --- | --- | --- |
| `krabka-metrics` | `distributor`, `block-builder`, `compactor` | Remote-write and OTLP ingest into the WAL, the building of blocks out of that WAL, and the merging of those blocks into larger ones |
| `krabka-metrics-service` | `querier`, `query-frontend`, `ruler` | The PromQL query API over those blocks, its splitting front end, and rule evaluation |

The `block-builder` reads the WAL and writes one block per tenant, kind and
offset window. The `compactor` reaches no broker: it reads the `.index`
manifests, merges the float and native-histogram blocks a level policy chooses,
and retires the inputs. The `block-builder` also runs the retention and orphan
sweep.

The split follows the crate graph: `krabka-metrics-service` depends on
`krabka-metrics` and on `krabka-promql`, so the read path can reach the write
path's types and the reverse would be a cycle.

`krabka-metrics` once accepted the three read-path names as well, over a router
that served `/api/v1/status/buildinfo` and nothing else -- it bound, logged,
and reported itself ready while answering every query with a 404. Those roles
are gone. `krabka-metrics --target=querier` now refuses to start, and says
which binary to run instead.
