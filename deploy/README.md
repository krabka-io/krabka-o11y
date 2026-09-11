# Deploying krabka-o11y

One role of each signal, a broker, and an object store. The same role
configuration drives both orchestrators.

| Path | What it is |
| --- | --- |
| [`roles/`](roles) | One `--config.file` per role. Both orchestrators read these files |
| [`compose/`](compose) | A Docker Compose stack |
| [`kustomization.yaml`](kustomization.yaml), [`kubernetes/`](kubernetes) | A kustomize base |

The image is built by Bazel from `//bazel/images/krabka`. It holds all six
binaries and sets no entrypoint, so each manifest names the binary it runs.

## Compose

```bash
docker compose -f deploy/compose/docker-compose.yaml up -d
```

To run an image built from this tree:

```bash
bazel run //bazel/images/krabka:load
KRABKA_O11Y_IMAGE=krabka-o11y:dev \
  docker compose -f deploy/compose/docker-compose.yaml up -d
```

The stack publishes Prometheus remote-write on `:4041`, the PromQL query API on
`:9090`, Loki push and query on `:3100` and `:3101`, OTLP traces on `:4317` and
`:4318`, the Tempo query API on `:3201`, Pyroscope ingest on `:4040`, and the
Pyroscope query API on `:4042`. Every request needs an `X-Scope-OrgID` header.

## Kubernetes

```bash
kubectl apply -k deploy
```

The kustomization root is `deploy/`, not `deploy/kubernetes/`, because kustomize
refuses to read a file above its own root and the role configuration is shared
with the compose stack.

Replace the `krabka-object-store` secret before you use this anywhere real. The
literals in `kustomization.yaml` exist so that one `kubectl apply` brings the
stack up on a scratch cluster, and that is the only claim the base makes.

The base renders and has not been applied to a cluster. The compose stack has
been run end to end: all four signals were ingested and read back, and the
drain, the probes, and the stop behaviour recorded below were measured on it.
The two share the image and the role configuration, so what is untested here is
the probe, volume, and lifecycle wiring that only Kubernetes has.

## Roles

Every signal names its stages the same way, and the names are the ones Loki,
Mimir, Tempo and Pyroscope use. A stage that exists in more than one signal is
spelled identically in all of them, so a chart author learns one topology
rather than four.

| Role | What it does | Signals that have it |
| --- | --- | --- |
| `distributor` | Accepts pushes, applies limits, writes the WAL | all four |
| `block-builder` | Consumes the WAL, writes blocks to object storage | all four |
| `live-store` | Serves the window no block covers yet | traces |
| `querier` | Answers a query over blocks and the live tier | all four |
| `query-frontend` | Shards a query, fans out, merges | traces, profiles, metrics |
| `compactor` | Merges blocks already in object storage, enforces retention | traces, profiles |
| `ruler` | Evaluates recording and alerting rules | metrics |
| `metrics-generator` | Derives metrics from spans and remote-writes them | traces |
| `symbolizer` | Resolves profile addresses to function names | profiles |
| `all` | Every role of that signal, in one process | logs, traces, profiles |

The table's `metrics` column means the two metrics binaries together.

The gaps are real, not oversights. Only traces keeps the recent window as a
role of its own; a logs or metrics querier tails the WAL itself. Only metrics
evaluates rules and only profiles resolves symbols. Logs and metrics have no
`compactor`: logs folds retention and delete materialisation into its block
builder, as Loki does, and nothing yet merges metric blocks that are already
in object storage, so the metrics retention sweep rides in its block builder
too.

`block-builder` is the name that settled the one collision worth naming. Metrics
and logs used to call this stage `compactor`, while traces and profiles used
that word for the object-storage block merger -- one word, two jobs, depending
on which binary was being read. Mimir and Tempo both spell the WAL-consuming
stage `block-builder` on the Kafka ingest path this stack is built on, and all
four upstreams reserve `compactor` for the merger.

### One process

`--target all` runs every role of a signal in one process, the way Loki's,
Mimir's, Tempo's and Pyroscope's single binaries do. It is the shape to
evaluate the stack in.

There are no new ports to configure. Logs and profiles serve push and query on
one listener, as `Loki` and `Pyroscope` do. Traces gives `--listen` to the
query-frontend, keeps the ingest protocol ports, and has the querier and the
live-store bind loopback ports the kernel chooses and the process wires up for
itself.

Readiness aggregates. `/ready` is 503 until every role of the process is up,
and names the one holding it back: `not ready: block-builder/wal-consumer`.

The stop is staged rather than simultaneous. The distributor goes first, so
nothing new enters the WAL, and only then does the block builder flush what is
behind it; the read path and the compactor go last, because neither holds
anything a stop could lose. Stopping them together would leave the last
acknowledged push in a WAL that, in a one-process stack, nothing restarts to
read. `--all-drain-stage-timeout` bounds each stage, and should sit under the
pod's `terminationGracePeriodSeconds`.

[`roles/logs-all.yaml`](roles/logs-all.yaml),
[`roles/traces-all.yaml`](roles/traces-all.yaml) and
[`roles/profiles-all.yaml`](roles/profiles-all.yaml) are worked examples, and
`//deploy:role_config_test` checks their keys against the binaries. The compose
stack and the kustomize base run the roles separately on purpose: that is the
topology a chart has to describe.

Metrics has no `all`, and the reason is structural rather than an omission: it
is the one signal whose roles are split across two binaries, `krabka-metrics`
for the write path and `krabka-metrics-service` for the read path, so no single
process holds them.

## Configuration

Each role reads one YAML file from `roles/`. The keys are the binary's own long
flag names, so `--help` documents the file. A flag and an environment variable
both win over the file. See `krabka_observability::config_file` for the
precedence rule.

Object-store credentials are the exception. `object_store` reads them from the
process environment and no flag names them, so they stay `AWS_*` environment
variables: a Secret in Kubernetes, and the compose file's `environment` block.

## What the manifests depend on

Each of these is a property of the binaries. The manifests are wired to them,
and a change to one of them is a change to the manifests.

**Every binary exits on SIGTERM.** `krabka_observability::shutdown_signal`
waits on SIGTERM and SIGINT together, every role calls it, and every role
returns from `main` once it has. The image sets no entrypoint, so the role
binary is PID 1 and the signal reaches the process that installed the handler.
This matters more than it looks, and in two ways. The kernel gives PID 1 no
default terminate action, so a process that installed no handler would discard
SIGTERM and run until the grace period ended. And a handler alone is not
enough: a role whose shutdown waits on a background task that never hears the
cancel is killed at the grace period just as surely, so every long-lived task a
role supervises races its own connect and its own poll against the role's
token. The suites that hold this are the `sigterm_*` tests, which signal a real
child process and assert its exit status is `Some(0)` -- a signalled process
reports `None`.

**`/ready` names the gates a role has not met.** It answers 200 with `ready`,
or 503 with `not ready:` and the names of the unmet gates. The compose
healthchecks and the Kubernetes readiness probes both use it. Every role of
every signal serves it on its admin port, and the roles with an HTTP data port
echo the same gates there -- a `krabka-traces` query port on both `/ready` and
Tempo's `/status` alias, which is what the traces query-frontend probes to
decide its fan-out.

**The drain is a readiness change, not an exit.** `POST
/ingester/prepare_shutdown` on the logs distributor clears the
`accepting-writes` gate. The data port then answers 503 and the orchestrator
stops sending pushes. The Kubernetes manifest calls it from a `preStop` hook.

**Probe the data port where a role has one.** The admin server on `:9404` is a
detached task with no graceful shutdown, so it answers 200 for the whole of a
drain. The `accepting-writes` gate is only on the data port. Roles with no data
port are probed on `:9404`, which is where their gates are.

**Liveness is never `/ready`.** No binary serves a liveness route, so liveness
is a TCP check on the admin port. A liveness probe on `/ready` restarts a role
for being slow to load an index, which throws away the work it had loaded.

**Grace periods are sized against what a drain does.** A distributor drains
in-flight pushes that are already written to the WAL. A block builder finishes
the batch in flight, because a batch killed halfway is replayed. The traces
block builder waits up to one `block-builder-window` before it even notices the
signal. The metrics-service ruler drains its alertmanager queue under a
30-second bound.

**Durable state gets a volume.** The broker's log directory holds the WAL and
the consumer-group offsets that record how far each block builder has read. The
logs block builder and the logs querier keep a local manifest and a
delete-request store under `--data-root`. None of that is reconstructible, so
none of it is on an `emptyDir`. Every other role gets an `emptyDir` for its working directory,
because its state is in the broker and in the object store.

**The topic contract is provisioned, not assumed.** `krabka-o11y-bootstrap`
creates the six topics, then describes each one and fails if one does not meet
the contract. It runs as an init container on every role pod, and as an ordered
dependency in compose.

**Every role checks that contract again for itself.** Each binary calls
`krabka_observability::topic_contract::require_topics` for the topics its role
touches, after it parses its configuration and before it builds a producer or a
consumer, and refuses to start when one is absent or a state topic is not
compacted. It creates nothing: the partition count is stated once, by the
provisioning step above, and read back here. A role that reaches no broker --
the metrics querier, the traces compactor, a logs role with no
`wal_bootstrap_server` -- skips the check rather than making a broker it does
not use a condition of its starting.

## Gaps

These are properties of the binaries that the manifests cannot work around.
Each one is a place where a probe says less than it appears to.

**A traces distributor is probed on its admin port.** Its seven data ports are
OTLP, Jaeger and Zipkin ingest routers, and none of them carries an HTTP
surface of its own, so `/ready` is on `:9404` alone. The admin server is a
detached task with no graceful shutdown, so this probe cannot report a drain --
which costs nothing today, because no traces role has a drain gate to report.
The traces querier is probed on its data port, where the query-frontend's own
membership probe asks it.

**No probe reports how far behind a WAL consumer is.** A gate answers whether a
consumer is attached, not whether it has caught up. A block builder that is
attached and an hour behind reads as ready, and the lag is visible only in the
metrics. This is true of `--target all` too, where one `/ready` now covers
every role of the process.

**Only the logs all-in-one empties the WAL on the way out.** Its stop ends with
a pass that compacts whatever the block builder had not yet flushed, so a push
the process accepted a moment earlier is in a block before it exits. The traces
and profiles block builders flush what they have buffered and stop; a record
produced after their last poll stays in the WAL, uncommitted, for the next start
to replay. Nothing is durably lost either way -- offsets move only behind a
flush -- but in those two signals the last records are not queryable until the
process is started again.
