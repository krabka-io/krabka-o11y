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

**Every binary handles SIGTERM.** `krabka_observability::shutdown_signal` waits
on SIGTERM and SIGINT together, and every role calls it. The image sets no
entrypoint, so the role binary is PID 1 and the signal reaches the process that
installed the handler. This matters more than it looks: the kernel gives PID 1
no default terminate action, so a process that did not install a handler would
discard SIGTERM and run until the grace period ended.

**`/ready` names the gates a role has not met.** It answers 200 with `ready`,
or 503 with `not ready:` and the names of the unmet gates. The compose
healthchecks and the Kubernetes readiness probes both use it.

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
in-flight pushes that are already written to the WAL. A compactor finishes the
batch in flight, because a batch killed halfway is replayed. The traces block
builder waits up to one `block-builder-window` before it even notices the
signal. The metrics-service ruler drains its alertmanager queue under a
30-second bound.

**Durable state gets a volume.** The broker's log directory holds the WAL and
the consumer-group offsets that record how far each block builder has read. The
logs compactor and the logs querier keep a local manifest and a delete-request
store under `--data-root`. None of that is reconstructible, so none of it is on
an `emptyDir`. Every other role gets an `emptyDir` for its working directory,
because its state is in the broker and in the object store.

**The topic contract is provisioned, not assumed.** `krabka-o11y-bootstrap`
creates the six topics, then describes each one and fails if one does not meet
the contract. It runs as an init container on every role pod, and as an ordered
dependency in compose.

## Gaps

These are properties of the binaries that the manifests cannot work around.
Each one is a place where a probe says less than it appears to.

**`krabka-traces` reports no readiness.** It registers no readiness gates, and
its `main` merges only the metrics router into the admin port, so `:9404/ready`
does not exist for any traces role. The `/ready` its data-port roles serve is a
constant string with nothing behind it. The manifests use TCP checks
for every traces role, and report only that a port is open.

**No binary checks the topic contract at startup.**
`krabka_observability::topic_contract` documents that "every role calls
`require_topics` at start-up ... and refuses to start when one is absent or not
compacted". No service binary calls it. A role started against a broker whose
WAL topic is compacted starts normally. The init container is what closes this,
and it closes it only for pods that this base creates.

**`krabka-profiles --target=querier` does not exit on SIGTERM.** It is
SIGKILLed at the end of its grace period on every stop, with no work in flight.
Two measured runs of `docker stop` on an idle, healthy container took 64.8 s and
73.2 s and both ended in exit 137. The other eleven roles in the same stack, the
profiles distributor and block builder among them, stop in 0.1 s to 1.3 s with
exit 0. Raising the grace period does not help, because the process never
exits. The value in the compose file stays at 60 s for that reason.

**`krabka-metrics-service --target=querier` can ignore SIGTERM.** This is a
different case: it stops normally when its dependencies answer, and hangs when
they do not. Started against an unreachable broker and object store it does not
exit on SIGTERM, or on a second one. Reproduce it with:

```bash
timeout 6 ./bazel-bin/crates/metrics-service/krabka-metrics-service \
  --config.file=deploy/roles/metrics-querier.yaml --admin-listen-addr=127.0.0.1:0
```
