//! Cross-signal integration suites: the one place in this workspace allowed to
//! depend on more than one signal.
//!
//! Every other crate here is scoped to a single signal, and the crate graph
//! keeps it that way. `krabka-promql` reaches `krabka-metrics`, `krabka-traces`
//! reaches `krabka-traceql`, `krabka-observability` reaches `krabka-logql` —
//! but nothing joins metrics to traces, logs or profiles. That is a deliberate
//! shape for the libraries and a hole in the tests: a signal that hands a value
//! to another signal has an encoder on one side and a decoder on the other, and
//! until this crate existed no test in the workspace ever put the two together.
//!
//! The traces `metrics-generator` is the case that made it concrete. Its
//! span-metrics processor writes exemplars carrying `trace_id` and `span_id`,
//! and its end-to-end suite remote-writes them into an upstream Prometheus
//! container rather than into Krabka's own metrics ingest — so Krabka's
//! exemplar encoder had never been read by Krabka's exemplar decoder. The
//! metrics Grafana suite reaches `/api/v1/query_exemplars` from the other
//! direction, and its own seed carries no exemplars, so it can only assert that
//! the status is `success`. Two suites, one gap between them.
//!
//! # What belongs here
//!
//! A suite belongs in this crate when it asserts something about the *link*
//! between two signals — a value one signal produces and another consumes, end
//! to end through both crates' real code. A suite that exercises one signal
//! belongs in that signal's own crate, where it stays close to the code it
//! covers and inside that crate's mutation sweep.
//!
//! # How the suites are wired
//!
//! In process, not in containers. `bazel test //...` filters out `docker`-tagged
//! targets, so a container suite would sit outside the lane that every commit
//! runs — which is the lane these links need to be in. Where a link is an HTTP
//! boundary in production, the suite binds a real loopback listener and speaks
//! real HTTP over it, so the production client and the production router both
//! run; where it is a library call, the suite makes the library call.
//!
//! The library itself is empty on purpose. This module is the crate's
//! documentation; the tests under `tests/` are its content.
