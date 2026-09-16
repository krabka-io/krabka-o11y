# Six-month compatibility qualification

Milestone 21 turns the six-month Mimir, Loki, Tempo, Pyroscope, Grafana, Alloy,
Prometheus, and OTLP roadmap into one release gate.

## Supported versions

The server oracles are Mimir 3.2.1, Loki 3.7.7, Tempo 3.0.3, and Pyroscope 2.3.1.
The supported client window is Grafana 13.2.2 and 13.1.6, Alloy 1.19.2 and 1.18.1,
Prometheus 3.14.0 and 3.13.3, and OTLP source schemas 1.11.0 and 1.10.0.
The [compatibility matrix](../api_compatibility.md) records the qualified surfaces,
known divergences, and out-of-scope behavior.

## Release envelope

- The [operating envelope](../operating_envelope.md) defines supported scale and latency bounds.
- The [persisted-format contract](../persisted_formats.md) and [v0.4.0 release procedure](v0.4.0.md) define upgrade and rollback behavior.
- The [disaster-recovery runbook](../disaster_recovery.md) defines consistent backup, audit, and restore cuts.
- The demo qualification is pinned to `krabka-io/krabka-o11y-demo` commit `73789644829c67107e9056b7f9718472ce2c089d` and [successful run 35083910051](https://github.com/krabka-io/krabka-o11y-demo/actions/runs/35083910051).

## Qualification evidence

`qualification/milestone-21.json` declares ordinary, differential, client,
Kubernetes, HA, scale, migration, recovery, and security gates. Each matrix job
writes its exact command, commit, result count, duration, skipped and flaky
counts, immutable run URL, and checksum. The report job promotes the draft only
when every declared command has matching passing evidence and no command is
failed, flaky, skipped, or missing.

Release tags consume that exact-commit report. Release publication then pulls
the published digest for both `linux/amd64` and `linux/arm64`, runs the clean
four-signal smoke, and publishes the report, checksums, SBOM, and attestations.
