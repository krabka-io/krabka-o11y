# krabka-o11y-fuzz

Fuzz targets for the wire decoders and the query-language parsers.

Part of [krabka-o11y](https://github.com/krabka-io/krabka-o11y), the Krabka observability stack.

## Overview

[The code style guide](../docs/style_guides/code_style_guide.md) states a rule for every decoder in this workspace: never `panic!` in response to malformed wire input, decoders return errors, and property tests and fuzzing check that. This directory holds the fuzzing half.

Each target drives one decoder or one parser. The decoders are the surfaces that read bytes a client chose, before anything has authenticated the client. The Jaeger compact-Thrift decoder is the widest of them, because the same code serves an HTTP push door and a UDP datagram handler, and a datagram carries attacker-chosen bytes off a connectionless socket.

The decoders were not unguarded before this directory existed. They return typed errors, they bound their reads with `checked_add` and `try_from`, and each one has hand-written error-path tests. What was missing is the check the style guide names, and a hand-written error case only covers the input its author thought of.

## Layout

| Path | Holds |
| --- | --- |
| `fuzz_targets/` | One file per target. |
| `src/` | The input builders the structured targets use. |
| `corpus/` | Checked-in seeds, written by [//tools/fuzz-corpus.py](../tools/fuzz-corpus.py). |

## Targets

| Target | Decoder | Input |
| --- | --- | --- |
| `jaeger_compact_thrift` | `decode_jaeger_thrift` | Raw bytes. |
| `jaeger_compact_structured` | `decode_jaeger_thrift` | A `Batch` whose framing holds and whose field ids, wire types and values the fuzzer picks. |
| `jaeger_binary_thrift` | `decode_jaeger_binary_thrift` | Raw bytes. |
| `zipkin_json` | `decode_zipkin` | Raw bytes. |
| `pprof_profile` | `PprofProfile::decode` | Raw bytes. |
| `remote_write_v1` | `decode_v1` | Raw bytes, so the snappy bound is in scope. |
| `remote_write_v1_structured` | `decode_v1` | A `WriteRequest` the fuzzer filled in, encoded and compressed. |
| `remote_write_v2` | `decode_v2` | Raw bytes. |
| `remote_read` | `decode_read_request` | Raw bytes. |
| `traces_otlp` | `decode_otlp` | Raw bytes, decoded to `TracesData` first. |
| `metrics_otlp` | `decode_otlp_bytes` | Raw bytes. |
| `promql_parse` | `parse_promql` | Text. |
| `logql_parse` | `parse_query` and `parse_logql_expr` | Text. |
| `traceql_parse` | `krabka_traceql::parse` | Text. |

A raw-bytes target and a structured target ask different questions of the same decoder. The raw one covers the outermost framing and the length bounds. The structured one gets past both and reaches the arithmetic behind them, which random bytes almost never do. Two decoders have both, and the rest have the raw form only.

## Running

```bash
tools/fuzz.sh                                   # every target, 60 seconds each
tools/fuzz.sh 300 jaeger_compact_thrift         # one target, 300 seconds
```

The script copies `corpus/` into a scratch directory before it runs, so a run never edits the checked-in seeds. The nightly `fuzz` job in //.github/workflows/ci.yml runs the same script with the same per-target budget, and each night starts from the checked-in seeds again.

To drive cargo-fuzz directly:

```bash
cargo +nightly fuzz build --fuzz-dir fuzz -s none --codegen-units 16
cargo +nightly fuzz run --fuzz-dir fuzz -s none jaeger_compact_thrift -- -max_total_time=60
```

`--fuzz-dir fuzz` is needed on every command, because this directory sits at the repository root rather than under one crate. `cargo fuzz list --fuzz-dir fuzz` names the targets.

Install the tool with `cargo install cargo-fuzz --locked --version 0.13.2`. The nightly `fuzz` job pins the same version.

### The toolchain

cargo-fuzz needs nightly. //rust-toolchain.toml pins stable 1.97.1, and Bazel builds through rules_rs against a pinned toolchain, so this directory is a workspace of its own and every command here names `+nightly`. `cargo build --workspace` and `bazel build //...` do not reach it. //Cargo.toml names it in `exclude`.

The channel is `nightly` rather than a date. This is the one place the repository floats a toolchain, and the reason is that the `-Z` flags cargo-fuzz passes are not stable, so a pinned date buys reproducibility at the cost of an old compiler that the tool may stop supporting. The job that runs these targets is scheduled, not a gate on a pull request, so a nightly regression delays a report and blocks nobody.

### The sanitizer

The runs set `-s none`. This workspace sets `unsafe_code = "forbid"`, so Address Sanitizer has no memory-unsafety to find here. What the targets look for is a panic, a hang, or an allocation sized from a number the sender chose, and libFuzzer reports all three without a sanitizer.

### The warnings every build here prints

Cargo prints two sets of manifest warnings on every build in this directory. Neither is a lint on the code, and `-D warnings` does not reach either, because both come from Cargo rather than from rustc.

- `patch ... was not used in the crate graph`, once for each broker-side sibling crate. The root workspace reaches those only through a dev-dependency on `krabka-broker`, and this crate takes no dev-dependency. The table is kept whole so [//tools/check-fuzz-patch.sh](../tools/check-fuzz-patch.sh) can compare it against //Cargo.toml line by line.
- `binary <name> should have a kebab-case name`, once for each target. The names stay in snake case, because each one has to match its file under `fuzz_targets/` and its directory under `corpus/`, and because that is the spelling cargo-fuzz itself writes.

## Corpus

[//tools/fuzz-corpus.py](../tools/fuzz-corpus.py) writes `corpus/`. Every seed comes from something the repository already holds:

- The PromQL seeds are the queries in the vendored Prometheus conformance corpus under `crates/promql/tests/testdata/`.
- The TraceQL seeds are the queries in the golden corpus under `crates/traceql/tests/testdata/traceql/`.
- The LogQL seeds are the stream selectors the LogQL and observability sources drive their parsers with.
- The Zipkin seeds are the JSON bodies `crates/traces/src/wire/zipkin.rs` decodes in its tests.
- The binary seeds are re-encodings of the fixtures the crates' own tests build, such as the Jaeger sample batch in `crates/traces/src/wire/jaeger` and the `WriteRequest` in `crates/metrics/src/wire/v1`.

Regenerate with `tools/fuzz-corpus.py`, and check with `tools/fuzz-corpus.py --check`. The script owns every file under `corpus/`, and the check compares the directory against what the script would write, in both directions. To keep an input a run found, add it to the script. That gives it a name and a reason, and puts it in front of a reviewer.

Four targets start from an empty corpus. `metrics_otlp` and `traces_otlp` would need an OTLP encoder this repository does not hold. The two structured targets take `Arbitrary`'s encoding rather than a wire format, so a wire-format seed says nothing to them.

Four decoders have no target at all:

- `normalize_loki_http_push` and `decode_loki_http_body`, which are `pub(crate)` in `krabka-observability`. Reaching them needs either a public re-export or an axum router under a tokio runtime.
- `decode_jaeger_grpc_batch` and the `krabka-profiles` ingest doors, which take a generated message rather than bytes, so a target for them needs an `Arbitrary` mirror of the generated types the way `src/remote_write.rs` holds one for `WriteRequest`.

## Reporting a finding

libFuzzer writes the input to `fuzz/artifacts/<target>/` and prints the path. Report the decoder, the input and the panic. Add the input to [//tools/fuzz-corpus.py](../tools/fuzz-corpus.py) as a seed only after the decoder is fixed, so the corpus stays green.

## License

Apache-2.0. See [LICENSE](../LICENSE).
