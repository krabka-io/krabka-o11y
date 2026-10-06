#!/usr/bin/env python3
"""Build immutable upstream query runners; preserve source and adaptation provenance."""

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tarfile
import urllib.request

RUNNERS = {
    "promql": ("prometheus/compliance", "67b8327a2e93dc28f64d4b21bbce00b362f565d5", "go1.25.0"),
    "logql": ("grafana/loki", "7a40404f32b3e6464c9cfc6cc7dd75a40f3931da", "go1.26.5"),
}
ARCHIVE_SHA256 = {
    "promql": "9c45856bef0b599445a7e72cd213d476434030fcd2e2e934e9242fedc799ccc8",
    "logql": "426e46991bc84535681b7b38a407655bfc2c685c7da6358adffe3aa53c79ef09",
}


def build(name, destination):
    repository, revision, toolchain = RUNNERS[name]
    archive = destination / f"{name}-{revision}.tar.gz"
    if not archive.exists():
        url = f"https://codeload.github.com/{repository}/tar.gz/{revision}"
        with urllib.request.urlopen(url, timeout=120) as response, archive.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != ARCHIVE_SHA256[name]:
        raise RuntimeError("upstream runner source archive differs from the pinned hash")
    source = destination / f"{repository.split('/')[-1]}-{revision}"
    temporary = destination / "tmp"
    temporary.mkdir(exist_ok=True)
    environment = dict(os.environ, GOTOOLCHAIN=toolchain, GOMAXPROCS="4", TMPDIR=str(temporary))
    binary = destination / f"{name}-upstream-runner"
    provenance_path = destination / f"{name}-runner-provenance.json"
    if binary.exists() and provenance_path.exists():
        previous = json.loads(provenance_path.read_text())
        if (previous.get("revision") == revision and previous.get("toolchain") == toolchain
                and previous.get("builder_sha256") == hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
                and previous.get("binary_sha256") == hashlib.sha256(binary.read_bytes()).hexdigest()
                and all((source / adaptation["path"]).exists() and
                        hashlib.sha256((source / adaptation["path"]).read_bytes()).hexdigest() == adaptation["adapted_sha256"]
                        for adaptation in previous.get("adaptations", []) if adaptation["path"] != "pkg/logql/bench/remote_test.go")):
            return
    # A rebuilt binary must come from the pinned archive, including files
    # outside the explicitly recorded adaptations.
    if source.exists():
        shutil.rmtree(source)
    with tarfile.open(archive) as contents:
        contents.extractall(destination, filter="data")
    adaptations = []
    def adapt_catalogue(relative, purpose, transform):
        with tarfile.open(archive) as contents:
            original = contents.extractfile(f"{source.name}/{relative}").read().decode()
        adapted = transform(original)
        (source / relative).write_text(adapted)
        adaptations.append({"path": relative, "purpose": purpose,
                            "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
                            "adapted_sha256": hashlib.sha256(adapted.encode()).hexdigest()})

    if name == "promql":
        def utf8_label_expectations(text):
            for query in [
                'label_replace(demo_num_cpus, "~invalid", "", "src", "(.*)")',
                'label_join(demo_num_cpus, "~invalid", "-", "instance")',
            ]:
                old = f"  - query: '{query}'\n    should_fail: true"
                if text.count(old) != 1:
                    raise RuntimeError("pinned compliance UTF-8 expectation changed")
                text = text.replace(old, f"  - query: '{query}'\n    should_fail: false", 1)
            return text
        adapt_catalogue("promql/promql-test-queries.yml", "Prometheus 3.14 accepts nonempty UTF-8 label names", utf8_label_expectations)
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/promql-compliance-tester"],
                       cwd=source / "promql", env=environment, check=True)
    else:
        def metric_kinds(text):
            descriptions = [
                "Loki duration unwrap with logfmt", "Max of min duration by level", "Avg duration by level",
                "Max avg duration without grouping", "Max avg duration by level without service_name",
                "Max sum without grouping modifier", "Avg duration without grouping", "Sum duration without outer aggregation",
            ]
            for description in descriptions:
                marker = f"  - description: {description}\n"
                if text.count(marker) != 1:
                    raise RuntimeError("pinned Loki omitted-kind catalogue changed")
                text = text.replace(marker, marker + "    kind: metric\n", 1)
            return text
        adapt_catalogue("pkg/logql/bench/queries/exhaustive/aggregations.yaml",
                        "Execute eight metric definitions omitted by the upstream default-kind expansion", metric_kinds)
        # The pinned catalogue deliberately includes an empty-result log query,
        # but its runner rejects all empty baselines. Adapt only that named case;
        # keep it in the denominator and independently require both answers empty.
        remote = source / "pkg/logql/bench/remote_test.go"
        original = remote.read_text()
        old = '\t\t\tassertResultNotEmpty(t, expected, "baseline returned empty")\n\t\t\tassertResultNotEmpty(t, actual, "test endpoint returned empty")'
        new = '''\t\t\tif tc.Query == "" { t.Fatal("query must not be empty") }
\t\t\tif tc.QueryDesc == "Log query with impossible filter (guarantees empty results, exercises log result cache)" {
\t\t\t\trequire.Empty(t, expected)
\t\t\t\trequire.Empty(t, actual)
\t\t\t} else {
\t\t\t\tassertResultNotEmpty(t, expected, "baseline returned empty")
\t\t\t\tassertResultNotEmpty(t, actual, "test endpoint returned empty")
\t\t\t}'''
        if old not in original:
            raise RuntimeError("pinned Loki nonempty guard changed; review adaptation")
        adapted = original.replace(old, new, 1)
        adapted = adapted.replace('\t"flag"\n', '\t"encoding/json"\n\t"flag"\n', 1)
        query_log = '\t\t\tt.Logf("Query: %s", tc.Description())'
        query_metadata = '''\t\t\tt.Logf("Query: %s", tc.Description())
\t\t\tmetadata := map[string]any{
\t\t\t\t"name": t.Name(), "query": tc.Query, "kind": tc.Kind(),
\t\t\t\t"direction": fmt.Sprint(tc.Direction), "source": tc.Source,
\t\t\t\t"description": tc.QueryDesc, "start_ns": tc.Start.UnixNano(),
\t\t\t\t"end_ns": tc.End.UnixNano(), "step_ns": tc.Step.Nanoseconds(),
\t\t\t\t"tolerance": *remoteTolerance, "expected_outcome": "query-result",
\t\t\t}
\t\t\tif tc.QueryDesc == "Log query with impossible filter (guarantees empty results, exercises log result cache)" {
\t\t\t\tmetadata["expected_outcome"] = "semantic-negative"
\t\t\t}
\t\t\tencoded, err := json.Marshal(metadata)
\t\t\trequire.NoError(t, err)
\t\t\tt.Logf("KRABKA_QUERY_CASE %s", encoded)'''
        compare = '\t\t\tassertDataEqualWithTolerance(t, expected, actual, *remoteTolerance)'
        completed = compare + '''
\t\t\tif !t.Failed() {
\t\t\t\tt.Logf("KRABKA_QUERY_VERDICT %s", t.Name())
\t\t\t}'''
        if adapted.count(query_log) != 1 or adapted.count(compare) != 1:
            raise RuntimeError("pinned Loki case logging/comparator seam changed")
        adapted = adapted.replace(query_log, query_metadata, 1).replace(compare, completed, 1)
        remote.write_text(adapted)
        adaptations.append({"path": "pkg/logql/bench/remote_test.go", "purpose": "assert both intentional empty answers; preserve exact query/request metadata and completed semantic comparisons",
                            "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
                            "adapted_sha256": hashlib.sha256(adapted.encode()).hexdigest()})
        try:
            subprocess.run(["go", "test", "-c", "-tags=remote_correctness", "-o", str(binary), "./pkg/logql/bench"],
                           cwd=source, env=environment, check=True)
        finally:
            remote.write_text(original)
    provenance = {"repository": repository, "revision": revision, "toolchain": toolchain,
                  "builder_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
                  "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                  "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "adaptations": adaptations}
    (destination / f"{name}-runner-provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps({"runner": name, "binary": str(binary), "source": str(source)}), flush=True)


def bazel_command(arguments, destination):
    """Prepare only the runners needed by these existing Bazel targets."""
    environment = []
    runners = {}
    joined = " ".join(arguments)
    for name, target in [("promql", "diff_prometheus"), ("logql", "loki_differential")]:
        if target not in joined:
            continue
        build(name, destination)
        binary = destination / f"{name}-upstream-runner"
        provenance = destination / f"{name}-runner-provenance.json"
        runners[name] = {"binary": str(binary),
                         "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                         "provenance_sha256": hashlib.sha256(provenance.read_bytes()).hexdigest()}
        repository, revision, _ = RUNNERS[name]
        source = destination / f"{repository.split('/')[-1]}-{revision}"
        if name == "promql":
            values = {"KRABKA_PROMQL_COMPLIANCE_BIN": destination / "promql-upstream-runner",
                      "KRABKA_PROMQL_COMPLIANCE_QUERIES": source / "promql/promql-test-queries.yml"}
        else:
            values = {"KRABKA_LOKI_CORRECTNESS_BIN": destination / "logql-upstream-runner",
                      "KRABKA_LOKI_CORRECTNESS_SOURCE": source}
        environment.extend(f"--test_env={key}={value}" for key, value in values.items())
    # Last flag wins in Bazel: preserve the exact binaries whose hashes we record.
    command = ["bazel", *arguments, *environment]
    invocation = {"command": command, "runners": runners}
    invocation_path = destination / "query-runner-invocation.json"
    invocation_path.write_text(json.dumps(invocation, indent=2) + "\n")
    result = subprocess.call(command)
    invocation["exit_code"] = result
    invocation["binaries_unchanged"] = all(
        hashlib.sha256(pathlib.Path(runner["binary"]).read_bytes()).hexdigest() == runner["binary_sha256"]
        for runner in runners.values())
    invocation_path.write_text(json.dumps(invocation, indent=2) + "\n")
    if not invocation["binaries_unchanged"]:
        raise RuntimeError("upstream runner binary changed during Bazel execution")
    return result


def self_test():
    # Check target selection and exact argument forwarding without Go/network/Bazel.
    from unittest.mock import patch
    import tempfile
    with tempfile.TemporaryDirectory() as temporary, patch(__name__ + ".build") as prepare, patch("subprocess.call", return_value=0) as run:
        destination = pathlib.Path(temporary)
        (destination / "promql-upstream-runner").write_bytes(b"pinned runner")
        (destination / "promql-runner-provenance.json").write_text("{}\n")
        assert bazel_command(["test", "--config=docker", "//crates/metrics-service:diff_prometheus_docker_test"], destination) == 0
        prepare.assert_called_once_with("promql", destination)
        assert f"--test_env=KRABKA_PROMQL_COMPLIANCE_BIN={destination}/promql-upstream-runner" in run.call_args.args[0]
        assert "//crates/metrics-service:diff_prometheus_docker_test" in run.call_args.args[0]
        invocation = json.loads((destination / "query-runner-invocation.json").read_text())
        assert invocation["exit_code"] == 0 and invocation["binaries_unchanged"]
        assert invocation["runners"]["promql"]["binary_sha256"] == hashlib.sha256(b"pinned runner").hexdigest()
        run.side_effect = lambda _: (destination / "promql-upstream-runner").write_bytes(b"changed") and 0
        try:
            bazel_command(["test", "//crates/metrics-service:diff_prometheus_docker_test"], destination)
        except RuntimeError:
            pass
        else:
            raise AssertionError("changed runner binary was accepted")
        run.side_effect = None
        prepare.reset_mock()
        bazel_command(["test", "//crates/traces:tempo_differential_docker_test"], destination)
        prepare.assert_not_called()
    print("query runner dispatch self-check passed")


def main():
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return
    if sys.argv[1:2] == ["--bazel"]:
        if len(sys.argv) < 4 or sys.argv[2] != "test":
            raise SystemExit("--bazel requires test and at least one target")
        destination = pathlib.Path(os.environ.get("QUERY_RUNNER_DIR", os.environ.get("RUNNER_TEMP", "target") + "/query-runners")).resolve()
        destination.mkdir(parents=True, exist_ok=True)
        raise SystemExit(bazel_command(sys.argv[2:], destination))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runner", choices=[*RUNNERS, "all"])
    parser.add_argument("destination", type=pathlib.Path)
    args = parser.parse_args()
    destination = args.destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    for name in RUNNERS if args.runner == "all" else [args.runner]:
        build(name, destination)


if __name__ == "__main__":
    main()
