#!/usr/bin/env python3
"""Validate and promote the Milestone 21 compatibility report."""

import argparse
import copy
import hashlib
import json
import re
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "qualification/milestone-21.json"
WORKFLOW = ROOT / ".github/workflows/qualification.yml"
SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
RUN_URL = re.compile(r"https://github\.com/[^/]+/[^/]+/actions/runs/[0-9]+")
REQUIRED_GATES = {"ordinary", "differential", "clients", "kubernetes", "ha", "scale", "migration", "recovery", "security"}


def fail(message):
    raise ValueError(message)


def load(path):
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def checksum(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def workflow_commands():
    return {
        match.group(1).strip()
        for match in re.finditer(r"^\s+command:\s+(.+)$", WORKFLOW.read_text(encoding="utf-8"), re.MULTILINE)
    }


def module_pins():
    module = (ROOT / "MODULE.bazel").read_text(encoding="utf-8")
    return {
        name: {"image": f"{registry}/{repository}:{tag}", "digest": digest}
        for name, registry, repository, tag, digest in re.findall(
            r'\("([^"]+)", "([^"]+)", "([^"]+)", "([^"]+)", "(sha256:[0-9a-f]{64})"\)',
            module,
        )
    }


def validate(report, final=False, commit=None):
    if report.get("schema_version") != 2 or report.get("status") not in {"draft", "qualified"}:
        fail("report must use schema 2 and have draft or qualified status")
    if report.get("support_window") != "current and previous minor release":
        fail("report must name the current and previous minor support window")

    products = report.get("artifacts", {})
    expected = {"alloy", "alloy_previous", "grafana", "grafana_previous", "loki", "mimir", "minio", "prometheus", "prometheus_previous", "pyroscope", "tempo"}
    if set(products) != expected:
        fail("artifacts must name every current and previous qualification image")
    pins = module_pins()
    for name, artifact in products.items():
        if pins.get(name) != {key: artifact[key] for key in ("image", "digest")}:
            fail(f"{name} differs from MODULE.bazel")

    upstream = load(ROOT / "docs/api/upstream_surfaces.json")["products"]
    for name in ("loki", "mimir", "pyroscope", "tempo"):
        for key in ("digest", "revision"):
            if products[name].get(key) != upstream[name].get(key):
                fail(f"{name} {key} differs from the upstream surface manifest")

    client_manifest = load(ROOT / "docs/api/client_oracles.json")
    if client_manifest.get("support_window") != report["support_window"]:
        fail("client support window differs from the report")
    for name, client in client_manifest["products"].items():
        required = client.get("classifications", {}).get("required", [])
        if not required:
            fail(f"{name} has no required public workflows")
        for window in ("current", "previous"):
            artifact = products[name if window == "current" else f"{name}_previous"]
            for key in ("image", "digest", "revision"):
                if artifact.get(key) != client[window].get(key):
                    fail(f"{name} {window} {key} differs from the client oracle manifest")

    for window in ("current", "previous"):
        expected_protocol = client_manifest["protocols"]["otlp"][window]
        actual_protocol = report.get("protocols", {}).get("otlp", {}).get(window, {})
        for key in ("tag", "revision", "sha256"):
            if actual_protocol.get(key) != expected_protocol.get(key):
                fail(f"OTLP {window} {key} differs from the client oracle manifest")

    fixtures = load(ROOT / "qualification/otlp-contracts.json")
    for window in ("current", "previous"):
        if fixtures.get("windows", {}).get(window) != report["protocols"]["otlp"][window]:
            fail(f"OTLP {window} fixture identity differs from the report")
    if {fixture.get("signal") for fixture in fixtures.get("fixtures", [])} != {"metrics", "logs", "traces", "profiles"}:
        fail("OTLP fixtures must cover all four signals")
    otlp_command = next(command for gate in report["gates"] if gate["id"] == "clients" for command in gate["commands"] if "distributor_otlp_test" in command)
    if not all(fixture.get("target") in otlp_command and fixture.get("transports") and fixture.get("bounds") for fixture in fixtures["fixtures"]):
        fail("OTLP fixtures are not connected to the qualification command")

    required_manifests = {"docs/api/upstream_surfaces.json", "docs/api/client_oracles.json", "docs/api/routes.json", "docs/api_compatibility.md", "docs/disaster_recovery.md", "docs/persisted_formats.md", "docs/releases/milestone-21-six-month-report.md", "qualification/otlp-contracts.json"}
    if set(report.get("manifests", [])) != required_manifests:
        fail("report must name every compatibility and operations manifest")

    demo = report.get("demo_qualification", {})
    if not COMMIT.fullmatch(demo.get("commit", "")) or not RUN_URL.fullmatch(demo.get("run_url", "")):
        fail("demo qualification must name an immutable commit and run")

    gates = report.get("gates", [])
    if {gate.get("id") for gate in gates} != REQUIRED_GATES or len(gates) != len(REQUIRED_GATES):
        fail("qualification gates are incomplete or duplicated")
    for gate in gates:
        if not gate.get("commands") or len(gate["commands"]) != len(set(gate["commands"])):
            fail(f"{gate['id']} has empty or duplicated commands")
        if gate.get("result") not in {"pending", "passed"}:
            fail(f"{gate['id']} has an invalid result")
    if workflow_commands() != {command for gate in gates for command in gate["commands"]}:
        fail("qualification workflow commands differ from the report")

    if not final:
        return
    if report["status"] != "qualified":
        fail("final report must be qualified")
    actual = report.get("krabka_commit", "")
    if not COMMIT.fullmatch(actual) or (commit and actual != commit):
        fail("final report must name the exact immutable Krabka commit")
    for gate in gates:
        count = len(gate["commands"])
        if gate["result"] != "passed" or gate["command_count"] != count or gate["result_count"] < count:
            fail(f"{gate['id']} has incomplete command evidence")
        if gate["flaky_count"] or gate["skipped_count"] or len(gate["evidence"]) != count:
            fail(f"{gate['id']} contains flaky, skipped, or missing evidence")
        if gate["duration_seconds"] < 0:
            fail(f"{gate['id']} has an invalid duration")
        for item in gate["evidence"]:
            if not SHA256.fullmatch(item.get("checksum", "")) or not RUN_URL.fullmatch(item.get("run_url", "")):
                fail(f"{gate['id']} evidence is not immutable")


def load_evidence(directory, commit):
    items = []
    for path in sorted(directory.glob("*.json")):
        item = load(path)
        recorded_checksum = item.pop("checksum", "")
        if checksum(item) != recorded_checksum:
            fail(f"{path.name} evidence checksum does not match")
        item["checksum"] = recorded_checksum
        if item.get("commit") != commit or item.get("result") != "passed" or item.get("exit_code") != 0:
            fail(f"{path.name} is not passing evidence for {commit}")
        if not isinstance(item.get("result_count"), int) or item["result_count"] < 1 or item.get("flaky_count") != 0 or item.get("skipped_count") != 0:
            fail(f"{path.name} is incomplete, flaky, or skipped")
        if not RUN_URL.fullmatch(item.get("run_url", "")) or not isinstance(item.get("duration_seconds"), (int, float)):
            fail(f"{path.name} has no immutable run or duration")
        items.append(item)
    return items


def promote(report, commit, evidence_dir):
    validate(report)
    if not COMMIT.fullmatch(commit):
        fail("--commit must be a full Git commit")
    items = load_evidence(evidence_dir, commit)
    expected = {(gate["id"], command) for gate in report["gates"] for command in gate["commands"]}
    actual = {(item.get("gate"), item.get("command")) for item in items}
    if actual != expected or len(items) != len(expected):
        fail("gate evidence differs from the declared qualification matrix")

    promoted = copy.deepcopy(report)
    promoted["status"] = "qualified"
    promoted["krabka_commit"] = commit
    for gate in promoted["gates"]:
        evidence = [item for item in items if item["gate"] == gate["id"]]
        gate["result"] = "passed"
        gate["command_count"] = len(evidence)
        gate["result_count"] = sum(item["result_count"] for item in evidence)
        gate["duration_seconds"] = round(sum(item["duration_seconds"] for item in evidence), 3)
        gate["flaky_count"] = sum(item["flaky_count"] for item in evidence)
        gate["skipped_count"] = sum(item["skipped_count"] for item in evidence)
        gate["evidence"] = [
            {key: item[key] for key in ("id", "run_url", "duration_seconds", "result_count", "checksum")}
            for item in evidence
        ]
    validate(promoted, final=True, commit=commit)
    return promoted


def self_test():
    report = load(REPORT)
    validate(report)
    commit = "a" * 40
    with tempfile.TemporaryDirectory() as directory:
        evidence_dir = Path(directory)
        for index, (gate, command) in enumerate((gate["id"], command) for gate in report["gates"] for command in gate["commands"]):
            item = {"schema_version": 1, "id": f"check-{index}", "gate": gate, "command": command, "commit": commit, "run_url": "https://github.com/krabka-io/krabka-o11y/actions/runs/1", "started_at": "2026-01-01T00:00:00+00:00", "finished_at": "2026-01-01T00:00:01+00:00", "duration_seconds": 1.0, "result": "passed", "exit_code": 0, "result_count": 1, "flaky_count": 0, "skipped_count": 0}
            item["checksum"] = checksum(item)
            (evidence_dir / f"{index}.json").write_text(json.dumps(item), encoding="utf-8")
        final = promote(report, commit, evidence_dir)
        final["gates"][0]["flaky_count"] = 1
        try:
            validate(final, final=True, commit=commit)
        except ValueError:
            return
    fail("flaky evidence unexpectedly validated")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--final", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--commit")
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
            print("qualification report self-test passed")
            return
        report = load(args.report)
        if args.promote:
            if not args.commit or not args.evidence_dir or not args.output:
                fail("--promote requires --commit, --evidence-dir, and --output")
            report = promote(report, args.commit, args.evidence_dir)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        validate(report, final=args.final, commit=args.commit)
        print(f"qualification report is valid ({report['status']}, {len(report['gates'])} gates)")
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"qualification-report.py: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
