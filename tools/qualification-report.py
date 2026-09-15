#!/usr/bin/env python3
"""Validate and promote the Milestone 16 compatibility report."""

import argparse
import copy
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "qualification/milestone-16.json"
SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
REQUIRED_GATES = {
    "ordinary",
    "oracle_versions",
    "differential",
    "grafana",
    "clients_and_correlation",
    "lifecycle_and_failures",
    "security_and_limits",
    "scale",
    "client_oracles",
    "persisted_upgrade",
}
WORKFLOW = ROOT / ".github/workflows/qualification.yml"


def fail(message):
    raise ValueError(message)


def load(path):
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def workflow_commands():
    source = WORKFLOW.read_text(encoding="utf-8")
    return {
        match.group(1).strip()
        for match in re.finditer(r"^\s+command:\s+(.+)$", source, re.MULTILINE)
    }


def validate(report, final=False, commit=None):
    if report.get("schema_version") != 1:
        fail("schema_version must be 1")
    if report.get("status") not in {"draft", "qualified"}:
        fail("status must be draft or qualified")

    products = report.get("artifacts", {})
    if set(products) != {"alloy", "grafana", "loki", "mimir", "minio", "prometheus", "pyroscope", "tempo"}:
        fail("artifacts must name every pinned qualification image")
    for name, artifact in products.items():
        if not artifact.get("image") or not SHA256.fullmatch(artifact.get("digest", "")):
            fail(f"{name} must have an image and immutable sha256 digest")

    module = (ROOT / "MODULE.bazel").read_text(encoding="utf-8")
    pins = {
        name: {"image": f"{registry}/{repository}:{tag}", "digest": digest}
        for name, registry, repository, tag, digest in re.findall(
            r'\("([^"]+)", "([^"]+)", "([^"]+)", "([^"]+)", "(sha256:[0-9a-f]{64})"\)',
            module,
        )
    }
    for name, artifact in products.items():
        if pins.get(name) != {key: artifact[key] for key in ("image", "digest")}:
            fail(f"{name} differs from MODULE.bazel")

    upstream = load(ROOT / "docs/api/upstream_surfaces.json")["products"]
    for name in ("loki", "mimir", "pyroscope", "tempo"):
        for key in ("digest", "revision"):
            if products[name].get(key) != upstream[name].get(key):
                fail(f"{name} {key} differs from the upstream surface manifest")

    clients = load(ROOT / "docs/api/client_oracles.json")["products"]
    if set(clients) != {"alloy", "grafana", "prometheus"}:
        fail("client oracle manifest must name Alloy, Grafana, and Prometheus")
    for name, client in clients.items():
        required = client.get("classifications", {}).get("required", [])
        if not required or not all(isinstance(surface, str) and surface for surface in required):
            fail(f"{name} must classify its required public workflows")
        for key in ("image", "digest", "revision"):
            if products[name].get(key) != client.get(key):
                fail(f"{name} {key} differs from the client oracle manifest")

    manifests = report.get("manifests", [])
    required_manifests = {
        "docs/api/upstream_surfaces.json",
        "docs/api/client_oracles.json",
        "docs/api/routes.json",
        "docs/persisted_formats.md",
    }
    if set(manifests) != required_manifests:
        fail("report must name the API, client, route, and persisted-format manifests")

    gates = report.get("gates", [])
    ids = {gate.get("id") for gate in gates}
    if ids != REQUIRED_GATES or len(gates) != len(REQUIRED_GATES):
        fail("qualification gates are incomplete or duplicated")
    for gate in gates:
        if not gate.get("commands") or not all(isinstance(command, str) and command for command in gate["commands"]):
            fail(f"{gate['id']} must name its commands")
        if not isinstance(gate.get("command_count"), int) or gate["command_count"] < 0:
            fail(f"{gate['id']} must have a non-negative command_count")
        if gate.get("result") not in {"pending", "passed"}:
            fail(f"{gate['id']} has an invalid result")

    declared_commands = {command for gate in gates for command in gate["commands"]}
    if workflow_commands() != declared_commands:
        fail("qualification workflow commands differ from the report")

    if final:
        if report["status"] != "qualified":
            fail("final report must be qualified")
        actual = report.get("krabka_commit", "")
        if not COMMIT.fullmatch(actual) or (commit and actual != commit):
            fail("final report must name the exact immutable Krabka commit")
        for gate in gates:
            if gate["result"] != "passed" or gate["command_count"] != len(gate["commands"]):
                fail(f"{gate['id']} has incomplete command evidence")
            evidence = gate.get("evidence", "")
            if not re.fullmatch(r"https://github\.com/[^/]+/[^/]+/actions/runs/[0-9]+", evidence):
                fail(f"{gate['id']} evidence must be an immutable Actions run URL")


def promote(report, commit, evidence):
    validate(report)
    if not COMMIT.fullmatch(commit):
        fail("--commit must be a full Git commit")
    promoted = copy.deepcopy(report)
    promoted["status"] = "qualified"
    promoted["krabka_commit"] = commit
    for gate in promoted["gates"]:
        gate["result"] = "passed"
        gate["command_count"] = len(gate["commands"])
        gate["evidence"] = evidence
    validate(promoted, final=True, commit=commit)
    return promoted


def self_test():
    sample = load(REPORT)
    validate(sample)
    drifted = copy.deepcopy(sample)
    drifted["gates"][0]["commands"].append("false")
    try:
        validate(drifted)
    except ValueError:
        pass
    else:
        fail("workflow command drift unexpectedly validated")
    try:
        validate(sample, final=True)
    except ValueError:
        pass
    else:
        fail("draft unexpectedly passed final validation")
    final = promote(sample, "a" * 40, "https://github.com/krabka-io/krabka-o11y/actions/runs/1")
    validate(final, final=True, commit="a" * 40)
    broken = copy.deepcopy(final)
    broken["gates"][0]["command_count"] = 0
    try:
        validate(broken, final=True, commit="a" * 40)
    except ValueError:
        return
    fail("empty passing evidence unexpectedly validated")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--final", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--commit")
    parser.add_argument("--evidence")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
            print("qualification report self-test passed")
            return
        report = load(args.report)
        if args.promote:
            if not args.commit or not args.evidence or not args.output:
                fail("--promote requires --commit, --evidence, and --output")
            report = promote(report, args.commit, args.evidence)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        validate(report, final=args.final, commit=args.commit)
        print(f"qualification report is valid ({report['status']}, {len(report['gates'])} gates)")
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"qualification-report.py: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
