#!/usr/bin/env python3
"""Replay one checked-in OTLP contract against its pinned source schema."""

import argparse
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "qualification/otlp-contracts.json"
CLIENTS = ROOT / "docs/api/client_oracles.json"
FIXTURES = {
    "metrics": (
        "opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
        "opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest",
        'resource_metrics { resource { attributes { key: "service.name" value { string_value: "qualification" } } } }',
        "resource_metrics",
    ),
    "logs": (
        "opentelemetry/proto/collector/logs/v1/logs_service.proto",
        "opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest",
        'resource_logs { resource { attributes { key: "service.name" value { string_value: "qualification" } } } }',
        "resource_logs",
    ),
    "traces": (
        "opentelemetry/proto/collector/trace/v1/trace_service.proto",
        "opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest",
        'resource_spans { resource { attributes { key: "service.name" value { string_value: "qualification" } } } }',
        "resource_spans",
    ),
    "profiles": (
        "opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
        "opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest",
        'resource_profiles { resource { attributes { key: "process.pid" value { int_value: 7 } } } } dictionary {}',
        "resource_profiles",
    ),
}


def load(path):
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def contract(window):
    declared = load(CONTRACTS)["windows"][window]
    client = load(CLIENTS)["protocols"]["otlp"][window]
    if declared != {key: client[key] for key in ("tag", "revision", "sha256")}:
        raise ValueError(f"OTLP {window} identities differ between manifests")
    return client


def protoc():
    if configured := os.environ.get("PROTOC"):
        return Path(configured)
    subprocess.run(["bazel", "build", "//bazel:protoc"], check=True)
    relative = subprocess.run(
        ["bazel", "cquery", "//bazel:protoc", "--output=files"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    execution_root = subprocess.run(
        ["bazel", "info", "execution_root"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    compiler = Path(execution_root) / relative
    if not compiler.is_file():
        raise ValueError("Bazel did not materialize the pinned protoc")
    return compiler


def replay(window):
    pin = contract(window)
    with urllib.request.urlopen(pin["source"], timeout=60) as response:
        archive = response.read()
    if hashlib.sha256(archive).hexdigest() != pin["sha256"]:
        raise ValueError(f"OTLP {window} archive checksum differs from its pin")

    compiler = protoc()
    encoded = []
    with tempfile.TemporaryDirectory() as directory:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as source:
            source.extractall(directory, filter="data")
        schema_root = Path(directory) / f"opentelemetry-proto-{pin['revision']}"
        for signal, (proto, message, fixture, marker) in FIXTURES.items():
            source = schema_root / proto
            wire = subprocess.run(
                [compiler, f"-I{schema_root}", f"--encode={message}", source],
                input=fixture.encode(),
                check=True,
                stdout=subprocess.PIPE,
            ).stdout
            decoded = subprocess.run(
                [compiler, f"-I{schema_root}", f"--decode={message}", source],
                input=wire,
                check=True,
                stdout=subprocess.PIPE,
                text=False,
            ).stdout.decode()
            if not wire or marker not in decoded:
                raise ValueError(f"OTLP {window} {signal} fixture did not round trip")
            encoded.append(wire)

    digest = hashlib.sha256(b"".join(encoded)).hexdigest()
    print(f"OTLP {window} {pin['tag']} {pin['revision']}: {len(encoded)} schema fixtures passed; wire sha256:{digest}")


def self_test():
    if set(FIXTURES) != {item["signal"] for item in load(CONTRACTS)["fixtures"]}:
        raise ValueError("OTLP fixture signals differ from the qualification manifest")
    contract("current")
    contract("previous")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window", choices=("current", "previous"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
            print("OTLP contract self-test passed")
        elif args.window:
            replay(args.window)
        else:
            raise ValueError("--window is required")
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError, tarfile.TarError) as error:
        print(f"otlp-contracts.py: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
