#!/usr/bin/env python3
"""Measure cold profile scans in separate processes, with the same Parquet file."""

import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
TEST_PREFIX = "cold_store::parquet_scan::tests::"


def run(binary, fixture, mode, warm):
    environment = dict(os.environ)
    environment.update(
        PROFILE_SCAN_FIXTURE=str(fixture),
        PROFILE_SCAN_MODE=mode,
        PROFILE_SCAN_WARM=str(int(warm)),
    )
    with tempfile.TemporaryFile() as output:
        child = os.fork()
        if child == 0:
            os.dup2(output.fileno(), 1)
            os.dup2(output.fileno(), 2)
            os.execve(binary, [binary, "--ignored", "--exact", TEST_PREFIX + "scan_resource_usage", "--nocapture"], environment)
        _, status, usage = os.wait4(child, 0)
        output.seek(0)
        log = output.read().decode()
    if status != 0 or "1 passed" not in log:
        raise RuntimeError(log)
    if mode == "fixture":
        return None
    elapsed = re.search(r"elapsed_us=(\d+)", log)
    if elapsed is None:
        raise RuntimeError(log)
    return {"elapsed_us": int(elapsed[1]), "peak_rss_kib": usage.ru_maxrss}


def main():
    binary = str(Path(sys.argv[1]).resolve())
    output_path = Path(sys.argv[2])
    io_log = subprocess.check_output([binary, "--exact", TEST_PREFIX + "selective_range_reads_prune_groups_and_project_metadata", "--nocapture"], stderr=subprocess.STDOUT, text=True)
    io = []
    for match in re.finditer(r"metadata=(\w+) warm=(\w+) bytes=(\d+)/(\d+) predicate_rows=(\d+)/(\d+) output_rows=(\d+)", io_log):
        metadata, warm, fetched, total, decoded, all_rows, selected = match.groups()
        io.append({"metadata_only": metadata == "true", "warm_metadata": warm == "true", "fetched_bytes": int(fetched), "file_bytes": int(total), "predicate_rows": int(decoded), "file_rows": int(all_rows), "output_rows": int(selected)})
    if len(io) != 4 or "1 passed" not in io_log:
        raise RuntimeError(io_log)
    measurements = []
    with tempfile.TemporaryDirectory() as temporary:
        fixture = Path(temporary) / "profiles.parquet"
        run(binary, fixture, "fixture", False)
        for trial in range(3):
            modes = ["full", "range", "full-metadata", "metadata"]
            if trial % 2:
                modes.reverse()
            for warm in [False, True]:
                for mode in modes:
                    result = run(binary, fixture, mode, warm)
                    measurements.append({"trial": trial, "mode": mode, "warm_metadata": warm, **result})
    source = ROOT / "crates/profiles/src/cold_store/parquet_scan.rs"
    report = {
        "schema_version": 1,
        "issue": "https://github.com/krabka-io/krabka-o11y/issues/378",
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "compiler": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "platform": platform.platform(),
        "build": "Cargo dev test profile, unoptimized; one binary for all modes.",
        "fixture": "32768 sample rows in 16 row groups. One selected series, inclusive [80000, 80002] milliseconds. Alternating profile types; binary trace and WAL identities; high-bit compacted partitions.",
        "io": "Bytes returned by the production object-store range reader, including the footer when metadata is cold. Measured through AsyncFileReader. Predicate rows count rows decoded for the filter; output rows count selected payload rows.",
        "scope": "Local file store, with warm OS page cache. Cold metadata parses the footer during the query. Warm metadata supplies a preloaded footer; the production reader does not add a footer cache. No network latency, symbol loading, or end-to-end API measurements.",
        "memory": "Whole-process peak RSS from wait4, in KiB. Fixture generation runs in a separate process.",
        "latency": "Three fresh processes per mode and metadata state, alternating mode order. Shared host; diagnostic timings only. The full and full-metadata modes reproduce the previous full-download/full-width decode/filter paths.",
        "reproduce": "cargo test -p krabka-profiles --lib --locked --no-run; python3 qualification/evidence/profile-cold-parquet-378/measure.py <test-binary> qualification/profile-cold-parquet-378.json",
        "range_reads": io,
        "measurements": measurements,
    }
    output_path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
