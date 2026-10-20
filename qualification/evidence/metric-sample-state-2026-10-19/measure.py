#!/usr/bin/env python3
"""Measure the ignored accumulator workload in a separate process for each case."""

import hashlib
import json
import os
import platform
from pathlib import Path
import re
import subprocess
import sys
import tempfile

DIRECTORY = Path(__file__).resolve().parent
ROOT = DIRECTORY.parents[2]
TEST = "querier::aggregate::metric_values::metric_sample_state::tests::profile_overlapping_windows"


def run(binary, operation, rows, windows, state, probe=None):
    environment = dict(os.environ)
    environment.update(
        METRIC_PROFILE_ROWS=str(rows),
        METRIC_PROFILE_WINDOWS=str(windows),
        METRIC_PROFILE_STATE=state,
        METRIC_PROFILE_OP=operation,
    )
    if probe:
        environment["LD_PRELOAD"] = str(probe)
    with tempfile.TemporaryFile() as output:
        child = os.fork()
        if child == 0:
            os.dup2(output.fileno(), 1)
            os.dup2(output.fileno(), 2)
            os.execve(binary, [binary, "--ignored", "--exact", TEST, "--nocapture"], environment)
        _, status, usage = os.wait4(child, 0)
        output.seek(0)
        log = output.read().decode()
    if status != 0 or "1 passed" not in log:
        raise RuntimeError(log)
    elapsed = re.search(r"elapsed_us=(\d+)", log)
    if elapsed is None:
        raise RuntimeError(log)
    result = {"elapsed_us": int(elapsed[1]), "peak_rss_kib": usage.ru_maxrss}
    if probe:
        result["allocation_calls"] = int(re.search(r"allocation_calls=(\d+)", log)[1])
        layout = re.search(r"state_bytes=(\d+) legacy_state_bytes=(\d+)", log)
        result["state_bytes"] = int(layout[1] if state == "selected" else layout[2])
    return result


def main():
    binary = str(Path(sys.argv[1]).resolve())
    result_path = Path(sys.argv[2])
    cases = [("count", rows, windows) for rows in (1000, 10000, 100000) for windows in (1, 8, 32)]
    cases += [(operation, 100000, 8) for operation in ("quantile", "counter")]
    results = []
    with tempfile.TemporaryDirectory() as temporary:
        probe = Path(temporary) / "allocations.so"
        subprocess.run(["cc", "-shared", "-fPIC", "-O2", str(DIRECTORY / "allocations.c"), "-o", str(probe)], check=True)
        for operation, rows, windows in cases:
            timings = {"legacy": [], "selected": []}
            for trial in range(3):
                order = ("legacy", "selected") if trial % 2 == 0 else ("selected", "legacy")
                for state in order:
                    timings[state].append(run(binary, operation, rows, windows, state))
            allocations = {state: run(binary, operation, rows, windows, state, probe) for state in timings}
            results.append({"operation": operation, "rows": rows, "windows": windows, "timings_without_probe": timings, "allocation_capture": allocations})
            print(f"Measured {operation}: rows={rows}, windows={windows}", flush=True)
    source = ROOT / "crates/observability/src/querier/aggregate/metric_values/metric_sample_state.rs"
    report = {
        "schema_version": 1,
        "issue": "https://github.com/krabka-io/krabka-o11y/issues/374",
        "baseline_commit": subprocess.check_output(["git", "rev-parse", "origin/main"], cwd=ROOT, text=True).strip(),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "compiler": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "build": "Cargo dev test profile, unoptimized; same binary for both states",
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "scope": "Accumulator creation, record, reduction and destruction. Every row contributes to every window. No Parquet, query planning or network IO.",
        "fixture": "Each sample has value one. Timestamps are distinct and increase from zero. Each process checks the complete reduction checksum.",
        "memory": "Whole-process peak RSS from wait4, in KiB. Allocation calls include malloc, calloc, realloc and aligned allocation calls, including test harness overhead.",
        "latency": "Three fresh processes per state, with alternating order. Latency captures have no allocation probe. Shared build host; these are diagnostic timings, not end-to-end speedup measurements.",
        "reproduce": "source /workspace/.cloud-setup/activate.sh; cargo test -p krabka-observability --lib --locked --no-run; python3 qualification/evidence/metric-sample-state-2026-10-19/measure.py <test-binary> qualification/metric-sample-state-2026-10-19.json",
        "measurements": results,
    }
    result_path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
