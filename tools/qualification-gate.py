#!/usr/bin/env python3
"""Run one qualification command and record immutable gate evidence."""

import argparse
import datetime
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

COMMIT = re.compile(r"^[0-9a-f]{40}$")


def checksum(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def line_counts(line):
    flaky = len(re.findall(r"\bFLAKY\b", line))
    skipped = len(re.findall(r"\bSKIPPED\b", line))
    result_count = 0
    ignored = 0
    if match := re.search(r"Executed \d+ out of \d+ tests?: (\d+) tests? pass", line):
        result_count = int(match.group(1))
    elif match := re.search(r"test result: ok\. (\d+) passed; \d+ failed; (\d+) ignored;", line):
        result_count = int(match.group(1))
        ignored = int(match.group(2))
    return flaky, skipped, result_count, ignored


def run(gate_id, category, command, output, allowed_ignored):
    commit = os.environ.get("GITHUB_SHA", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not COMMIT.fullmatch(commit) or not run_id.isdigit() or not repository:
        raise ValueError("GITHUB_SHA, GITHUB_RUN_ID, and GITHUB_REPOSITORY must identify this run")
    started = datetime.datetime.now(datetime.UTC)
    before = time.monotonic()
    process = subprocess.Popen(
        command,
        executable="/bin/bash",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    flaky = 0
    skipped = 0
    ignored = 0
    result_count = 0
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
        line_flaky, line_skipped, line_results, line_ignored = line_counts(line)
        flaky += line_flaky
        skipped += line_skipped
        result_count += line_results
        ignored += line_ignored
    returncode = process.wait()
    if returncode == 0 and ignored != allowed_ignored:
        print(f"expected {allowed_ignored} ignored tests, observed {ignored}", file=sys.stderr)
        returncode = 1
    finished = datetime.datetime.now(datetime.UTC)
    evidence = {
        "schema_version": 1,
        "id": gate_id,
        "gate": category,
        "command": command,
        "commit": commit,
        "run_url": f"https://github.com/{repository}/actions/runs/{run_id}",
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_seconds": round(time.monotonic() - before, 3),
        "result": "passed" if returncode == 0 else "failed",
        "exit_code": returncode,
        "result_count": result_count or 1,
        "flaky_count": flaky,
        "skipped_count": skipped + ignored,
        "allowed_skipped_count": allowed_ignored,
    }
    evidence["checksum"] = checksum(evidence)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    return returncode


def self_test():
    sample = {"id": "ordinary", "result": "passed"}
    if checksum(sample) != checksum(dict(reversed(list(sample.items())))):
        raise ValueError("evidence checksum depends on key order")
    if line_counts("test result: ok. 7 passed; 0 failed; 3 ignored; 0 measured;") != (0, 0, 7, 3):
        raise ValueError("libtest ignored tests were not counted")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id")
    parser.add_argument("--gate")
    parser.add_argument("--command")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--allowed-ignored", type=int, default=0)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
            print("qualification gate self-test passed")
            return
        if not all((args.id, args.gate, args.command, args.output)):
            raise ValueError("--id, --gate, --command, and --output are required")
        raise SystemExit(run(args.id, args.gate, args.command, args.output, args.allowed_ignored))
    except (OSError, ValueError) as error:
        print(f"qualification-gate.py: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
