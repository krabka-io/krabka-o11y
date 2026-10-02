#!/usr/bin/env python3
"""Checks the evidence an object-store qualification run uploads.

Run it on the directory the `object-store contract` workflow fills:

    tools/object-store-evidence.py EVIDENCE_DIR --provider aws --commit SHA

The workflow runs the provider contract and one lifecycle test per signal
against a real provider. Each test writes a JSON report of what it checked
and what it cost. A test that is skipped, filtered out or renamed does not
fail: Bazel reports a pass with no report written. This script turns each such
silent gap into a named failure:

  * every file in the directory has a SHA256SUMS line, and every line matches
  * every report names the cloud and commit of the run
  * the contract report lists every required provider case
  * the contract listed more objects than one provider page holds
  * one lifecycle report per signal, each with its required steps
  * every report counts requests and bytes, and each total is the sum of its
    per-operation map

The request and byte counts are the M19 cost figures. The metering wrapper in
`krabka-blockstore` counts them per operation. See
`docs/object_store_contract.md`.

`--self-test` runs the checks against synthetic evidence and needs no
provider.
"""

import argparse
import hashlib
import json
import os
import pathlib
import sys
import tempfile

# The URL schemes of each cloud. The `provider` field of a report is the URL
# scheme. An `https://` URL can name an S3 or an Azure endpoint, so it does not
# identify a cloud. The `cloud` field does: the report writer records the
# client `object_store` picks for the URL, from the scheme or the `https://`
# host. The scheme sets are disjoint, and so are the cloud names.
PROVIDER_SCHEMES = {
    "aws": {"s3", "s3a"},
    "gcs": {"gs"},
    "azure": {"az", "adl", "azure", "abfs", "abfss"},
}

# The provider cases the contract must run. See `docs/object_store_contract.md`.
REQUIRED_CASES = (
    "conditional_writes",
    "multipart",
    "pagination",
    "stale_listing",
    "checksum_mismatch",
    "throttling",
    "deletion",
)

# One provider list page holds at most 1000 objects on S3, GCS and Azure.
PAGE_SIZE = 1000

# The operations a contract case cannot pass without.
CONTRACT_OPERATIONS = ("put", "put_multipart", "get", "list", "delete_stream")

ALL_STEPS = (
    "flush",
    "query",
    "compaction",
    "retention",
    "orphan_reconciliation",
    "restart",
)

# The logs lifecycle has no block merge and no orphan sweep. Its compaction
# turns WAL records into blocks, which the flush step covers.
REQUIRED_STEPS = {
    "metrics": ALL_STEPS,
    "logs": ("flush", "query", "retention", "restart"),
    "traces": ALL_STEPS,
    "profiles": ALL_STEPS,
}

SUMS = "SHA256SUMS"
CONTRACT = "object-store-contract.json"
LIFECYCLE_GLOB = "object-store-lifecycle-*.json"


def annotate(level, message):
    """Prints a message, as a GitHub annotation when the run is on Actions."""
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::{level}::{message}", flush=True)
    else:
        print(f"{level}: {message}", flush=True)


def check_sums(evidence):
    """Problems with SHA256SUMS, over every other file in `evidence`."""
    problems = []
    sums = evidence / SUMS
    if not sums.is_file():
        return [f"{SUMS} is missing"]
    listed = {}
    for number, line in enumerate(sums.read_text().splitlines(), 1):
        if not line.strip():
            continue
        digest, sep, name = line.partition("  ")
        if not sep or len(digest) != 64:
            problems.append(f"{SUMS} line {number} does not parse")
            continue
        listed[os.path.normpath(name)] = digest
    present = {
        os.path.normpath(path.relative_to(evidence).as_posix())
        for path in evidence.rglob("*")
        if path.is_file() and path.name != SUMS
    }
    for name in sorted(present - listed.keys()):
        problems.append(f"{name} has no {SUMS} line")
    for name, digest in sorted(listed.items()):
        path = evidence / name
        if not path.is_file():
            problems.append(f"{name} is in {SUMS} but missing")
        elif hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            problems.append(f"{name} does not match its {SUMS} line")
    return problems


def read_report(path):
    """The report at `path`, or a problem string when it does not parse."""
    try:
        report = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        return None, f"{path.name} does not parse: {error}"
    if not isinstance(report, dict):
        return None, f"{path.name} is not a JSON object"
    return report, None


def check_common(name, report, kind, provider, commit):
    """Problems with the fields every report carries."""
    problems = []
    if report.get("schema_version") != 1:
        problems.append(f"{name}: schema_version is not 1")
    if report.get("kind") != kind:
        problems.append(f"{name}: kind is {report.get('kind')!r}, not {kind!r}")
    if report.get("cloud") != provider:
        problems.append(f"{name}: cloud {report.get('cloud')!r} is not {provider!r}")
    if report.get("provider") not in PROVIDER_SCHEMES[provider] | {"https"}:
        problems.append(
            f"{name}: provider {report.get('provider')!r} is not a {provider} scheme"
        )
    if commit is not None and report.get("commit") != commit:
        problems.append(f"{name}: commit {report.get('commit')!r} is not {commit}")
    problems.extend(check_costs(name, report))
    return problems


def check_costs(name, report):
    """Problems with the request and byte counts of one report."""
    problems = []
    for total, per_operation in (
        ("requests_total", "operations"),
        ("transferred_bytes_total", "transferred_bytes"),
    ):
        value = report.get(total)
        counts = report.get(per_operation)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            problems.append(f"{name}: {total} is not a positive integer")
            continue
        if not isinstance(counts, dict) or not all(
            isinstance(count, int) and count >= 0 for count in counts.values()
        ):
            problems.append(f"{name}: {per_operation} is not a map of counts")
            continue
        if sum(counts.values()) != value:
            problems.append(
                f"{name}: {total} {value} is not the sum of {per_operation} "
                f"{sum(counts.values())}"
            )
    return problems


def check_contract(evidence, provider, commit):
    """Problems with the contract report."""
    path = evidence / CONTRACT
    if not path.is_file():
        return [f"{CONTRACT} is missing"]
    report, problem = read_report(path)
    if problem:
        return [problem]
    problems = check_common(CONTRACT, report, "contract", provider, commit)
    cases = report.get("cases")
    cases = set(cases) if isinstance(cases, list) else set()
    for case in REQUIRED_CASES:
        if case not in cases:
            problems.append(f"{CONTRACT}: case {case} did not run")
    objects = report.get("pagination_objects")
    if not isinstance(objects, int) or objects <= PAGE_SIZE:
        problems.append(
            f"{CONTRACT}: pagination_objects {objects!r} fits in one page"
        )
    operations = report.get("operations")
    if isinstance(operations, dict):
        for operation in CONTRACT_OPERATIONS:
            if not operations.get(operation):
                problems.append(f"{CONTRACT}: no {operation} request was counted")
    return problems


def check_lifecycles(evidence, provider, commit):
    """Problems with the lifecycle reports, one per signal."""
    problems = []
    by_signal = {}
    for path in sorted(evidence.glob(LIFECYCLE_GLOB)):
        report, problem = read_report(path)
        if problem:
            problems.append(problem)
            continue
        problems.extend(check_common(path.name, report, "lifecycle", provider, commit))
        signal = report.get("signal")
        if signal not in REQUIRED_STEPS:
            problems.append(f"{path.name}: signal {signal!r} is not known")
            continue
        by_signal.setdefault(signal, []).append((path.name, report))
    for signal, required in REQUIRED_STEPS.items():
        reports = by_signal.get(signal, [])
        if not reports:
            problems.append(f"no lifecycle report for {signal}")
            continue
        if len(reports) > 1:
            names = ", ".join(name for name, _ in reports)
            problems.append(f"{signal} has more than one lifecycle report: {names}")
        for name, report in reports:
            steps = report.get("steps")
            steps = set(steps) if isinstance(steps, list) else set()
            for step in required:
                if step not in steps:
                    problems.append(f"{name}: step {step} did not run")
            restarts = report.get("restarts")
            if not isinstance(restarts, int) or restarts < 1:
                problems.append(f"{name}: the test did not restart its client")
    return problems


def run(evidence, provider, commit):
    """Checks `evidence`, and returns the exit code."""
    evidence = pathlib.Path(evidence)
    if not evidence.is_dir():
        annotate("error", f"{evidence} is not a directory")
        return 1
    problems = (
        check_sums(evidence)
        + check_contract(evidence, provider, commit)
        + check_lifecycles(evidence, provider, commit)
    )
    for problem in problems:
        annotate("error", problem)
    if problems:
        print(f"{len(problems)} problems in {evidence}", flush=True)
        return 1
    for path in [evidence / CONTRACT, *sorted(evidence.glob(LIFECYCLE_GLOB))]:
        report = json.loads(path.read_text())
        print(
            f"  {path.name}: {report['requests_total']} requests, "
            f"{report['transferred_bytes_total']} bytes",
            flush=True,
        )
    print(f"evidence for {provider} is complete", flush=True)
    return 0


def contract_report(**overrides):
    """A contract report that passes every check."""
    operations = {
        "copy": 5,
        "delete_stream": 40,
        "get": 120,
        "list": 70,
        "list_with_delimiter": 14,
        "put": 1100,
        "put_multipart": 20,
        "write_block": 0,
    }
    transferred = dict.fromkeys(operations, 0)
    transferred.update(get=38_000_000, put=2_600, put_multipart=41_000_000)
    report = {
        "schema_version": 1,
        "kind": "contract",
        "commit": "abc123",
        "provider": "s3",
        "cloud": "aws",
        "bucket": "krabka-qualification",
        "endpoint_host": None,
        "duration_seconds": 12.5,
        "pagination_objects": 1005,
        "listing_convergence_attempts": 1,
        "deletion_convergence_attempts": 1,
        "cases": list(REQUIRED_CASES),
        "requests_total": sum(operations.values()),
        "transferred_bytes_total": sum(transferred.values()),
        "operations": operations,
        "transferred_bytes": transferred,
    }
    report.update(overrides)
    return report


def lifecycle_report(signal, **overrides):
    """A lifecycle report for `signal` that passes every check."""
    operations = {
        "copy": 0,
        "delete_stream": 2,
        "get": 14,
        "list": 4,
        "list_with_delimiter": 1,
        "put": 7,
        "put_multipart": 0,
        "write_block": 0,
    }
    transferred = dict.fromkeys(operations, 0)
    transferred.update(get=8_500, put=5_100)
    report = {
        "schema_version": 1,
        "kind": "lifecycle",
        "commit": "abc123",
        "provider": "s3",
        "cloud": "aws",
        "bucket": "krabka-qualification",
        "endpoint_host": None,
        "signal": signal,
        "test": "block_lifecycle",
        "prefix": f"krabka-contract/ci/lifecycle/{signal}/block_lifecycle/1-1",
        "duration_seconds": 0.5,
        "restarts": 1,
        "steps": list(REQUIRED_STEPS[signal]),
        "requests_total": sum(operations.values()),
        "transferred_bytes_total": sum(transferred.values()),
        "operations": operations,
        "transferred_bytes": transferred,
        "failures": dict.fromkeys(operations, 0),
    }
    report.update(overrides)
    return report


def clean_evidence():
    """File name to contents, for a run that passes every check."""
    files = {
        CONTRACT: json.dumps(contract_report()),
        "run.metadata.txt": "commit=abc123\nprovider=aws\n",
        "blockstore-object_store_provider_contract_test.test.log": "ok\n",
    }
    for signal in REQUIRED_STEPS:
        files[f"object-store-lifecycle-{signal}-block_lifecycle.json"] = json.dumps(
            lifecycle_report(signal)
        )
    return files


def write_evidence(directory, files, sums=True):
    """Writes `files` into `directory`, with a SHA256SUMS over them."""
    for name, contents in files.items():
        (directory / name).write_text(contents)
    if sums:
        lines = [
            f"{hashlib.sha256(contents.encode()).hexdigest()}  ./{name}"
            for name, contents in sorted(files.items())
        ]
        (directory / SUMS).write_text("\n".join(lines) + "\n")


def with_file(name, contents):
    """The clean evidence, with `name` set to `contents` or removed on `None`."""
    files = clean_evidence()
    if contents is None:
        files.pop(name)
    else:
        files[name] = contents
    return files


def every_report(**fields):
    """The clean evidence, with `fields` set in every JSON report."""
    return {
        name: json.dumps(dict(json.loads(body), **fields))
        if name.endswith(".json")
        else body
        for name, body in clean_evidence().items()
    }


def self_test():
    clean = clean_evidence()
    metrics = "object-store-lifecycle-metrics-block_lifecycle.json"
    logs = "object-store-lifecycle-logs-block_lifecycle.json"
    no_throttling = [case for case in REQUIRED_CASES if case != "throttling"]
    logs_steps = list(REQUIRED_STEPS["logs"])

    def tamper(files):
        files = dict(files)
        files["run.metadata.txt"] = "commit=abc123\nprovider=gcs\n"
        return files

    # Each case is (name, files, whether to write SHA256SUMS, a function that
    # edits the files after the sums, provider, expected exit code).
    cases = [
        ("a complete run", clean, True, None, "aws", 0),
        (
            "an https URL to an S3 host",
            every_report(provider="https", bucket="s3.us-east-1.amazonaws.com"),
            True,
            None,
            "aws",
            0,
        ),
        (
            "an Azure run",
            every_report(provider="az", cloud="azure"),
            True,
            None,
            "azure",
            0,
        ),
        (
            "an https URL to an Azure host",
            every_report(
                provider="https",
                cloud="azure",
                bucket="account.blob.core.windows.net",
            ),
            True,
            None,
            "azure",
            0,
        ),
        ("a run from another provider", clean, True, None, "gcs", 1),
        ("an AWS run checked as Azure", clean, True, None, "azure", 1),
        (
            "an AWS run that used an https URL to an Azure host",
            every_report(
                provider="https",
                cloud="azure",
                bucket="account.blob.core.windows.net",
            ),
            True,
            None,
            "aws",
            1,
        ),
        (
            "an Azure run that used an https URL to an S3 host",
            every_report(provider="https", bucket="s3.us-east-1.amazonaws.com"),
            True,
            None,
            "azure",
            1,
        ),
        (
            "an https URL to a host that is not a cloud store",
            every_report(provider="https", cloud="other", bucket="example.com"),
            True,
            None,
            "aws",
            1,
        ),
        (
            "an Azure scheme with an AWS cloud",
            every_report(provider="az"),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a report with no cloud",
            with_file(CONTRACT, json.dumps(contract_report(cloud=None))),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a lifecycle report from another cloud",
            with_file(
                "object-store-lifecycle-traces-block_lifecycle.json",
                json.dumps(
                    lifecycle_report(
                        "traces",
                        provider="https",
                        cloud="azure",
                        bucket="account.blob.core.windows.net",
                    )
                ),
            ),
            True,
            None,
            "aws",
            1,
        ),
        ("no SHA256SUMS", clean, False, None, "aws", 1),
        ("a file changed after it was hashed", clean, True, tamper, "aws", 1),
        (
            "a file with no SHA256SUMS line",
            clean,
            True,
            lambda files: dict(files, **{"stray.json": "{}"}),
            "aws",
            1,
        ),
        (
            "a file in SHA256SUMS that is missing",
            clean,
            True,
            lambda files: {k: v for k, v in files.items() if k != "run.metadata.txt"},
            "aws",
            1,
        ),
        ("no contract report", with_file(CONTRACT, None), True, None, "aws", 1),
        (
            "a contract report that is not JSON",
            with_file(CONTRACT, "<html>a proxy error</html>"),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a contract case that did not run",
            with_file(CONTRACT, json.dumps(contract_report(cases=no_throttling))),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a listing that fit in one page",
            with_file(CONTRACT, json.dumps(contract_report(pagination_objects=1000))),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a contract from another commit",
            with_file(CONTRACT, json.dumps(contract_report(commit="def456"))),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a contract that moved no bytes",
            with_file(
                CONTRACT,
                json.dumps(
                    contract_report(
                        transferred_bytes_total=0,
                        transferred_bytes=dict.fromkeys(CONTRACT_OPERATIONS, 0),
                    )
                ),
            ),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a request total that is not the sum of its operations",
            with_file(CONTRACT, json.dumps(contract_report(requests_total=1))),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a contract with no multipart request",
            with_file(
                CONTRACT,
                json.dumps(
                    contract_report(
                        requests_total=contract_report()["requests_total"] - 20,
                        operations=dict(
                            contract_report()["operations"], put_multipart=0
                        ),
                    )
                ),
            ),
            True,
            None,
            "aws",
            1,
        ),
        ("a signal with no lifecycle report", with_file(metrics, None), True, None, "aws", 1),
        (
            "a lifecycle step that did not run",
            with_file(
                metrics,
                json.dumps(
                    lifecycle_report("metrics", steps=["flush", "query", "restart"])
                ),
            ),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a lifecycle report with no restart",
            with_file(logs, json.dumps(lifecycle_report("logs", restarts=0))),
            True,
            None,
            "aws",
            1,
        ),
        (
            "a lifecycle report with no requests",
            with_file(
                logs,
                json.dumps(
                    lifecycle_report(
                        "logs",
                        requests_total=0,
                        operations=dict.fromkeys(CONTRACT_OPERATIONS, 0),
                    )
                ),
            ),
            True,
            None,
            "aws",
            1,
        ),
        (
            "logs need no compaction or orphan step",
            with_file(logs, json.dumps(lifecycle_report("logs", steps=logs_steps))),
            True,
            None,
            "aws",
            0,
        ),
        (
            "a signal reported twice",
            dict(
                clean,
                **{
                    "object-store-lifecycle-metrics-other.json": json.dumps(
                        lifecycle_report("metrics", test="other")
                    )
                },
            ),
            True,
            None,
            "aws",
            1,
        ),
    ]

    failures = 0
    for name, files, sums, after, provider, expected in cases:
        with tempfile.TemporaryDirectory() as scratch:
            evidence = pathlib.Path(scratch)
            write_evidence(evidence, files, sums)
            if after is not None:
                edited = after(files)
                for path in evidence.iterdir():
                    if path.name != SUMS and path.name not in edited:
                        path.unlink()
                for file_name, contents in edited.items():
                    (evidence / file_name).write_text(contents)
            got = run(evidence, provider, "abc123")
        verdict = "ok" if got == expected else "FAILED"
        if got != expected:
            failures += 1
        print(f"  {verdict:<8} exit {got}, expected {expected}: {name}", flush=True)

    print(f"{len(cases)} cases, {failures} failed", flush=True)
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "evidence", nargs="?",
        help="the directory the workflow collected the reports into",
    )
    parser.add_argument(
        "--provider", choices=sorted(PROVIDER_SCHEMES),
        help="the provider the run qualified",
    )
    parser.add_argument(
        "--commit",
        help="the commit every report must name; unchecked when omitted",
    )
    parser.add_argument(
        "--self-test", action="store_true",
        help="run the checks against synthetic evidence",
    )
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.evidence is None or args.provider is None:
        parser.error("an evidence directory and --provider are required")
    return run(args.evidence, args.provider, args.commit)


if __name__ == "__main__":
    sys.exit(main())
