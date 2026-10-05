#!/usr/bin/env python3
"""Bind query evidence to one checkout; --strict forbids gaps or missing suites."""

import argparse
import collections
import hashlib
import json
import pathlib
import re
import runpy
import tempfile
import subprocess


ROOT = pathlib.Path(__file__).resolve().parent.parent
REQUIRED = {
    "promql-3.14.0-qualification.json",
    "promql-http-compliance-summary.json",
    "loki-remote-range.json",
    "loki-remote-instant.json",
    "tempo-forest-conformance.json",
    "pyroscope-populated-rpcs.json",
    *{f"tempo-metric-result-{ordinal}.json" for ordinal in range(4)},
    "metrics-v1-query-transitions.json", "metrics-v2-query-transitions.json",
    "tempo-structural-query-transitions.json", "tempo-tenant-query-transitions.json",
    "diff_mimir-report.json", "diff_prometheus-report.json", "backup-query-invariance.json",
    *{f"{language}-generated-differential.json" for language in ("promql", "logql", "traceql", "pyroscope", "promql-rejections", "logql-rejections", "traceql-rejections", "pyroscope-rejections")},
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def case_verdict(case):
    status = case.get("status", "uncovered")
    return case.get("classification", status) if status in ("matched", "pass", "passed", "confirmed-unimplemented") else status


def verdicts(report):
    if isinstance(report, list):
        return collections.Counter(case.get("status", "uncovered") for case in report)
    if "compared_payloads" in report:
        return collections.Counter(matched=report["compared_payloads"] + report["paired_expected_errors"],
                                   skipped=report["skipped_comparisons"], mismatch=report["mismatches"])
    if "files" in report:
        return collections.Counter(case["status"] for file in report["files"] for case in file["cases"])
    if "passed_count" in report:
        counts = {name: report[name + "_count"] for name in ("passed", "failed", "skipped", "not_run")}
        if sum(counts.values()) != report["planned"]:
            raise ValueError("Loki report denominator does not balance")
        return collections.Counter(counts)
    if isinstance(report.get("cases"), list):
        counts = collections.Counter(case_verdict(case) for case in report["cases"])
        if "planned" in report and sum(counts.values()) != report["planned"]:
            if report.get("suite") == "pyroscope-populated-rpcs" and sum(counts.values()) < report["planned"]:
                counts["not_run"] += report["planned"] - sum(counts.values())
            else:
                raise ValueError("generated report denominator does not balance")
        return counts
    # Preserve unknown report shapes as uncovered; no inferred passing coverage.
    return collections.Counter(uncovered=1)


PINNED_COUNTS = {
    "promql-3.14.0-qualification.json": 2195,
    "promql-http-compliance-summary.json": 539,
    "loki-remote-range.json": 104,
    "loki-remote-instant.json": 81,
    "tempo-forest-conformance.json": 8,
    "pyroscope-populated-rpcs.json": 89,
    "metrics-v1-query-transitions.json": 26,
    "metrics-v2-query-transitions.json": 26,
    "tempo-structural-query-transitions.json": 46,
    "tempo-tenant-query-transitions.json": 51,
}


def qualification_counts(name, report, generated_count=None):
    errors = []
    try:
        counts = verdicts(report)
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        return collections.Counter(uncovered=1), [str(error)]
    if any(type(count) is not int or count < 0 for count in counts.values()):
        return collections.Counter(uncovered=1), ["case counts must be nonnegative integers"]
    expected = PINNED_COUNTS.get(name)
    if name.endswith("-generated-differential.json"):
        if generated_count is None:
            errors.append("missing or invalid generated_cases in run-profile.txt")
        else:
            expected = generated_count
            if type(report.get("planned")) is not int or report["planned"] != generated_count:
                errors.append("generated planned count differs from run profile")
    if name in ("loki-remote-range.json", "loki-remote-instant.json"):
        mode = "range" if name == "loki-remote-range.json" else "instant"
        if report.get("mode") != mode or type(report.get("planned")) is not int or report["planned"] != expected:
            errors.append("Loki mode or planned count differs from pinned corpus")
        names = []
        for status in ("passed", "failed", "skipped", "not_run"):
            entries = report.get(status)
            if (not isinstance(entries, list)
                    or any(not isinstance(entry, str) or not entry for entry in entries)
                    or len(entries) != report.get(status + "_count")):
                errors.append(f"Loki {status} names do not match count")
            else:
                names.extend(entries)
        if len(names) != len(set(names)):
            errors.append("Loki case names are duplicated across verdicts")
        if report.get("success") is not True and is_complete(counts):
            errors.append("Loki runner did not declare successful execution")
    if expected is not None:
        total = sum(counts.values())
        if total > expected:
            errors.append("pinned denominator changed")
        elif total < expected:
            counts["uncovered"] += expected - total
    return counts, errors


def generated_case_count(directory):
    if directory is None:
        return None
    try:
        values = [line.partition("=")[2] for line in
                  (directory / "run-profile.txt").read_text().splitlines()
                  if line.startswith("generated_cases=")]
        return int(values[0]) if len(values) == 1 and values[0].isdigit() and int(values[0]) > 0 else None
    except (OSError, UnicodeError, ValueError):
        return None


def verify_manifest(directory):
    manifest = directory / "SHA256SUMS"
    try:
        lines = manifest.read_text().splitlines()
    except (OSError, UnicodeError) as error:
        return [f"missing or unreadable SHA256SUMS: {error}"]
    errors = []
    listed = set()
    root = directory.resolve()
    for line in lines:
        match = re.fullmatch(r"([0-9a-fA-F]{64}) [ *](.+)", line)
        if match is None:
            errors.append("malformed SHA256SUMS entry")
            continue
        path = (directory / match[2]).resolve()
        if not path.is_relative_to(root) or path == manifest.resolve():
            errors.append(f"invalid manifest path: {match[2]}")
            continue
        if path in listed:
            errors.append(f"duplicate manifest path: {match[2]}")
        listed.add(path)
        try:
            if digest(path) != match[1].lower():
                errors.append(f"checksum mismatch: {match[2]}")
        except OSError as error:
            errors.append(f"unreadable manifest file {match[2]}: {error}")
    required = {path.resolve() for path in directory.rglob("*")
                if path.is_file() and path != manifest}
    required.update((directory / name).resolve() for name in ("commit", "run-result"))
    for path in sorted(required - listed):
        errors.append(f"file omitted from SHA256SUMS: {path}")
    if not any(path.is_relative_to(root / "raw") for path in listed):
        errors.append("SHA256SUMS contains no raw evidence")
    return errors


def verify_runner_binding(directory, name, pin):
    if directory is None:
        return ["missing runner binding directory"]
    provenance_path = directory / f"{name}-runner-provenance.json"
    invocation_path = directory / "query-runner-invocation.json"
    try:
        provenance = json.loads(provenance_path.read_text())
        invocation = json.loads(invocation_path.read_text())
    except (OSError, ValueError) as error:
        return [f"missing or invalid runner provenance/invocation: {error}"]
    if not isinstance(provenance, dict) or not isinstance(invocation, dict):
        return ["runner provenance/invocation must be JSON objects"]
    errors = []
    if (provenance.get("repository"), provenance.get("revision"), provenance.get("toolchain")) != pin:
        errors.append("runner repository/revision/toolchain differs from pin")
    if provenance.get("builder_sha256") != digest(ROOT / "tools/build-query-runners.py"):
        errors.append("runner builder differs from current checkout")
    if type(invocation.get("exit_code")) is not int or invocation["exit_code"] != 0:
        errors.append("runner invocation did not exit successfully")
    if invocation.get("binaries_unchanged") is not True:
        errors.append("runner executable changed during invocation")
    runners = invocation.get("runners", {})
    runner = runners.get(name, {}) if isinstance(runners, dict) else {}
    if not isinstance(runner, dict):
        runner = {}
    binary_hash = provenance.get("binary_sha256", "")
    if (not isinstance(binary_hash, str) or re.fullmatch(r"[0-9a-f]{64}", binary_hash) is None
            or runner.get("binary_sha256") != binary_hash
            or runner.get("provenance_sha256") != digest(provenance_path)):
        errors.append("invoked runner does not match provenance hashes")
    command = invocation.get("command", [])
    target = "//crates/metrics-service:diff_prometheus_docker_test" if name == "promql" else "//crates/observability:loki_differential_docker_test"
    flag = "KRABKA_PROMQL_COMPLIANCE_BIN" if name == "promql" else "KRABKA_LOKI_CORRECTNESS_BIN"
    binary_flags = [argument for argument in command if isinstance(argument, str) and argument.startswith(f"--test_env={flag}=")] if isinstance(command, list) else []
    if (not isinstance(command, list) or any(not isinstance(argument, str) for argument in command) or command[:2] != ["bazel", "test"] or target not in command
            or not isinstance(runner.get("binary"), str) or not binary_flags
            or binary_flags[-1] != f"--test_env={flag}={runner.get('binary')}"):
        errors.append("invocation command does not bind suite and runner executable")
    return errors


def is_complete(counts):
    if any(type(count) is not int or count < 0 for count in counts.values()):
        raise ValueError("case counts must be nonnegative integers")
    return sum(counts.values()) > 0 and all(status in ("matched", "pass", "passed") or count == 0 for status, count in counts.items())


def usable_evidence(reference, kind, executed, rejected_cases):
    return executed.get(reference) in ("matched", "pass", "passed") and (kind == "negative" or reference not in rejected_cases)


def self_check():
    for status in ("failed", "transport_error", "not_run", "uncovered"):
        failed = {"cases": [{"classification": "matched", "status": status}]}
        assert verdicts(failed) == {status: 1} and not is_complete(verdicts(failed))
    assert case_verdict({"classification": "matched"}) == "uncovered"
    assert case_verdict({"classification": "expected-divergence", "status": "passed"}) == "expected-divergence"
    assert case_verdict({"classification": "unsupported-oracle", "status": "confirmed-unimplemented"}) == "unsupported-oracle"
    assert usable_evidence("bad-query", "negative", {"bad-query": "pass"}, {"bad-query"})
    assert not usable_evidence("bad-query", "positive", {"bad-query": "pass"}, {"bad-query"})
    assert not usable_evidence("bad-query", "composition", {"bad-query": "pass"}, {"bad-query"})
    good = verdicts({"planned": 2, "cases": [{"status": "pass"}, {"status": "pass"}]})
    assert is_complete(good)
    for status in ("expected-divergence", "unsupported-oracle", "not_run", "uncovered", "mismatch"):
        assert not is_complete(collections.Counter(pass_=0, **{status: 1}))
    try:
        verdicts({"planned": 2, "cases": [{"status": "pass"}]})
    except ValueError:
        pass
    else:
        raise AssertionError("truncated denominator accepted")
    assert not is_complete(collections.Counter())
    assert not is_complete(collections.Counter(matched=0, mismatch=0))
    partial = verdicts({"suite":"pyroscope-populated-rpcs", "planned":2, "cases":[{"status":"matched"}]})
    assert partial == {"matched":1, "not_run":1} and not is_complete(partial)
    counts, errors = qualification_counts("pyroscope-populated-rpcs.json", {
        "suite": "pyroscope-populated-rpcs", "planned": 1, "cases": [{"status": "matched"}]})
    assert counts == {"matched": 1, "uncovered": 88} and not is_complete(counts)
    for name, mode, total in [("loki-remote-range.json", "range", 104), ("loki-remote-instant.json", "instant", 81)]:
        loki = {"mode": mode, "planned": total, "success": True,
                "passed_count": total, "failed_count": 0, "skipped_count": 0, "not_run_count": 0,
                "passed": [f"case-{number}" for number in range(total)],
                "failed": [], "skipped": [], "not_run": []}
        counts, errors = qualification_counts(name, loki)
        assert is_complete(counts) and not errors
        for field, value in [("planned", 1), ("passed", ["case-0"]),
                             ("passed", ["case-0"] * total), ("success", False), ("mode", "wrong")]:
            counts, errors = qualification_counts(name, dict(loki, **{field: value}))
            assert errors or not is_complete(counts)
        short = dict(loki, planned=1, passed_count=1, passed=["case-0"])
        counts, errors = qualification_counts(name, short)
        assert errors and counts["uncovered"] == total - 1 and not is_complete(counts)
    prom = {"compared_payloads": 539, "paired_expected_errors": 0, "skipped_comparisons": 0, "mismatches": 0}
    counts, errors = qualification_counts("promql-http-compliance-summary.json", prom)
    assert is_complete(counts) and not errors
    for total in (1, 540):
        counts, errors = qualification_counts("promql-http-compliance-summary.json", dict(prom, compared_payloads=total))
        assert errors or not is_complete(counts)
    generated = {"planned": 2, "cases": [{"status": "pass"}, {"status": "pass"}]}
    counts, errors = qualification_counts("traceql-generated-differential.json", generated, 2)
    assert is_complete(counts) and not errors
    for planned in (None, 1, 512):
        counts, errors = qualification_counts("traceql-generated-differential.json", generated, planned)
        assert errors and (planned != 512 or counts["uncovered"] == 510)

    with tempfile.TemporaryDirectory(prefix="query-evidence-self-check-") as temporary:
        directory = pathlib.Path(temporary)
        (directory / "raw").mkdir()
        for name, body in [("commit", "commit-sha"), ("run-result", "success"),
                           ("run-profile.txt", "generated_cases=512\n"), ("raw/report.json", "{}")]:
            (directory / name).write_text(body)
        files = sorted(path for path in directory.rglob("*") if path.is_file())
        manifest = "".join(f"{digest(path)}  ./{path.relative_to(directory)}\n" for path in files)
        (directory / "SHA256SUMS").write_text(manifest)
        assert not verify_manifest(directory)
        assert generated_case_count(directory) == 512
        for name in ("commit", "run-result", "raw/report.json"):
            path = directory / name
            original = path.read_text()
            path.write_text("altered")
            assert any("checksum mismatch" in error for error in verify_manifest(directory))
            path.write_text(original)
        (directory / "SHA256SUMS").write_text("\n".join(line for line in manifest.splitlines() if not line.endswith("./commit")) + "\n")
        assert any("omitted" in error for error in verify_manifest(directory))
        (directory / "SHA256SUMS").unlink()
        assert verify_manifest(directory)
        (directory / "run-profile.txt").unlink()
        assert generated_case_count(directory) is None
        (directory / "run-profile.txt").write_text("generated_cases=2\ngenerated_cases=512\n")
        assert generated_case_count(directory) is None

        pin = ("prometheus/compliance", "pinned-revision", "go1.25.0")
        provenance = {"repository": pin[0], "revision": pin[1], "toolchain": pin[2],
                      "builder_sha256": digest(ROOT / "tools/build-query-runners.py"), "binary_sha256": "a" * 64}
        provenance_path = directory / "promql-runner-provenance.json"
        provenance_path.write_text(json.dumps(provenance))
        invocation = {"exit_code": 0, "binaries_unchanged": True,
                      "command": ["bazel", "test", "//crates/metrics-service:diff_prometheus_docker_test",
                                  "--test_env=KRABKA_PROMQL_COMPLIANCE_BIN=/runner"],
                      "runners": {"promql": {"binary": "/runner", "binary_sha256": "a" * 64,
                                               "provenance_sha256": digest(provenance_path)}}}
        invocation_path = directory / "query-runner-invocation.json"
        invocation_path.write_text(json.dumps(invocation))
        assert not verify_runner_binding(directory, "promql", pin)
        for field, value in [("revision", "other"), ("toolchain", "other"), ("builder_sha256", "b" * 64)]:
            provenance_path.write_text(json.dumps(dict(provenance, **{field: value})))
            assert verify_runner_binding(directory, "promql", pin)
        provenance_path.write_text(json.dumps(provenance))
        for field, value in [("exit_code", 1), ("binaries_unchanged", False), ("command", ["bazel", "test"])]:
            invocation_path.write_text(json.dumps(dict(invocation, **{field: value})))
            assert verify_runner_binding(directory, "promql", pin)
        invocation_path.write_text(json.dumps(dict(invocation, command=invocation["command"] + ["--test_env=KRABKA_PROMQL_COMPLIANCE_BIN=/other"])))
        assert verify_runner_binding(directory, "promql", pin)
        for field in ("binary_sha256", "provenance_sha256", "binary"):
            changed = json.loads(json.dumps(invocation))
            changed["runners"]["promql"][field] = "wrong"
            invocation_path.write_text(json.dumps(changed))
            assert verify_runner_binding(directory, "promql", pin)
        invocation_path.unlink()
        assert verify_runner_binding(directory, "promql", pin)
    print("query evidence negative controls passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=pathlib.Path, action="append", default=[])
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "qualification/evidence/query-languages.json")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    subprocess.run(["python3", str(ROOT / "tools/query-language-inventory.py"), "--check"], check=True)
    inventory_path = ROOT / "qualification/query-language-inventory.json"
    inventory = json.loads(inventory_path.read_text())
    artifacts = []
    executed = {}
    rejected_cases = set()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    unbound = []
    unsuccessful_runs = []
    integrity_errors = {}
    invalid_reports = []
    runner_errors = {}
    runner_pins = runpy.run_path(str(ROOT / "tools/build-query-runners.py"))["RUNNERS"]
    if runner_pins["logql"][1] != inventory["surfaces"]["logql"]["pin"]["revision"]:
        runner_errors["logql pin"] = ["builder revision differs from inventory"]
    for directory in args.evidence:
        for binding in sorted(directory.rglob("commit")):
            if binding.is_file():
                integrity_errors[str(binding.parent)] = verify_manifest(binding.parent)
        for path in sorted(directory.rglob("*.json")):
            metric_result = path.name.startswith("tempo-metric-result-")
            if path.name not in REQUIRED and not metric_result:
                continue
            binding = next((parent / "commit" for parent in path.parents if (parent / "commit").is_file()), None)
            if binding is None or binding.read_text().strip() != commit:
                unbound.append(str(path))
            result = binding.parent / "run-result" if binding is not None else None
            if result is None or not result.is_file() or result.read_text().strip() != "success":
                unsuccessful_runs.append(str(path))
            binding_directory = binding.parent if binding is not None else None
            integrity_key = str(binding_directory) if binding_directory is not None else str(path)
            if integrity_key not in integrity_errors:
                integrity_errors[integrity_key] = verify_manifest(binding_directory) if binding_directory is not None else ["missing artifact commit binding"]
            runner = {"promql-http-compliance-summary.json": "promql", "loki-remote-range.json": "logql", "loki-remote-instant.json": "logql"}.get(path.name)
            if runner is not None:
                runner_key = f"{integrity_key}/{runner}"
                if runner_key not in runner_errors:
                    runner_errors[runner_key] = verify_runner_binding(binding_directory, runner, runner_pins[runner])
            try:
                report = json.loads(path.read_text())
                counts, errors = qualification_counts(path.name, report, generated_case_count(binding_directory))
            except (OSError, ValueError) as error:
                report = {}
                counts, errors = collections.Counter(uncovered=1), [f"unreadable or invalid report: {error}"]
            if errors:
                invalid_reports.append({"path": str(path), "errors": errors})
            if errors:
                pass
            elif path.name == "promql-3.14.0-qualification.json":
                if report.get("upstream_revision") != inventory["surfaces"]["promql"]["pin"]["revision"]:
                    raise ValueError("PromQL report pin differs from inventory")
                for file in report["files"]:
                    for case in file["cases"]:
                        reference = f"promql/{file['name']}/{case['ordinal']}"
                        executed[reference] = case["status"]
                        if case.get("expected_rejection"):
                            rejected_cases.add(reference)
            elif path.name.endswith("-generated-differential.json"):
                for ordinal, case in enumerate(report["cases"], 1):
                    reference = f"{report['language']}/generated/{ordinal}"
                    executed[reference] = case["status"]
                    if report.get("expected_outcome") == "query-rejection" or report["language"].endswith("-rejections"):
                        rejected_cases.add(reference)
            elif isinstance(report, list):
                for ordinal, case in enumerate(report, 1):
                    executed[f"traceql/forest/{ordinal}"] = case.get("status", "uncovered")
            elif "passed_count" in report:
                for status in ("passed", "failed", "skipped", "not_run"):
                    for name in report[status]:
                        executed[f"logql/{report['mode']}/{name}"] = status
            elif "cases" in report:
                namespace = f"storage/transitions/{path.stem}" if path.name.endswith("-query-transitions.json") else f"traceql/metrics/{path.stem}" if metric_result else {"diff_mimir-report.json":"promql/mimir", "diff_prometheus-report.json":"promql/prometheus", "backup-query-invariance.json":"storage/restore"}.get(path.name, "pyroscope/rpc")
                for ordinal, case in enumerate(report["cases"], 1):
                    reference = f"{namespace}/{ordinal}"
                    executed[reference] = case_verdict(case)
                    if case.get("expected_rejection"):
                        rejected_cases.add(reference)
            artifacts.append({"path": str(path), "name": path.name, "sha256": digest(path),
                              "counts": counts, "discovered": sum(counts.values()),
                              "validation_errors": errors,
                              "complete_corpus": not errors and is_complete(counts)})
    present = {artifact["name"] for artifact in artifacts}
    missing = sorted(REQUIRED - present)
    features = {language: collections.Counter(feature["status"] for feature in surface["features"])
                for language, surface in inventory["surfaces"].items()}
    unresolved_evidence = sorted({reference for surface in inventory["surfaces"].values()
                                  for feature in surface["features"] for kind, references in feature["evidence"].items()
                                  for reference in references if not usable_evidence(reference, kind, executed, rejected_cases)})
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT))
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard"], cwd=ROOT, text=True).splitlines()
    dirty |= any(path.startswith(("crates/", "tools/", "bazel/", ".github/", "Cargo", "qualification/query-language")) for path in untracked)
    report = {
        "schema_version": 1,
        "commit": commit, "unbound_artifacts": unbound, "unsuccessful_runs": unsuccessful_runs,
        "dirty_checkout": dirty,
        "integrity_errors": {path: errors for path, errors in integrity_errors.items() if errors},
        "runner_binding_errors": {path: errors for path, errors in runner_errors.items() if errors},
        "invalid_reports": invalid_reports,
        "inventory_sha256": digest(inventory_path),
        "oracle_manifest_sha256": digest(ROOT / "bazel/images/images.bzl"),
        "fixture_sources": {str(path.relative_to(ROOT)): digest(path) for pattern in (
            "crates/promql/tests/testdata/upstream-3.14.0/*.test",
            "crates/*/tests/support/*fixture.rs", "crates/*/tests/*differential.rs",
            "crates/metrics-service/tests/diff_*.rs", "crates/*/tests/*deployment.rs",
            "crates/integration/tests/backup_restore.rs",
            "crates/traceql/tests/testdata/traceql/*.case",
        ) for path in sorted(ROOT.glob(pattern))},
        "features": features, "missing_suites": missing, "artifacts": artifacts,
        "unresolved_evidence": unresolved_evidence, "executed_case_ids": executed,
        "complete_versioned_conformance": not dirty and not missing and not unbound and not unsuccessful_runs
            and not any(integrity_errors.values()) and not any(runner_errors.values())
            and not invalid_reports and all(artifact["complete_corpus"] for artifact in artifacts)
            and all(set(counts) <= {"mapped"} for counts in features.values())
            and not unresolved_evidence,
    }
    # This tool binds local evidence only. Remote CI must produce its own artifact
    # on the final immutable SHA; a local pass does not certify remote CI.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"complete_versioned_conformance={report['complete_versioned_conformance']}; missing={len(missing)}; {args.output}")
    if args.strict and not report["complete_versioned_conformance"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
