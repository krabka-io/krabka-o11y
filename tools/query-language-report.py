#!/usr/bin/env python3
"""Bind query evidence to one checkout; --strict forbids gaps or missing suites."""

import argparse
import collections
import copy
import hashlib
import json
import pathlib
import re
import runpy
import tempfile
import subprocess


ROOT = pathlib.Path(__file__).resolve().parent.parent
EVIDENCE_TYPED_EQUAL = runpy.run_path(str(ROOT / "tools/query-language-evidence.py"))["typed_equal"]
REQUIRED = {
    "promql-3.14.0-qualification.json",
    "promql-http-compliance-summary.json",
    "loki-remote-range.json",
    "loki-remote-instant.json",
    "tempo-forest-conformance.json",
    "pyroscope-populated-rpcs.json", "pyroscope-v2-fields.json",
    "pyroscope-v1-aggregation.json", "pyroscope-v2-async-disabled.json",
    "tempo-numeric-metric-conformance.json", "tempo-live-exemplar-conformance.json", "tempo-typed-group-conformance.json", "tempo-field-arithmetic-conformance.json",
    *{f"tempo-metric-result-{ordinal}.json" for ordinal in range(4)},
    "metrics-v1-query-transitions.json", "metrics-v2-query-transitions.json",
    "tempo-structural-query-transitions.json", "tempo-tenant-query-transitions.json",
    "diff_mimir-report.json", "diff_prometheus-report.json", "backup-query-invariance.json",
    *{f"{language}-generated-differential.json" for language in ("promql", "logql", "traceql", "pyroscope", "promql-rejections", "logql-rejections", "traceql-rejections", "pyroscope-rejections", "traceql-field-comparisons")},
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def paired_rejection(case):
    oracle, candidate = case.get("oracle"), case.get("candidate")
    valid = lambda response: (isinstance(response, dict)
                              and type(response.get("error_code")) is str
                              and bool(response["error_code"])
                              and type(response.get("status")) is int
                              and 400 <= response["status"] <= 599)
    return (valid(oracle) and valid(candidate) and EVIDENCE_TYPED_EQUAL(oracle, candidate)
            and case.get("oracle_error") is None and case.get("candidate_error") is None)


def case_verdict(case):
    if case.get("classification") == "paired-expected-error":
        return "matched" if case.get("status") in ("matched", "pass", "passed") and paired_rejection(case) else "invalid-rejection"
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
    "diff_mimir-report.json": 1795,
    "diff_prometheus-report.json": 1795,
    "promql-3.14.0-qualification.json": 2195,
    "promql-http-compliance-summary.json": 539,
    "loki-remote-range.json": 104,
    "loki-remote-instant.json": 81,
    "tempo-forest-conformance.json": 8,
    "tempo-numeric-metric-conformance.json": 6,
    "tempo-live-exemplar-conformance.json": 1,
    "tempo-typed-group-conformance.json": 2,
    "tempo-field-arithmetic-conformance.json": 4,
    "pyroscope-populated-rpcs.json": 89,
    "pyroscope-v2-fields.json": 91,
    "pyroscope-v1-aggregation.json": 2,
    "pyroscope-v2-async-disabled.json": 2,
    "metrics-v1-query-transitions.json": 26,
    "metrics-v2-query-transitions.json": 26,
    "tempo-structural-query-transitions.json": 46,
    "tempo-tenant-query-transitions.json": 51,
}

PROVENANCE_SOURCES = (
    "MODULE.bazel", "bazel/images/images.bzl", "rust-toolchain.toml",
    "tools/build-query-runners.py",
)

CURATED_ADAPTER_EXCLUSIONS = {
    "aggregators.test:426", "aggregators.test:430",
    "extended_vectors.test:443", "extended_vectors.test:446", "extended_vectors.test:449",
    "functions.test:426", "functions.test:430", "functions.test:435", "functions.test:440",
    "functions.test:899", "functions.test:908", "functions.test:917", "functions.test:930",
    "info.test:108", "native_histograms.test:1233", "native_histograms.test:1237",
    "native_histograms.test:1243", "limit.test",
}


def pyroscope_image_binding(image):
    """Resolve immutable IDs from the pinned manifest and its config offline.

    Docker's classic image store reports the config digest; its containerd
    image store can report the manifest digest. Both must bind to the same
    reviewed image, rather than accepting arbitrary digest-shaped IDs.
    """
    manifest_path = ROOT / "qualification/query-language-pyroscope-manifest.json"
    config_path = ROOT / "qualification/query-language-pyroscope-config.json"
    if "sha256:" + digest(manifest_path) != image[4]:
        raise ValueError("reviewed Pyroscope manifest differs from MODULE.bazel pin")
    manifest = json.loads(manifest_path.read_text())
    config_digest = manifest.get("config", {}).get("digest")
    if config_digest != "sha256:" + digest(config_path):
        raise ValueError("reviewed Pyroscope config differs from pinned manifest")
    config = json.loads(config_path.read_text())
    labels = config.get("config", {}).get("Labels", {})
    pin = json.loads((ROOT / "qualification/query-language-inventory.json").read_text())["surfaces"]["pyroscope"]["pin"]
    if (config.get("architecture") != "amd64" or config.get("os") != "linux"
            or labels.get("org.opencontainers.image.revision") != pin["revision"]
            or labels.get("org.opencontainers.image.version") != image[3]):
        raise ValueError("reviewed Pyroscope image platform/source/version differs from fixture")
    return {image[4], config_digest}


def pyroscope_binding_errors(name, report):
    if name not in ("pyroscope-populated-rpcs.json", "pyroscope-v2-fields.json",
                    "pyroscope-v1-aggregation.json", "pyroscope-v2-async-disabled.json"):
        return []
    if not isinstance(report, dict):
        return ["Pyroscope report must be an object"]
    errors = []
    upstream = report.get("upstream", {})
    if not isinstance(upstream, dict):
        return ["Pyroscope upstream binding must be an object"]
    pin = json.loads((ROOT / "qualification/query-language-inventory.json").read_text())["surfaces"]["pyroscope"]["pin"]
    image = re.search(r'\("pyroscope", "([^"]+)", "([^"]+)", "([^"]+)", "(sha256:[0-9a-f]{64})"\)',
                      (ROOT / "MODULE.bazel").read_text())
    if image is None:
        return ["Pyroscope image pin missing from MODULE.bazel"]
    try:
        image_ids = pyroscope_image_binding(image)
    except (OSError, ValueError, AttributeError) as error:
        return [str(error)]
    revision = upstream.get("source_revision") if name == "pyroscope-populated-rpcs.json" else report.get("upstream_revision")
    if revision != pin["revision"] or upstream.get("image_tag") != image[3] or upstream.get("image_id") not in image_ids:
        errors.append("Pyroscope source revision or loaded image differs from pin")
    if report.get("suite") != name.removesuffix(".json") or report.get("status") != "passed":
        errors.append("Pyroscope suite did not declare complete successful execution")
    if name == "pyroscope-populated-rpcs.json":
        if upstream.get("storage") != "v1" or upstream.get("query_analysis_series_enabled") is not True:
            errors.append("Pyroscope v1 storage/query-analysis configuration differs from fixture")
    elif name == "pyroscope-v2-fields.json":
        expected = {"oracle": {"query-frontend.async-queries-enabled": True},
                    "candidate": {"query_architecture": "v2", "async_queries_enabled": True}}
        if report.get("storage") != "v2" or not EVIDENCE_TYPED_EQUAL(expected, report.get("settings")):
            errors.append("Pyroscope v2 architecture/async configuration differs from fixture")
    else:
        architecture = "v1" if name == "pyroscope-v1-aggregation.json" else "v2"
        expected = {"query_architecture": architecture,
                    "oracle_async_enabled": False, "candidate_async_enabled": False}
        if not EVIDENCE_TYPED_EQUAL(expected, report.get("settings")):
            errors.append("Pyroscope architecture control configuration differs from fixture")
    return errors


PROMQL_CORPUS_FILES = {
    "aggregators.test", "at_modifier.test", "collision.test", "duration_expression.test",
    "extended_vectors.test", "fill-modifier.test", "functions.test", "histograms.test",
    "info.test", "limit.test", "literals.test", "name_label_dropping.test",
    "native_histograms.test", "operators.test", "range_queries.test", "selectors.test",
    "staleness.test", "start_timestamps.test", "subquery.test", "trig_functions.test",
    "type_and_unit.test",
}


def promql_corpus_queries():
    """Read exact eval expressions after the pinned fixture command header."""
    directory = ROOT / "crates/promql/tests/testdata/upstream-3.14.0"
    if {path.name for path in directory.glob("*.test")} != PROMQL_CORPUS_FILES:
        raise ValueError("pinned PromQL fixture file set changed")
    result = {}
    for name in sorted(PROMQL_CORPUS_FILES):
        queries = []
        for line in (directory / name).read_text().splitlines():
            header = line.strip()
            if not header.startswith(("eval ", "eval_fail ")):
                continue
            match = re.fullmatch(r"(?:eval|eval_fail) (?:instant at \S+|range from \S+ to \S+ step \S+)\s+(.+)", header)
            if match is None:
                raise ValueError(f"unsupported pinned evaluation header: {name}")
            queries.append(match[1].strip())
        result[name] = queries
    if sum(map(len, result.values())) != 2195:
        raise ValueError("pinned PromQL evaluation denominator changed")
    return result


def promql_case_binding_errors(report):
    expected = promql_corpus_queries()
    files = report.get("files")
    if not isinstance(files, list) or any(not isinstance(file, dict) for file in files):
        return ["PromQL corpus files must be an array of objects"]
    names = [file.get("name") for file in files]
    if (any(not isinstance(name, str) for name in names)
            or len(names) != len(expected) or set(names) != set(expected)):
        return ["PromQL corpus requires each pinned file exactly once"]
    errors = []
    for file in files:
        cases = file.get("cases")
        queries = expected[file["name"]]
        if not isinstance(cases, list) or any(not isinstance(case, dict) for case in cases):
            errors.append("PromQL file cases must be an array of objects")
            continue
        ordinals = [case.get("ordinal") for case in cases]
        if (any(type(ordinal) is not int for ordinal in ordinals)
                or len(ordinals) != len(queries) or set(ordinals) != set(range(1, len(queries) + 1))):
            errors.append(f"PromQL corpus ordinal set differs: {file['name']}")
            continue
        for case in cases:
            if case.get("query") != queries[case["ordinal"] - 1]:
                errors.append(f"PromQL corpus query differs: {file['name']}/{case['ordinal']}")
    return errors


def qualification_counts(name, report, generated_count=None):
    errors = []
    try:
        counts = verdicts(report)
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        return collections.Counter(uncovered=1), [str(error)]
    if name in ("diff_mimir-report.json", "diff_prometheus-report.json"):
        cases = report.get("cases", [])
        excluded = [case for case in cases if case.get("status") == "skipped"]
        ids = [case.get("id") for case in cases]
        if (report.get("suite") != name.removesuffix("-report.json")
                or any(not isinstance(case_id, str) or not case_id for case_id in ids)
                or len(ids) != len(set(ids))
                or {case.get("id") for case in excluded} != CURATED_ADAPTER_EXCLUSIONS
                or any(not isinstance(case.get("detail"), str) or not case["detail"].strip() for case in excluded)
                or report.get("run") != len(cases) - len(excluded)
                or report.get("skipped") != len(excluded)):
            errors.append("curated adapter case ledger or declared exclusions changed")
        # These exclusions belong to the historical, time-shifted HTTP adapter.
        # They stay gaps in strict qualification, independently of execution.
        counts["adapter-excluded"] = counts.pop("skipped", 0)
        counts["expected-divergence"] = counts.pop("expected_divergence", 0)
    if any(type(count) is not int or count < 0 for count in counts.values()):
        return collections.Counter(uncovered=1), ["case counts must be nonnegative integers"]
    expected = PINNED_COUNTS.get(name)
    errors.extend(pyroscope_binding_errors(name, report))
    if name == "promql-3.14.0-qualification.json" and report.get("enable_type_and_unit_labels") is not False:
        errors.append("PromQL corpus engine type/unit-label flag differs from pinned test engine")
    if name == "promql-3.14.0-qualification.json":
        errors.extend(promql_case_binding_errors(report))
        if type(report.get("experimental_functions")) is not bool:
            errors.append("PromQL corpus must declare its experimental feature configuration")
        functions = {function["name"] for function in json.loads((ROOT / "qualification/query-language-inventory.json").read_text())["surfaces"]["promql"]["functions"] if function["experimental"]}
        feature_registry = report.get("experimental_feature_registry", {})
        if (feature_registry.get("revision") != report.get("upstream_revision")
                or feature_registry.get("functions_source") != "promql/parser/functions.go"
                or set(feature_registry.get("functions", [])) != functions
                or feature_registry.get("aggregators_source") != "promql/parser/lex.go:IsExperimentalAggregator"
                or feature_registry.get("aggregators") != ["limitk", "limit_ratio"]):
            errors.append("PromQL disabled-feature provenance differs from pinned registry")
        for file in report.get("files", []):
            for case in file.get("cases", []):
                if case.get("status") == "feature_disabled":
                    disabled = case.get("disabled_features", [])
                    if (report.get("experimental_functions") is not False or not disabled
                            or any(feature not in functions | {"limitk", "limit_ratio"} for feature in disabled)
                            or case.get("observed_status") not in ("matched", "mismatch", "uncovered")
                            or "evaluation_error" not in case or "observed_detail" not in case):
                        errors.append("PromQL disabled case lacks configuration, source gate or preserved observed result")
    if isinstance(report, dict) and isinstance(report.get("cases"), list):
        identities = [case.get("stableid", case.get("id")) for case in report["cases"] if isinstance(case, dict)]
        if any(identity is not None for identity in identities):
            valid = [identity for identity in identities if isinstance(identity, str) and identity]
            if len(valid) != len(identities) or len(set(valid)) != len(valid):
                errors.append("stable case identities must be nonempty and unique")
    if name.endswith("-generated-differential.json"):
        if generated_count is None:
            errors.append("missing or invalid generated_cases in run-profile.txt")
        else:
            expected = generated_count
            if type(report.get("planned")) is not int or report["planned"] != generated_count:
                errors.append("generated planned count differs from run profile")
        if report.get("seed") != 42 or report.get("maximum_depth") != 3:
            errors.append("generated seed/depth differs from bounded generator contract")
        expected_outcome = "query-rejection" if name.endswith("-rejections-generated-differential.json") else "query-result"
        if report.get("expected_outcome") != expected_outcome:
            errors.append("generated expected outcome differs from suite")
        if not name.endswith("-rejections-generated-differential.json"):
            if report.get("generation_mode") != "type-preserving-ast" or report.get("shrinking") != "same-result-type-ast-subtrees":
                errors.append("positive generated queries must use typed AST generation and shrinking")
            expected_type = {"promql":"PromVector", "logql":"LogVector", "traceql":"TracePredicate", "traceql-field-comparisons":"TracePredicate", "pyroscope":"ProfileSelector"}.get(report.get("language"))
            verifier = runpy.run_path(str(ROOT / "tools/query-language-evidence.py"))["typed_tree"]
            for case in report.get("cases", []):
                try:
                    value_type, expression, depth, parents = verifier(case.get("typed_ast"))
                    if (value_type != expected_type or case.get("value_type") != value_type
                            or case.get("expression") != expression or depth > 3 or case.get("parents") != parents
                            or any(attempt.get("expression") not in parents for attempt in case.get("shrink_attempts", []))):
                        raise ValueError("generated query/type/subtrees differ from serialized AST")
                except (AttributeError, KeyError, TypeError, ValueError) as error:
                    errors.append(str(error))
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
        cases = report.get("cases")
        if not isinstance(cases, list) or any(not isinstance(case, dict) for case in cases):
            errors.append("Loki exact query case metadata missing")
        else:
            case_names = [case.get("name") for case in cases]
            valid_names = [name for name in case_names if isinstance(name, str)]
            if len(case_names) != len(set(valid_names)) or set(valid_names) != set(names):
                errors.append("Loki query metadata does not bind every named outcome")
            for case in cases:
                status = case.get("status")
                query = case.get("query")
                if (not isinstance(query, str) or not query.strip()
                        or status not in ("passed", "failed", "skipped", "not_run")
                        or case.get("name") not in report.get(status, [])
                        or case.get("expected_outcome") not in ("query-result", "semantic-negative")
                        or case.get("semantic_comparison_completed") is not (status == "passed")):
                    errors.append("Loki query metadata/comparison differs from completion status")
        if report.get("success") is not True and is_complete(counts):
            errors.append("Loki runner did not declare successful execution")
    if expected is not None:
        total = sum(counts.values())
        if total > expected:
            errors.append("pinned denominator changed")
        elif total < expected:
            counts["uncovered"] += expected - total
            errors.append("pinned suite execution denominator is incomplete")
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
    required.update((directory / name).resolve() for name in (
        "commit", "run-result", "target", "run-profile.txt", "source-sha256.txt",
        "oracle-images.bzl", "test.log",
    ))
    for path in sorted(required - listed):
        errors.append(f"file omitted from SHA256SUMS: {path}")
    if not any(path.is_relative_to(root / "raw") for path in listed):
        errors.append("SHA256SUMS contains no raw evidence")
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        if (directory / "commit").read_text().strip() != commit:
            errors.append("artifact commit differs from current checkout")
        if (directory / "run-result").read_text().strip() != "success":
            errors.append("artifact suite did not finish successfully")
        target = (directory / "target").read_text().strip()
        if re.fullmatch(r"//[A-Za-z0-9_./-]+:[A-Za-z0-9_.-]+", target) is None:
            errors.append("artifact target is not a single Bazel test label")
        profile = {}
        for line in (directory / "run-profile.txt").read_text().splitlines():
            key, separator, value = line.partition("=")
            if not separator or not key or key in profile:
                errors.append("malformed or duplicate run-profile entry")
            profile[key] = value
        if (profile.get("generated_cases"), profile.get("generated_nightly")) not in (("16", "0"), ("512", "1")):
            errors.append("generated case count/nightly profile differs from allowed execution profiles")
        for field in ("docker", "upstream_runner"):
            if profile.get(field) not in ("true", "false"):
                errors.append(f"run-profile {field} must be true or false")
        expected_docker = target.endswith(("_docker_test", "_version_test"))
        if profile.get("docker") != str(expected_docker).lower():
            errors.append("run-profile Docker setting differs from selected suite")
        expected_runner = "promql" if "diff_prometheus" in target else "logql" if "loki_differential" in target else None
        if profile.get("upstream_runner") != str(expected_runner is not None).lower():
            errors.append("run-profile upstream runner differs from selected suite")
        if expected_runner is not None:
            pins = runpy.run_path(str(ROOT / "tools/build-query-runners.py"))["RUNNERS"]
            errors.extend(verify_runner_binding(directory, expected_runner, pins[expected_runner]))
        if profile.get("cache_test_results") != "false" or profile.get("flaky_test_attempts") != "1":
            errors.append("artifact permits cached results or automatic reruns")
        for field in ("run_id", "run_attempt"):
            if re.fullmatch(r"[1-9][0-9]*", profile.get(field, "")) is None:
                errors.append(f"run-profile {field} must be a positive integer")
        if not profile.get("runner") or profile.get("runner_os") != "Linux":
            errors.append("run-profile runner identity/OS missing or invalid")
        sources = {}
        for line in (directory / "source-sha256.txt").read_text().splitlines():
            match = re.fullmatch(r"([0-9a-f]{64}) [ *](.+)", line)
            if match is None or match[2] in sources:
                errors.append("malformed or duplicate source checksum entry")
                continue
            sources[match[2]] = match[1]
        if sources != {name: digest(ROOT / name) for name in PROVENANCE_SOURCES}:
            errors.append("artifact source checksums differ from current checkout")
        if (directory / "oracle-images.bzl").read_bytes() != (ROOT / "bazel/images/images.bzl").read_bytes():
            errors.append("artifact oracle image configuration differs from current checkout")
    except (OSError, UnicodeError) as error:
        errors.append(f"missing or unreadable artifact provenance: {error}")
    return errors


def verify_runner_binding(directory, name, pin):
    if directory is None:
        return ["missing runner binding directory"]
    provenance_path = directory / f"{name}-runner-provenance.json"
    invocation_path = directory / "query-runner-invocation.json"
    try:
        provenance = json.loads(provenance_path.read_text())
        invocation = json.loads(invocation_path.read_text())
        target = (directory / "target").read_text().strip()
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
    allowed_targets = {"//crates/metrics-service:diff_prometheus_docker_test",
                       "//crates/metrics-service:diff_prometheus_previous_docker_test"} if name == "promql" else {"//crates/observability:loki_differential_docker_test"}
    flag = "KRABKA_PROMQL_COMPLIANCE_BIN" if name == "promql" else "KRABKA_LOKI_CORRECTNESS_BIN"
    binary_flags = [argument for argument in command if isinstance(argument, str) and argument.startswith(f"--test_env={flag}=")] if isinstance(command, list) else []
    if (target not in allowed_targets or not isinstance(command, list) or any(not isinstance(argument, str) for argument in command) or command[:2] != ["bazel", "test"] or target not in command
            or not isinstance(runner.get("binary"), str) or not binary_flags
            or binary_flags[-1] != f"--test_env={flag}={runner.get('binary')}"):
        errors.append("invocation command does not bind suite and runner executable")
    return errors


def is_complete(counts):
    if any(type(count) is not int or count < 0 for count in counts.values()):
        raise ValueError("case counts must be nonnegative integers")
    return sum(counts.values()) > 0 and all(status in ("matched", "pass", "passed") or count == 0 for status, count in counts.items())


def valid_execution_evidence(report, allow_partial=False):
    allowed = {"matched", "pass", "passed", "feature_disabled", "expected-divergence", "unsupported-oracle", "adapter-excluded"}
    return (
        bool(report["integrity_errors_checked"]) and not report["dirty_checkout"]
        and (allow_partial or not report["missing_suites"])
        and not report["unbound_artifacts"] and not report["unsuccessful_runs"] and not report["invalid_reports"]
        and not report["integrity_errors"] and not report["runner_binding_errors"]
        and all(all(status in allowed or count == 0 for status, count in artifact["counts"].items()) for artifact in report["artifacts"])
    )


def usable_evidence(reference, kind, executed, rejected_cases):
    return executed.get(reference) in ("matched", "pass", "passed") and (kind == "negative" or reference not in rejected_cases)


def self_check():
    bound = {"integrity_errors_checked": ["suite"], "dirty_checkout": False, "missing_suites": [],
             "unbound_artifacts": [], "unsuccessful_runs": [], "invalid_reports": [],
             "integrity_errors": {}, "runner_binding_errors": {},
             "artifacts": [{"counts": {"matched": 88, "unsupported-oracle": 1, "feature_disabled": 117, "expected-divergence": 4}}]}
    assert valid_execution_evidence(bound)
    assert not is_complete(bound["artifacts"][0]["counts"])
    assert not valid_execution_evidence(dict(bound, integrity_errors_checked=[]))
    for field, wrong in [("dirty_checkout", True), ("missing_suites", ["suite"]),
                         ("unbound_artifacts", ["artifact"]), ("unsuccessful_runs", ["suite"]),
                         ("invalid_reports", ["report"]), ("integrity_errors", {"suite": ["error"]}),
                         ("runner_binding_errors", {"runner": ["error"]})]:
        assert not valid_execution_evidence(dict(bound, **{field: wrong}))
    assert valid_execution_evidence(dict(bound, missing_suites=["other-suite"]), allow_partial=True)
    for status in ("failed", "mismatch", "not_run", "uncovered", "invalid-rejection", "skipped"):
        assert not valid_execution_evidence(dict(bound, artifacts=[{"counts": {"matched": 88, status: 1}}]), allow_partial=True)
    curated = {"suite": "diff_mimir", "run": 1777, "skipped": 18, "cases": [
        *[{"id": f"executed:{ordinal}", "status": "matched"} for ordinal in range(1776)],
        {"id": "declared-version-difference", "status": "expected_divergence"},
        *[{"id": case_id, "status": "skipped", "detail": "declared adapter exclusion"} for case_id in sorted(CURATED_ADAPTER_EXCLUSIONS)],
    ]}
    counts, errors = qualification_counts("diff_mimir-report.json", curated)
    assert not errors and counts == {"matched": 1776, "expected-divergence": 1, "adapter-excluded": 18}
    assert not is_complete(counts)
    assert valid_execution_evidence(dict(bound, artifacts=[{"counts": counts}]), allow_partial=True)
    for change in ("extra-exclusion", "missing-case", "duplicate-case", "missing-reason"):
        altered = copy.deepcopy(curated)
        if change == "extra-exclusion":
            altered["cases"][0].update(status="skipped", detail="unexpected omission")
            altered.update(run=1776, skipped=19)
        elif change == "missing-case":
            altered["cases"].pop(0)
            altered["run"] -= 1
        elif change == "duplicate-case":
            altered["cases"][0]["id"] = altered["cases"][1]["id"]
        else:
            altered["cases"][-1]["detail"] = ""
        assert qualification_counts("diff_mimir-report.json", altered)[1]
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
                "failed": [], "skipped": [], "not_run": [],
                "cases": [{"name": f"case-{number}", "query": "sum(count_over_time({app=\"test\"}[5m]))",
                           "status": "passed", "expected_outcome": "query-result",
                           "semantic_comparison_completed": True} for number in range(total)]}
        counts, errors = qualification_counts(name, loki)
        assert is_complete(counts) and not errors
        for field, value in [("planned", 1), ("passed", ["case-0"]),
                             ("passed", ["case-0"] * total), ("success", False), ("mode", "wrong")]:
            counts, errors = qualification_counts(name, dict(loki, **{field: value}))
            assert errors or not is_complete(counts)
        for bad_cases in ([], [{"name": "case-0", "query": "", "status": "passed"}],
                          [{**case, "semantic_comparison_completed": False} for case in loki["cases"]]):
            assert qualification_counts(name, dict(loki, cases=bad_cases))[1]
        short = dict(loki, planned=1, passed_count=1, passed=["case-0"])
        counts, errors = qualification_counts(name, short)
        assert errors and counts["uncovered"] == total - 1 and not is_complete(counts)
    prom = {"compared_payloads": 539, "paired_expected_errors": 0, "skipped_comparisons": 0, "mismatches": 0}
    counts, errors = qualification_counts("promql-http-compliance-summary.json", prom)
    assert is_complete(counts) and not errors
    for total in (1, 540):
        counts, errors = qualification_counts("promql-http-compliance-summary.json", dict(prom, compared_payloads=total))
        assert errors or not is_complete(counts)
    corpus = {"files": [{"name": name, "cases": [
        {"ordinal": ordinal, "query": query, "status": "matched"}
        for ordinal, query in enumerate(queries, 1)]}
        for name, queries in promql_corpus_queries().items()]}
    assert not promql_case_binding_errors(corpus)
    # Each mutation preserves the full 2,195 count; counts alone cannot qualify it.
    duplicated = copy.deepcopy(corpus)
    duplicated["files"][0]["cases"][0] = copy.deepcopy(duplicated["files"][0]["cases"][1])
    missing = copy.deepcopy(corpus)
    missing["files"][0]["cases"].extend(missing["files"].pop()["cases"])
    wrong = copy.deepcopy(corpus)
    wrong["files"][0]["cases"][0]["query"] = "0"
    renamed = copy.deepcopy(corpus)
    renamed["files"][0]["name"] = "renamed.test"
    truncated = copy.deepcopy(corpus)
    suffix_case = next(case for file in truncated["files"] for case in file["cases"] if len(case["query"].split()) > 1)
    suffix_case["query"] = suffix_case["query"].split()[-1]
    for changed in (duplicated, missing, wrong, renamed, truncated):
        assert sum(len(file["cases"]) for file in changed["files"]) == 2195
        assert promql_case_binding_errors(changed)
    tree = {"node": {"node": "trace_duration", "field": {"scope":"", "key":"duration", "scalar_type":"Duration"}, "op":"Gt", "nanos":0}}
    generated_case = {"status":"pass", "typed_ast":tree, "value_type":"TracePredicate", "expression":"duration > 0ns", "parents":[], "shrink_attempts":[]}
    generated = {"planned":2, "language":"traceql", "seed":42, "maximum_depth":3, "expected_outcome":"query-result", "generation_mode":"type-preserving-ast", "shrinking":"same-result-type-ast-subtrees", "cases":[generated_case, generated_case]}
    counts, errors = qualification_counts("traceql-generated-differential.json", generated, 2)
    assert is_complete(counts) and not errors
    for changed in ({**generated_case, "expression":"duration < 0ns"},
                    {**generated_case, "value_type":"PromVector"},
                    {**generated_case, "parents":["name=\"bad\""]},
                    {**generated_case, "typed_ast":None},
                    {**generated_case, "shrink_attempts":[{"expression":"invalid()"}]}):
        assert qualification_counts("traceql-generated-differential.json", {**generated, "cases":[generated_case, changed]}, 2)[1]
    paired = {"status":"passed", "classification":"paired-expected-error", "oracle":{"error_code":"invalid_argument", "status":400}, "candidate":{"error_code":"invalid_argument", "status":400}}
    assert case_verdict(paired) == "matched"
    for changed in ({**paired, "oracle":{"error_code":"invalid_argument", "status":200}},
                    {**paired, "candidate":{"error_code":"internal", "status":500}},
                    {**paired, "candidate":{"error_code":"invalid_argument", "status":400.0}},
                    {**paired, "candidate":{"error_code":True, "status":400}},
                    {**paired, "candidate":{"error_code":"invalid_argument", "status":True}},
                    {**paired, "candidate":{"error_code":"invalid_argument", "status":400,"extra":None}},
                    {**paired, "oracle":{**paired["oracle"],"nested":[True]}, "candidate":{**paired["candidate"],"nested":[1]}},
                    {**paired, "status":"failed"}, {**paired, "oracle_error":"connection failed"}):
        assert case_verdict(changed) == "invalid-rejection"
    for planned in (None, 1, 512):
        counts, errors = qualification_counts("traceql-generated-differential.json", generated, planned)
        assert errors and (planned != 512 or counts["uncovered"] == 510)

    with tempfile.TemporaryDirectory(prefix="query-evidence-self-check-") as temporary:
        directory = pathlib.Path(temporary)
        (directory / "raw").mkdir()
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        profile = ("generated_cases=512\ngenerated_nightly=1\ndocker=true\nupstream_runner=false\n"
                   "cache_test_results=false\nflaky_test_attempts=1\nrun_id=1\nrun_attempt=1\n"
                   "runner=control-runner\nrunner_os=Linux\n")
        sources = "".join(f"{digest(ROOT / name)}  {name}\n" for name in PROVENANCE_SOURCES)
        for name, body in [("commit", commit), ("run-result", "success"),
                           ("target", "//crates/profiles:pyroscope_differential_docker_test"),
                           ("run-profile.txt", profile), ("source-sha256.txt", sources),
                           ("oracle-images.bzl", (ROOT / "bazel/images/images.bzl").read_text()),
                           ("test.log", "executed fixture\n"), ("raw/report.json", "{}")]:
            (directory / name).write_text(body)
        files = sorted(path for path in directory.rglob("*") if path.is_file())
        manifest = "".join(f"{digest(path)}  ./{path.relative_to(directory)}\n" for path in files)
        (directory / "SHA256SUMS").write_text(manifest)
        assert not verify_manifest(directory)
        assert generated_case_count(directory) == 512
        # Rehash altered metadata: integrity alone must not accept a wrong
        # source revision, configuration, failed run, or cached execution.
        for name, body in [
            ("commit", "0" * 40), ("run-result", "failure"), ("target", "//two:targets //bad:target"),
            ("source-sha256.txt", sources.replace(sources[:64], "0" * 64, 1)),
            ("source-sha256.txt", sources + sources.splitlines()[0] + "\n"),
            ("oracle-images.bzl", "altered image configuration"),
            *[("run-profile.txt", profile.replace(old, new)) for old, new in (
                ("generated_cases=512", "generated_cases=16"),
                ("generated_nightly=1", "generated_nightly=true"),
                ("docker=true", "docker=1"), ("upstream_runner=false", "upstream_runner=0"),
                ("docker=true", "docker=false"), ("upstream_runner=false", "upstream_runner=true"),
                ("cache_test_results=false", "cache_test_results=true"),
                ("flaky_test_attempts=1", "flaky_test_attempts=2"),
                ("run_id=1", "run_id=0"), ("runner_os=Linux", "runner_os="),
            )],
            ("run-profile.txt", profile + "generated_cases=512\n"),
        ]:
            path = directory / name
            original = path.read_text()
            path.write_text(body)
            altered = "".join(f"{digest(path)}  ./{path.relative_to(directory)}\n" for path in files)
            (directory / "SHA256SUMS").write_text(altered)
            assert verify_manifest(directory), (name, body)
            path.write_text(original)
        (directory / "SHA256SUMS").write_text(manifest)
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

    image_pin = re.search(r'\("pyroscope", "([^"]+)", "([^"]+)", "([^"]+)", "(sha256:[0-9a-f]{64})"\)', (ROOT / "MODULE.bazel").read_text())
    image_ids = pyroscope_image_binding(image_pin)
    assert len(image_ids) == 2, "manifest and configuration digests must remain distinct"
    from unittest.mock import patch
    with tempfile.TemporaryDirectory(prefix="pyroscope-image-binding-self-check-") as temporary:
        directory = pathlib.Path(temporary)
        (directory / "qualification").mkdir()
        for name in ("query-language-pyroscope-manifest.json", "query-language-pyroscope-config.json", "query-language-inventory.json"):
            (directory / "qualification" / name).write_bytes((ROOT / "qualification" / name).read_bytes())
        manifest_path = directory / "qualification/query-language-pyroscope-manifest.json"
        config_path = directory / "qualification/query-language-pyroscope-config.json"
        original_manifest, original_config = manifest_path.read_bytes(), config_path.read_bytes()
        with patch.dict(globals(), ROOT=directory):
            assert pyroscope_image_binding(image_pin) == image_ids
            for file in (manifest_path, config_path):
                original = file.read_bytes()
                file.write_bytes(original + b" ")
                try:
                    pyroscope_image_binding(image_pin)
                except ValueError:
                    pass
                else:
                    raise AssertionError("altered manifest/configuration accepted")
                file.write_bytes(original)
            # Even an internally rehashed chain must preserve the source
            # revision; a matching digest shape cannot establish that contract.
            config = json.loads(original_config)
            config["config"]["Labels"]["org.opencontainers.image.revision"] = "wrong"
            config_path.write_text(json.dumps(config))
            manifest = json.loads(original_manifest)
            manifest["config"]["digest"] = "sha256:" + digest(config_path)
            manifest_path.write_text(json.dumps(manifest))
            changed_pin = [image_pin[index] for index in range(5)]
            changed_pin[4] = "sha256:" + digest(manifest_path)
            try:
                pyroscope_image_binding(changed_pin)
            except ValueError:
                pass
            else:
                raise AssertionError("rehashed image with another source revision accepted")
    upstream = {"image_tag": image_pin[3], "image_id": image_pin[4]}
    revision = json.loads((ROOT / "qualification/query-language-inventory.json").read_text())["surfaces"]["pyroscope"]["pin"]["revision"]
    for name, settings in [
        ("pyroscope-v2-fields.json", {"oracle": {"query-frontend.async-queries-enabled": True}, "candidate": {"query_architecture": "v2", "async_queries_enabled": True}}),
        ("pyroscope-v1-aggregation.json", {"query_architecture": "v1", "oracle_async_enabled": False, "candidate_async_enabled": False}),
        ("pyroscope-v2-async-disabled.json", {"query_architecture": "v2", "oracle_async_enabled": False, "candidate_async_enabled": False}),
        ("pyroscope-populated-rpcs.json", None),
    ]:
        report = {"suite": name.removesuffix(".json"), "status": "passed", "upstream": dict(upstream), "upstream_revision": revision, "storage": "v2", "settings": settings}
        if settings is None:
            report["upstream"].update(source_revision=revision, storage="v1", query_analysis_series_enabled=True)
        assert not pyroscope_binding_errors(name, report)
        for image_id in image_ids:
            assert not pyroscope_binding_errors(name, dict(report, upstream={**report["upstream"], "image_id": image_id}))
        for field, wrong in [("status", "running"), ("suite", "other-suite"), ("settings", {}), ("upstream_revision", "wrong")]:
            if settings is None and field in ("settings", "upstream_revision"):
                continue
            assert pyroscope_binding_errors(name, dict(report, **{field: wrong}))
        for field, wrong in [("image_tag", "wrong:2.3.1"), ("image_id", "sha256:" + "0" * 64)]:
            assert pyroscope_binding_errors(name, dict(report, upstream={**report["upstream"], field: wrong}))
        if settings is None:
            for field, wrong in [("storage", "v2"), ("query_analysis_series_enabled", 1), ("source_revision", "wrong")]:
                assert pyroscope_binding_errors(name, dict(report, upstream={**report["upstream"], field: wrong}))
        else:
            changed = copy.deepcopy(report)
            if name == "pyroscope-v2-fields.json":
                changed["settings"]["candidate"]["async_queries_enabled"] = 1
            else:
                changed["settings"]["oracle_async_enabled"] = 0
            assert pyroscope_binding_errors(name, changed)

    with tempfile.TemporaryDirectory(prefix="query-runner-self-check-") as temporary:
        directory = pathlib.Path(temporary)
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
        target_path = directory / "target"
        target_path.write_text(invocation["command"][2])
        assert not verify_runner_binding(directory, "promql", pin)
        previous = "//crates/metrics-service:diff_prometheus_previous_docker_test"
        target_path.write_text(previous)
        assert verify_runner_binding(directory, "promql", pin)
        invocation_path.write_text(json.dumps(dict(invocation, command=["bazel", "test", previous, invocation["command"][3]])))
        assert not verify_runner_binding(directory, "promql", pin)
        target_path.write_text("//crates/metrics-service:diff_prometheus_fake_docker_test")
        assert verify_runner_binding(directory, "promql", pin)
        target_path.write_text(invocation["command"][2])
        invocation_path.write_text(json.dumps(invocation))
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
    parser.add_argument("--fail-invalid-evidence", action="store_true",
                        help="Fail invalid provenance, missing executions and failures while retaining advertised feature gaps")
    parser.add_argument("--allow-partial", action="store_true",
                        help="Validate a selected PR suite without requiring every nightly report")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    subprocess.run(["python3", str(ROOT / "tools/query-language-inventory.py"), "--check"], check=True)
    inventory_path = ROOT / "qualification/query-language-inventory.json"
    inventory = json.loads(inventory_path.read_text())
    registry_path = ROOT / "qualification/query-language-evidence.json"
    registry = json.loads(registry_path.read_text())
    evidence_helpers = runpy.run_path(str(ROOT / "tools/query-language-evidence.py"))
    artifacts = []
    executed = {}
    rejected_cases = set()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    unbound = []
    unsuccessful_runs = []
    integrity_errors = {}
    invalid_reports = []
    runner_errors = {}

    def record_executed(reference, status, case, report_name, report, path):
        errors = evidence_helpers["runtime_errors"](registry, reference, report_name, case, report)
        if errors:
            invalid_reports.append({"path": str(path), "case": reference, "errors": errors})
            status = "invalid_evidence"
        previous = executed.get(reference)
        # Repeated artifacts cannot hide a failed witness behind a later pass.
        if previous is None or previous in ("matched", "pass", "passed"):
            executed[reference] = status

    runner_pins = runpy.run_path(str(ROOT / "tools/build-query-runners.py"))["RUNNERS"]
    if runner_pins["logql"][1] != inventory["surfaces"]["logql"]["pin"]["revision"]:
        runner_errors["logql pin"] = ["builder revision differs from inventory"]
    for directory in args.evidence:
        bindings = list(directory.rglob("commit"))
        if not bindings:
            integrity_errors[str(directory)] = ["missing artifact commit binding"]
        for binding in sorted(bindings):
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
                        configuration = "experimental" if report["experimental_functions"] else "default"
                        reference = f"promql/{configuration}/{file['name']}/{case['ordinal']}"
                        record_executed(reference, case["status"], case, path.name, report, path)
                        if case.get("expected_rejection"):
                            rejected_cases.add(reference)
            elif path.name.endswith("-generated-differential.json"):
                for ordinal, case in enumerate(report["cases"], 1):
                    reference = f"{report['language']}/generated/{ordinal}"
                    record_executed(reference, case["status"], case, path.name, report, path)
                    if report.get("expected_outcome") == "query-rejection" or report["language"].endswith("-rejections"):
                        rejected_cases.add(reference)
            elif isinstance(report, list):
                for ordinal, case in enumerate(report, 1):
                    record_executed(f"traceql/forest/{ordinal}", case.get("status", "uncovered"),
                                    case, path.name, {}, path)
            elif "passed_count" in report:
                metadata = {case["name"]: case for case in report["cases"]}
                for status in ("passed", "failed", "skipped", "not_run"):
                    for name in report[status]:
                        record_executed(f"logql/{report['mode']}/{name}", status,
                                        metadata[name], path.name, report, path)
            elif "cases" in report:
                namespace = f"storage/transitions/{path.stem}" if path.name.endswith("-query-transitions.json") else f"traceql/metrics/{path.stem}" if metric_result or path.name.startswith("tempo-numeric-") or path.name.startswith("tempo-live-exemplar-") or path.name.startswith("tempo-typed-group-") or path.name.startswith("tempo-field-arithmetic-") else "pyroscope/v2" if path.name == "pyroscope-v2-fields.json" else "pyroscope/v1-aggregation" if path.name == "pyroscope-v1-aggregation.json" else "pyroscope/v2-async-disabled" if path.name == "pyroscope-v2-async-disabled.json" else {"diff_mimir-report.json":"promql/mimir", "diff_prometheus-report.json":"promql/prometheus", "backup-query-invariance.json":"storage/restore"}.get(path.name, "pyroscope/rpc")
                for ordinal, case in enumerate(report["cases"], 1):
                    identity = case.get("stableid", case.get("id", ordinal))
                    reference = f"{namespace}/{identity}"
                    record_executed(reference, case_verdict(case), case, path.name, report, path)
                    if case.get("expected_rejection") or case.get("classification") == "paired-expected-error":
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
        "integrity_errors_checked": sorted(integrity_errors),
        "runner_binding_errors": {path: errors for path, errors in runner_errors.items() if errors},
        "invalid_reports": invalid_reports,
        "inventory_sha256": digest(inventory_path),
        "evidence_registry_sha256": digest(registry_path),
        "reviewed_case_descriptors": registry["cases"],
        "feature_gaps": {language: {feature["id"]: feature["gaps"]
                                     for feature in surface["features"] if feature["gaps"]}
                         for language, surface in inventory["surfaces"].items()},
        "oracle_manifest_sha256": digest(ROOT / "bazel/images/images.bzl"),
        "fixture_sources": {str(path.relative_to(ROOT)): digest(path) for pattern in (
            "crates/promql/tests/testdata/upstream-3.14.0/*.test",
            "crates/*/tests/support/*fixture.rs", "crates/*/tests/*differential.rs",
            "crates/metrics-service/tests/diff_*.rs", "crates/*/tests/*deployment.rs",
            "crates/integration/tests/backup_restore.rs",
            "crates/traceql/tests/testdata/traceql/*.case",
            "qualification/query-language-pyroscope-*.json",
        ) for path in sorted(ROOT.glob(pattern))},
        "features": features,
        "feature_execution": {language: {
            feature["id"]: {role: {"reviewed": len(references),
                                     "matched": sum(usable_evidence(reference, role, executed, rejected_cases) for reference in references),
                                     "unresolved": [reference for reference in references if not usable_evidence(reference, role, executed, rejected_cases)]}
                            for role, references in feature["evidence"].items()}
            for feature in surface["features"]}
            for language, surface in inventory["surfaces"].items()},
        "missing_suites": missing, "artifacts": artifacts,
        "unresolved_evidence": unresolved_evidence, "executed_case_ids": executed,
        "complete_versioned_conformance": not dirty and not missing and not unbound and not unsuccessful_runs
            and not any(integrity_errors.values()) and not any(runner_errors.values())
            and not invalid_reports and all(artifact["complete_corpus"] for artifact in artifacts)
            and all(set(counts) <= {"mapped"} for counts in features.values())
            and not unresolved_evidence,
    }
    report["valid_execution_evidence"] = valid_execution_evidence(report, args.allow_partial)
    # This tool binds local evidence only. Remote CI must produce its own artifact
    # on the final immutable SHA; a local pass does not certify remote CI.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"complete_versioned_conformance={report['complete_versioned_conformance']}; missing={len(missing)}; {args.output}")
    if args.strict and not report["complete_versioned_conformance"]:
        raise SystemExit(1)
    if args.fail_invalid_evidence and not report["valid_execution_evidence"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
