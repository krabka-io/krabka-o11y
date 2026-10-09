#!/usr/bin/env python3
"""Bind query evidence to one checkout; --strict forbids gaps or missing suites."""

import argparse
import collections
import copy
import hashlib
import json
import math
import pathlib
import re
import runpy
import tempfile
import subprocess
import urllib.parse


ROOT = pathlib.Path(__file__).resolve().parent.parent
EVIDENCE_TYPED_EQUAL = runpy.run_path(str(ROOT / "tools/query-language-evidence.py"))["typed_equal"]
REQUIRED = {
    "promql-3.14.0-qualification.json",
    "promql-http-compliance-summary.json",
    "loki-remote-range.json",
    "loki-remote-instant.json",
    "loki-supported-template-functions.json",
    "loki-experimental-query-conformance.json",
    "tempo-pipeline-hint-conformance.json",
    "tempo-forest-conformance.json",
    "pyroscope-populated-rpcs.json", "pyroscope-v2-fields.json",
    "tempo-supported-scopes-conformance.json", "tempo-expression-output-conformance.json",
    "tempo-array-metric-frontend-conformance.json", "tempo-instant-bounds-conformance.json",
    "tempo-retrieval-shape-conformance.json", "tempo-exemplar-reservoir-conformance.json",
    "pyroscope-v1-aggregation.json", "pyroscope-v2-async-disabled.json",
    "tempo-numeric-metric-conformance.json", "tempo-live-exemplar-conformance.json", "tempo-typed-group-conformance.json", "tempo-field-arithmetic-conformance.json",
    *{f"tempo-metric-result-{ordinal}.json" for ordinal in range(4)},
    "metrics-v1-query-transitions.json", "metrics-v2-query-transitions.json",
    "tempo-structural-query-transitions.json", "tempo-tenant-query-transitions.json",
    "diff_mimir-report.json", "diff_prometheus-report.json", "backup-query-invariance.json",
    *{f"{language}-generated-differential.json" for language in ("promql", "logql", "traceql", "pyroscope", "promql-rejections", "logql-rejections", "traceql-rejections", "pyroscope-rejections", "traceql-field-comparisons")},
}

PROMQL_TARGETS = {
    False: "//crates/promql:upstream_qualification_test",
    True: "//crates/promql:upstream_qualification_experimental_test",
}
REQUIRED_TARGETS = {
    *PROMQL_TARGETS.values(),
    "//crates/metrics-service:diff_prometheus_docker_test",
    "//crates/metrics-service:diff_mimir_docker_test",
    "//crates/observability:loki_differential_docker_test",
    "//crates/traces:tempo_differential_docker_test",
    "//crates/profiles:pyroscope_differential_docker_test",
    "//crates/metrics-service:metrics_deployment_docker_test",
    "//crates/traces:tempo_deployment_docker_test",
    "//crates/profiles:pyroscope_deployment_docker_test",
    "//crates/integration:backup_restore_test",
}


def missing_suites(artifacts, targets):
    missing = REQUIRED - {artifact["name"] for artifact in artifacts}
    missing |= REQUIRED_TARGETS - targets
    configurations = {artifact.get("configuration") for artifact in artifacts
                      if artifact["name"] == "promql-3.14.0-qualification.json"}
    missing |= {f"promql-full-{mode}" for mode in ("default", "experimental") if mode not in configurations}
    return sorted(missing)


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
        return "matched" if case.get("status") in ("matched", "pass", "passed", "paired-expected-error") and paired_rejection(case) else "invalid-rejection"
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
    "diff_mimir-report.json": 1800,  # Includes five numeric last-operation cases.
    "diff_prometheus-report.json": 1795,
    "promql-3.14.0-qualification.json": 2195,
    "promql-http-compliance-summary.json": 539,
    "loki-remote-range.json": 104,
    "loki-remote-instant.json": 81,
    "loki-supported-template-functions.json": 171,
    "loki-experimental-query-conformance.json": 40,
    "tempo-pipeline-hint-conformance.json": 15,
    "tempo-forest-conformance.json": 8,
    "tempo-numeric-metric-conformance.json": 6,
    "tempo-live-exemplar-conformance.json": 1,
    "tempo-typed-group-conformance.json": 2,
    "tempo-field-arithmetic-conformance.json": 4,
    "tempo-supported-scopes-conformance.json": 12,
    "tempo-expression-output-conformance.json": 8,
    "tempo-array-metric-frontend-conformance.json": 6,
    "tempo-instant-bounds-conformance.json": 10,
    "tempo-retrieval-shape-conformance.json": 12,
    "tempo-exemplar-reservoir-conformance.json": 2,
    "pyroscope-populated-rpcs.json": 113,
    "pyroscope-v2-fields.json": 100,
    "pyroscope-v1-aggregation.json": 4,
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
        if upstream.get("storage") != "v1" or upstream.get("query_analysis_series_enabled") is not True or upstream.get("candidate_query_analysis_series_enabled") is not True or upstream.get("self_profiling_disable_push") is not True:
            errors.append("Pyroscope v1 storage/query-analysis configuration differs from fixture")
    elif name == "pyroscope-v2-fields.json":
        expected = {"oracle": {"query-frontend.async-queries-enabled": True},
                    "candidate": {"query_architecture": "v2", "async_queries_enabled": True}}
        if report.get("storage") != "v2" or not EVIDENCE_TYPED_EQUAL(expected, report.get("settings")):
            errors.append("Pyroscope v2 architecture/async configuration differs from fixture")
    else:
        architecture = "v1" if name == "pyroscope-v1-aggregation.json" else "v2"
        expected = {"query_architecture": architecture,
                    "oracle_async_enabled": False, "candidate_async_enabled": False,
                    "oracle_query_analysis_series_enabled": False, "candidate_query_analysis_series_enabled": False}
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


def loki_template_cases():
    # The reviewed fixture is a table of literal Rust string triples, not a
    # search for names in arbitrary queries. Bind every ID to its whole query.
    source = (ROOT / "crates/logql/tests/support/template_functions.rs").read_text()
    pattern = r'r(?P<h>#+)"(?P<raw>[\s\S]*?)"(?P=h)|(?P<normal>"(?:\\.|[^"\\])*")'
    literals = [match["raw"] if match["raw"] is not None else json.loads(match["normal"])
                for match in re.finditer(pattern, source)]
    if len(literals) % 3:
        raise ValueError("Loki template fixture literal triples changed")
    cases = {}
    for name, expression, line in zip(literals[::3], literals[1::3], literals[2::3]):
        if name in cases:
            raise ValueError("duplicate Loki template fixture ID")
        query = '{app="api",format="logfmt"} | line_format ' + json.dumps("{{ " + expression + " }}", ensure_ascii=False, separators=(",", ":"))
        cases[name] = (expression, query, line)
    return cases



def loki_template_seed():
    # Parse the reviewed literal seed, not an oracle response. These sixteen
    # identities define the independent timestamp/line ledger for every case.
    source = (ROOT / "crates/observability/tests/loki_differential.rs").read_text()
    parser_streams = source.partition("const PARSER_STREAMS:")[2]
    stream = re.search(r'labels:\s*&\[\("app", "api"\), \("env", "prod"\), \("format", "logfmt"\)\],\s*entries:\s*&\[(.*?)\n        \],', parser_streams, re.S)
    if stream is None:
        raise ValueError("reviewed Loki logfmt seed absent")
    literal = r'r(?P<h>#+)"(?P<raw>[\s\S]*?)"(?P=h)|(?P<normal>"(?:\\.|[^"\\])*")'
    entries = []
    for entry in re.finditer(r'SeedEntry\s*\{\s*offset_secs:\s*(?P<offset>\d+),\s*line:\s*(?:' + literal + r'),\s*metadata:\s*&\[\],\s*\}', stream[1]):
        text = entry["raw"]
        if text is None:
            quoted = re.sub(r'\\u\{([0-9A-Fa-f]+)\}', lambda match: json.dumps(chr(int(match[1], 16)))[1:-1], entry["normal"])
            text = json.loads(quoted)
        offset = int(entry["offset"])
        # Pinned distributor/field_detection.go::extractLogLevelFromLogLine
        # falls back to bounded word detection without a severity field. The
        # fixture's only level words are error at offsets eight and twelve.
        level = "error" if offset in (8, 12) else "unknown"
        if ("error" in text) != (level == "error"):
            raise ValueError("reviewed Loki seed level words changed")
        entries.append((offset, text, level))
    if [offset for offset, _, _ in entries] != [2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 21, 22]:
        raise ValueError("reviewed Loki template sixteen-entry seed changed")
    return entries


def loki_template_expected(case_id, golden, base_ns):
    streams = []
    for level in ("error", "unknown"):
        values = [[str(base_ns + offset * 1_000_000_000), text if case_id == "line" else golden]
                  for offset, text, entry_level in loki_template_seed() if entry_level == level]
        streams.append({"stream":{"app":"api", "env":"prod", "format":"logfmt", "service_name":"api", "detected_level":level}, "values":values})
    return {"status":200, "body":{"status":"success", "data":{"resultType":"streams", "result":streams}}}


def loki_template_binding_errors(report):
    if not isinstance(report, dict):
        return ["Loki template report must be an object"]
    expected = loki_template_cases()
    rows = report.get("cases")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return ["Loki template cases must be an array of objects"]
    ids = [row.get("id") for row in rows]
    if len(ids) != len(expected) or any(not isinstance(value, str) for value in ids) or set(ids) != set(expected):
        return ["Loki template fixture requires every source-bound case exactly once"]
    errors = []
    base_ns = report.get("timeline_base_ns")
    if type(base_ns) is not int or base_ns < 0:
        return ["Loki template fixture requires its source-created integer timeline_base_ns"]
    if report.get("upstream_source") != "7a40404f32b3e6464c9cfc6cc7dd75a40f3931da":
        errors.append("Loki template source revision differs from pin")
    for row in rows:
        expression, query, line = expected[row["id"]]
        request = row.get("request")
        required_request = {"method":"GET", "path":"/loki/api/v1/query_range", "tenant":"parsers", "query":query}
        if row.get("name") != row["id"] or row.get("expression") != expression or not EVIDENCE_TYPED_EQUAL(request, required_request):
            errors.append("Loki template exact expression/request differs from source fixture")
        ledger = row.get("independent_expected")
        if not isinstance(ledger, dict) or any(not EVIDENCE_TYPED_EQUAL(row.get(side), ledger) for side in ("oracle", "candidate")):
            errors.append("Loki template responses differ from independent stream ledger")
        for side in ("raw_oracle", "raw_candidate"):
            response = row.get(side)
            if not isinstance(response, dict) or type(response.get("status")) is not int or response.get("status") != 200 or not isinstance(response.get("body"), dict):
                errors.append("Loki template positive query lacks successful raw HTTP response")
        if not EVIDENCE_TYPED_EQUAL(ledger, loki_template_expected(row["id"], line, base_ns)):
            errors.append("Loki template independent result differs from exact sixteen-entry grouped fixture golden")
    return errors


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


def experimental_query_binding_errors(name, report):
    """Require real exchanges and the independent ledgers of the new suites."""
    language = {"loki-experimental-query-conformance.json": "logql",
                "tempo-pipeline-hint-conformance.json": "traceql"}.get(name)
    if language is None:
        return []
    if not isinstance(report, dict):
        return ["experimental query report must be an object"]
    pin = json.loads((ROOT / "qualification/query-language-inventory.json").read_text())["surfaces"][language]["pin"]
    expected_source = ({key:pin[key] for key in ("repository", "version", "revision")}
                       if language == "traceql" else pin["revision"])
    errors = []
    if not EVIDENCE_TYPED_EQUAL(report.get("upstream_source"), expected_source):
        errors.append("experimental query source differs from pin")
    rows = report.get("cases")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return errors + ["experimental query cases must be objects"]
    for row in rows:
        request, ledger = row.get("request"), row.get("independent_expected")
        outcome = row.get("expected_outcome")
        if not isinstance(request, dict) or ledger is None or outcome not in ("query-result", "query-rejection"):
            errors.append("experimental query lacks request, ledger or explicit outcome")
            continue
        if outcome == "query-rejection":
            if not paired_rejection(row) or row.get("classification") != "paired-expected-error":
                errors.append("experimental rejection lacks matching HTTP error contract")
            if language == "traceql":
                responses = row.get("responses", [])
                if (not isinstance(responses, list) or any(not isinstance(item, dict) for item in responses)
                        or len(responses) != 2 or {item.get("implementation") for item in responses} != {"upstream", "krabka"}
                        or any(item.get("http_status") != 400 or item.get("query_rejection") is not True
                               or not isinstance(item.get("body"), str) or not item["body"] for item in responses)
                        or ledger != {"outcome":"query-rejection", "http_status":400}):
                    errors.append("Tempo rejection lacks both classified raw HTTP diagnostics")
            else:
                for side in ("oracle", "candidate"):
                    raw = row.get("raw_" + side, {})
                    normalized = row.get(side, {})
                    if (not isinstance(raw, dict) or not isinstance(normalized, dict)
                            or raw.get("status") != row.get("expected_status") or raw.get("status") != normalized.get("status")
                            or not isinstance(raw.get("text"), str) or raw["text"].rstrip("\n") != ledger
                            or normalized.get("error") != ledger):
                        errors.append("Loki rejection differs from raw exchange or exact diagnostic ledger")
        elif language == "logql":
            discovery = row.get("id")
            if discovery in ("discovery/disabled-does-not-synthesize-unknown", "discovery/custom-logfmt-field",
                             "discovery/bounded-json-depth", "discovery/unlimited-json-depth"):
                try:
                    empty = discovery in ("discovery/disabled-does-not-synthesize-unknown", "discovery/bounded-json-depth")
                    instant = dict(request["params"])["time"]
                    # The fixtures use Loki's integer form: at most ten
                    # characters means seconds, otherwise nanoseconds.
                    integer = int(instant)
                    if not -(1 << 63) <= integer < (1 << 63):
                        raise ValueError("instant is outside Loki's signed timestamp range")
                    timestamp = float(integer) if len(instant) <= 10 else integer / 1_000_000_000
                    expected_discovery = {"samples":[] if empty else [{"metric":{}, "points":[[timestamp, "1"]]}], "warnings":[]}
                    if not math.isfinite(timestamp) or not EVIDENCE_TYPED_EQUAL(ledger, expected_discovery):
                        errors.append("Loki discovery differs from its independent eligible-entry count")
                except (KeyError, ValueError, TypeError):
                    errors.append("Loki discovery lacks its bound instant timestamp")
            if row.get("expected_status") != 200 or not EVIDENCE_TYPED_EQUAL(row.get("oracle"), row.get("candidate")):
                errors.append("Loki positive query lacks matching complete normalized responses")
            for side in ("oracle", "candidate"):
                raw = row.get("raw_" + side, {})
                body = raw.get("body", {}) if isinstance(raw, dict) else {}
                if not isinstance(raw, dict) or raw.get("status") != 200 or not isinstance(body, dict) or body.get("status") != "success":
                    errors.append("Loki positive query lacks successful raw HTTP response")
                if not EVIDENCE_TYPED_EQUAL(loki_experimental_normalized(body), row.get(side)):
                    errors.append("Loki complete normalized response differs from its raw exchange")
                if not EVIDENCE_TYPED_EQUAL(row.get(side + "_semantic"), ledger):
                    errors.append("Loki positive query differs from independent sample/warning ledger")
                if not EVIDENCE_TYPED_EQUAL(loki_experimental_semantic(body), ledger):
                    errors.append("Loki raw response differs from independent sample/warning ledger")
        else:
            if not isinstance(ledger, dict):
                errors.append("Tempo positive metric ledger must be an object")
                continue
            if row.get("stableid") in ("tempo-live-pipeline-scalar-before-metrics-negative",
                                       "tempo-live-pipeline-group-filter-before-metrics-negative"):
                try:
                    bounds = urllib.parse.parse_qs(request["range"], strict_parsing=True)
                    start, end = int(bounds["start"][0]), int(bounds["end"][0])
                    zero = {"series":[{"labels":[{"key":"__name__", "value":{"stringValue":"count_over_time"}}],
                                       "samples":[{"timestampMs":str(timestamp * 1000), "value":0.0}
                                                  for timestamp in range(start, end + 1, 30)], "exemplars":[]}]}
                    if request.get("step") != "30s" or end - start != 180 or not EVIDENCE_TYPED_EQUAL(ledger, zero):
                        errors.append("Tempo rejected spansets lack the independent complete zero-count grid")
                except (KeyError, ValueError, TypeError):
                    errors.append("Tempo zero-count grid lacks valid request bounds")
            expected = ledger.get("allowed_complete_outputs") if row.get("comparison_kind") == "independent-sampling-domain" else [ledger]
            if not isinstance(expected, list) or not expected:
                errors.append("Tempo sampling query lacks complete independently enumerated cohorts")
                continue
            for side in ("upstream", "krabka"):
                actual = row.get(side)
                if not isinstance(actual, dict) or not isinstance(actual.get("series"), list):
                    errors.append("Tempo positive query lacks successful metric-series response")
                elif not any(tempo_pipeline_metrics_match(cohort, actual) for cohort in expected):
                    errors.append("Tempo positive query differs from complete independent metric ledger")
            if row.get("comparison_kind") not in ("exact-whole-series", "independent-sampling-domain"):
                errors.append("Tempo query lacks its explicit comparison contract")
    return errors


def loki_experimental_normalized(body):
    try:
        value = copy.deepcopy(body)
        data = value["data"]
        data.pop("stats", None)
        if data["resultType"] not in ("matrix", "vector"):
            return None
        for row in data["result"]:
            points = row["values"] if data["resultType"] == "matrix" else [row["value"]]
            for point in points:
                number = point[1]
                if isinstance(number, str) and number not in ("NaN", "+Inf", "-Inf"):
                    point[1] = format(float(number), ".6f")
        data["result"].sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        return value
    except (KeyError, TypeError, ValueError, AttributeError, IndexError):
        return None


def loki_experimental_semantic(body):
    try:
        rows = []
        for row in body["data"]["result"]:
            metric = {key:value for key, value in row["metric"].items()
                      if key in ("app", "__variant__", "__tenant_id__")}
            points = row.get("values", [row.get("value")])
            rows.append({"metric":metric, "points":[[float(point[0]), point[1]] for point in points]})
        rows.sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        return {"samples":rows, "warnings":body.get("warnings", [])}
    except (KeyError, TypeError, ValueError, AttributeError, IndexError):
        return None


def tempo_pipeline_metrics_match(expected, actual):
    """These fixtures disable exemplars; keep labels, every point and its order."""
    def canonical(response):
        indexed = {}
        for series in response["series"]:
            labels = sorted(series["labels"], key=lambda label: json.dumps(label, sort_keys=True))
            key = json.dumps(labels, sort_keys=True)
            if key in indexed or series.get("exemplars", []):
                raise ValueError("duplicate series or unexpected exemplar")
            points = []
            for point in series.get("samples", []):
                timestamp, value = point.get("timestampMs", 0), point.get("value", 0.0)
                if type(timestamp) not in (int, str) or type(value) not in (int, float):
                    raise ValueError("invalid timestamp or sample value")
                points.append((int(timestamp), float(value)))
            indexed[key] = points
        return indexed
    try:
        left, right = canonical(expected), canonical(actual)
        return (left.keys() == right.keys()
                and all(len(points) == len(right[key]) and all(
                    timestamp == actual_timestamp and (value == actual_value
                        or math.isnan(value) and math.isnan(actual_value)
                        or math.isfinite(value) and math.isfinite(actual_value)
                           and abs(value - actual_value) < 2.220446049250313e-16)
                    for (timestamp, value), (actual_timestamp, actual_value) in zip(points, right[key]))
                    for key, points in left.items()))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def tempo_array_metric_binding_errors(report):
    """Bind the public frontend's actual nil label to its complete fixture grid."""
    if not isinstance(report, dict) or not isinstance(report.get("cases"), list):
        return ["Tempo expression output report lacks cases"]
    rows = [row for row in report["cases"] if isinstance(row, dict)
            and row.get("stableid") == "tempo-live-output-array-metric-label"]
    if len(rows) != 1:
        return ["Tempo array metric frontend witness must occur exactly once"]
    row = rows[0]
    query = '{ resource.service.name = "checkout" && span.numbers != nil } | count_over_time() by(span.numbers) with(exemplars=false)'
    request = row.get("request")
    if row.get("query") != query or not isinstance(request, dict) or request.get("step") != "30s":
        return ["Tempo array metric witness differs from its existence predicate, grouping or step"]
    try:
        bounds = urllib.parse.parse_qs(request["range"], strict_parsing=True)
        if set(bounds) != {"start", "end"} or any(len(values) != 1 for values in bounds.values()):
            raise ValueError("duplicate or additional bounds")
        start, end = int(bounds["start"][0]), int(bounds["end"][0])
        if start < 0 or end - start != 180 or end * 1000 > 2**63 - 1:
            raise ValueError("invalid fixture window")
    except (KeyError, TypeError, ValueError, AttributeError):
        return ["Tempo array metric witness lacks its valid 180-second request window"]
    # The three checkout spans occur at anchor and anchor+1 second. The public
    # count grid is anchored at request start+60, with the latter pair in +30s.
    anchor = start + 60
    complete = {"series":[{
        "labels":[{"key":"span.numbers", "value":{"stringValue":"nil"}}],
        "samples":[{"timestampMs":str((anchor + index * 30) * 1000),
                    "value":1.0 if index == 0 else 2.0 if index == 1 else 0.0}
                   for index in range(-2, 5)],
        "exemplars":[],
    }]}
    legend = '{"span.numbers"="<nil>"}'
    ledger = {"label_key":"span.numbers", "label_values":[{"stringValue":"nil"}],
              "prom_labels":[legend], "group_count":1, "each_total":3.0,
              "complete_series":complete}
    errors = []
    if not EVIDENCE_TYPED_EQUAL(row.get("independent_expected"), ledger):
        errors.append("Tempo array metric ledger differs from independently derived nil-label grid")
    for side in ("upstream", "krabka"):
        actual = row.get(side)
        if not tempo_pipeline_metrics_match(complete, actual):
            errors.append(f"Tempo array metric {side} differs from actual typed nil labels and complete count grid")
            continue
        # promLabels is optional on the public wire. When supplied, nil's
        # Static.String legend is <nil>, distinct from AsAnyValue's string nil.
        series = actual["series"][0]
        if "promLabels" in series and series["promLabels"] != legend:
            errors.append(f"Tempo array metric {side} legend differs from independent label identity")
    return errors


def tempo_instant_requests(anchor):
    end = anchor + 90
    return {
        "default-since": f"end={end}",
        "empty-start": f"start=&end={end}",
        "explicit-since": f"end={end}&since=2m",
        "ignored-time": f"end={end}&time=bogus",
        "nanoseconds": f"end={end}123456789",
        "fractional-seconds": f"end={end}.123456789",
        "default-clock": "time=bogus",
        "start-only": f"start={anchor - 60}",
        "short-since": f"end={end}&since=30s",
        "rate": f"end={end}&since=2m",
    }


def tempo_instant_binding_errors(report):
    errors = []
    rows = report.get("cases", [])
    if ({row.get("stableid") for row in rows} != {f"tempo-instant-{name}" for name in tempo_instant_requests(0)}
            or len(rows) != 10):
        errors.append("Tempo instant bounds witness identities differ")
    for row in rows:
        try:
            anchor = row["fixture_anchor"]
            if type(anchor) is not int or anchor < 60 or anchor * 1000 > 2**63 - 100000:
                raise ValueError("invalid anchor")
            name = row["stableid"].removeprefix("tempo-instant-")
            params = tempo_instant_requests(anchor)[name]
            operation = "rate" if name == "rate" else "count_over_time"
            query = f'{{ resource.service.name = "checkout" }} | {operation}() with(exemplars=false)'
            value = 3.0 / 120.0 if name == "rate" else 3.0
            expected = [{"labels":[{"key":"__name__","value":{"stringValue":operation}}]}] if name == "short-since" else [{
                "labels":[{"key":"__name__","value":{"stringValue":operation}}],"value":value}]
            if (row["query"] != query or row["request"] != {"params":params}
                    or not EVIDENCE_TYPED_EQUAL(row["independent_expected"], expected)):
                raise ValueError("request or independent scalar ledger differs")
            for side in ("upstream", "krabka"):
                observed = row[side]
                before, after = observed["clock_ms"]
                if (observed["error"] is not None or type(before) is not int
                        or type(after) is not int or before > after):
                    raise ValueError("invalid execution clock or error")
                # InstantSeries has only labels and value. In particular, its
                # removed prom_labels field and range samples are not retained.
                series = observed["body"].get("series", [])
                if not isinstance(series, list) or len(series) != len(expected):
                    raise ValueError("instant series count differs from independent window ledger")
                for actual, wanted in zip(series, expected):
                    if (set(actual) != set(wanted)
                            or not EVIDENCE_TYPED_EQUAL(actual["labels"], wanted["labels"])
                            or type(actual.get("value", 0.0)) not in (int, float)
                            or not math.isfinite(actual.get("value", 0.0))
                            or abs(actual.get("value", 0.0) - wanted.get("value", 0.0)) >= 2.220446049250313e-16):
                        raise ValueError("complete instant scalar differs from independent span/window ledger")
        except (KeyError, TypeError, ValueError, AttributeError, IndexError) as error:
            errors.append(f"Tempo instant witness invalid: {error}")
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
    errors.extend(experimental_query_binding_errors(name, report))
    if name == "tempo-instant-bounds-conformance.json":
        errors.extend(tempo_instant_binding_errors(report))
    if name == "tempo-expression-output-conformance.json":
        errors.extend(tempo_array_metric_binding_errors(report))
    if name == "loki-supported-template-functions.json":
        errors.extend(loki_template_binding_errors(report))
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
    anchor = 1700000000
    instant_rows = []
    for name, params in tempo_instant_requests(anchor).items():
        clock = (anchor + 180) * 1000
        operation = "rate" if name == "rate" else "count_over_time"
        value = 3.0 / 120.0 if name == "rate" else 3.0
        expected = [{"labels":[{"key":"__name__","value":{"stringValue":operation}}]}] if name == "short-since" else [{
            "labels":[{"key":"__name__","value":{"stringValue":operation}}],"value":value}]
        observation = {"body":{"series":copy.deepcopy(expected)},
                       "clock_ms":[clock - 1, clock + 1],"error":None}
        instant_rows.append({"stableid":f"tempo-instant-{name}",
            "query":f'{{ resource.service.name = "checkout" }} | {operation}() with(exemplars=false)',
            "request":{"params":params},"fixture_anchor":anchor,"independent_expected":expected,
            "upstream":copy.deepcopy(observation),"krabka":copy.deepcopy(observation),"status":"matched"})
    instant_report = {"planned":10,"cases":instant_rows}
    assert not tempo_instant_binding_errors(instant_report)
    for mutation in ("both-counts", "range-samples", "wrong-label", "rate-window", "short-window", "params", "ledger", "missing"):
        altered = copy.deepcopy(instant_report)
        row = altered["cases"][0]
        if mutation == "both-counts":
            for side in ("upstream", "krabka"):
                row[side]["body"]["series"][0]["value"] = 4
        elif mutation == "range-samples":
            row["krabka"]["body"]["series"][0]["samples"] = [{"timestampMs":"1","value":3}]
        elif mutation == "wrong-label":
            row["upstream"]["body"]["series"][0]["labels"][0]["value"] = {"stringValue":"rate"}
        elif mutation == "rate-window":
            altered["cases"][9]["krabka"]["body"]["series"][0]["value"] = 3.0 / (120.0 + 1e-9)
        elif mutation == "short-window":
            altered["cases"][8]["upstream"]["body"]["series"] = copy.deepcopy(row["upstream"]["body"]["series"])
        elif mutation == "params":
            row["request"]["params"] += "&start=1"
        elif mutation == "ledger":
            row["independent_expected"][0]["value"] = 4
        else:
            altered["cases"].pop()
        assert tempo_instant_binding_errors(altered), mutation
    array_metrics = {"series":[{
        "labels":[{"key":"span.numbers", "value":{"stringValue":"nil"}}],
        "samples":[{"timestampMs":str(timestamp * 1000), "value":value}
                   for timestamp, value in [(300, 0.0), (330, 0.0), (360, 1.0),
                                            (390, 2.0), (420, 0.0), (450, 0.0), (480, 0.0)]],
        "exemplars":[],
    }]}
    array_report = {"cases":[{
        "stableid":"tempo-live-output-array-metric-label",
        "query":'{ resource.service.name = "checkout" && span.numbers != nil } | count_over_time() by(span.numbers) with(exemplars=false)',
        "request":{"range":"start=300&end=480", "step":"30s"},
        "independent_expected":{"label_key":"span.numbers", "label_values":[{"stringValue":"nil"}],
                                "prom_labels":['{"span.numbers"="<nil>"}'], "group_count":1,
                                "each_total":3.0, "complete_series":copy.deepcopy(array_metrics)},
        "upstream":copy.deepcopy(array_metrics), "krabka":copy.deepcopy(array_metrics),
    }]}
    assert not tempo_array_metric_binding_errors(array_report)
    array_report["cases"][0]["krabka"]["series"][0]["promLabels"] = '{"span.numbers"="<nil>"}'
    assert not tempo_array_metric_binding_errors(array_report)
    for mutation in ("missing-case", "duplicate-case", "predicate", "step", "window", "duplicate-bound",
                     "missing-series", "duplicate-series", "wrong-label", "array-label", "missing-point",
                     "duplicate-point", "wrong-grid", "cancelling-values", "exemplar", "legend", "null-legend",
                     "ledger-and-responses-wrong-counts"):
        altered = copy.deepcopy(array_report)
        row = altered["cases"][0]
        series = row["krabka"]["series"][0]
        if mutation == "missing-case":
            altered["cases"] = []
        elif mutation == "duplicate-case":
            altered["cases"].append(copy.deepcopy(row))
        elif mutation == "predicate":
            row["query"] = row["query"].replace("span.numbers != nil", "span.numbers = nil")
        elif mutation == "step":
            row["request"]["step"] = "60s"
        elif mutation == "window":
            row["request"]["range"] = "start=300&end=450"
        elif mutation == "duplicate-bound":
            row["request"]["range"] += "&start=300"
        elif mutation == "missing-series":
            row["krabka"]["series"] = []
        elif mutation == "duplicate-series":
            row["krabka"]["series"].append(copy.deepcopy(series))
        elif mutation == "wrong-label":
            series["labels"][0]["value"] = {"stringValue":"<nil>"}
        elif mutation == "array-label":
            series["labels"][0]["value"] = {"arrayValue":{"values":[{"intValue":"1"}]}}
        elif mutation == "missing-point":
            series["samples"].pop()
        elif mutation == "duplicate-point":
            series["samples"][1] = copy.deepcopy(series["samples"][0])
        elif mutation == "wrong-grid":
            series["samples"][0]["timestampMs"] = "300001"
        elif mutation == "cancelling-values":
            series["samples"][2]["value"] = 2.0
            series["samples"][3]["value"] = 1.0
        elif mutation == "exemplar":
            series["exemplars"] = [{"value":1.0}]
        elif mutation in ("legend", "null-legend"):
            series["promLabels"] = '{"span.numbers"="nil"}' if mutation == "legend" else None
        else:
            row["independent_expected"]["each_total"] = 9.0
            for response in (row["independent_expected"]["complete_series"], row["upstream"], row["krabka"]):
                response["series"][0]["samples"][2]["value"] = 4.0
                response["series"][0]["samples"][3]["value"] = 5.0
        assert tempo_array_metric_binding_errors(altered), mutation
    corrupted_oracle = copy.deepcopy(array_report)
    corrupted_oracle["cases"][0]["upstream"]["series"][0]["labels"][0]["value"] = {"stringValue":"array"}
    assert tempo_array_metric_binding_errors(corrupted_oracle)
    metrics = {"series":[{"labels":[{"key":"name", "value":{"stringValue":"checkout"}}],
                         "samples":[{"timestampMs":"30", "value":2.0}], "exemplars":[]}]}
    report = {"upstream_source":{"repository":"grafana/tempo", "version":"3.0.3",
              "revision":"1900ed7bb5cad1a3edc285783d7d4ac4278337dc"}, "cases":[{
        "request":{"range":"start=0&end=60", "step":"30s"}, "expected_outcome":"query-result",
        "comparison_kind":"exact-whole-series", "independent_expected":metrics,
        "upstream":metrics, "krabka":metrics}]}
    assert not experimental_query_binding_errors("tempo-pipeline-hint-conformance.json", report)
    zero = copy.deepcopy(report)
    zero_metrics = {"series":[{"labels":[{"key":"__name__", "value":{"stringValue":"count_over_time"}}],
                              "samples":[{"timestampMs":str(timestamp * 1000), "value":0.0}
                                         for timestamp in range(0, 181, 30)], "exemplars":[]}]}
    zero["cases"][0].update(stableid="tempo-live-pipeline-scalar-before-metrics-negative",
                             request={"range":"start=0&end=180", "step":"30s"},
                             independent_expected=zero_metrics, upstream=zero_metrics, krabka=zero_metrics)
    assert not experimental_query_binding_errors("tempo-pipeline-hint-conformance.json", zero)
    for mutation in ("empty", "missing-bucket", "nonzero", "wrong-label"):
        altered = copy.deepcopy(zero)
        changed = altered["cases"][0]["independent_expected"]
        if mutation == "empty":
            changed["series"] = []
        elif mutation == "missing-bucket":
            changed["series"][0]["samples"].pop()
        elif mutation == "nonzero":
            changed["series"][0]["samples"][0]["value"] = 1.0
        else:
            changed["series"][0]["labels"][0]["value"] = {"stringValue":"rate"}
        altered["cases"][0].update(upstream=changed, krabka=changed)
        assert experimental_query_binding_errors("tempo-pipeline-hint-conformance.json", altered)
    assert tempo_pipeline_metrics_match(metrics, metrics)
    for field, wrong in [("upstream_source", "wrong"), ("comparison_kind", "unbounded"),
                         ("expected_outcome", "query-rejection"), ("independent_expected", {"series":[]})]:
        altered = copy.deepcopy(report)
        (altered if field == "upstream_source" else altered["cases"][0])[field] = wrong
        assert experimental_query_binding_errors("tempo-pipeline-hint-conformance.json", altered)
    for path, wrong in [("timestampMs", "31"), ("value", 3.0)]:
        altered = copy.deepcopy(metrics)
        altered["series"][0]["samples"][0][path] = wrong
        assert not tempo_pipeline_metrics_match(metrics, altered)
    altered = copy.deepcopy(metrics)
    altered["series"][0]["labels"][0]["value"] = {"intValue":"2"}
    assert not tempo_pipeline_metrics_match(metrics, altered)
    sampled = copy.deepcopy(report)
    sampled["cases"][0].update(comparison_kind="independent-sampling-domain",
                               independent_expected={"allowed_complete_outputs":[metrics]})
    assert not experimental_query_binding_errors("tempo-pipeline-hint-conformance.json", sampled)
    sampled["cases"][0]["krabka"] = {"series":[]}
    assert experimental_query_binding_errors("tempo-pipeline-hint-conformance.json", sampled)
    average = copy.deepcopy(report)
    average_metrics = copy.deepcopy(metrics)
    average_metrics["series"][0]["samples"][0]["value"] = 7.0
    average["cases"][0].update(comparison_kind="independent-sampling-domain",
                               independent_expected={"allowed_complete_outputs":[average_metrics]},
                               upstream=average_metrics, krabka=copy.deepcopy(average_metrics))
    assert not experimental_query_binding_errors("tempo-pipeline-hint-conformance.json", average)
    average["cases"][0]["krabka"]["series"][0]["samples"][0]["value"] *= 9.0 / 5.0
    assert experimental_query_binding_errors("tempo-pipeline-hint-conformance.json", average)
    body = {"status":"success", "data":{"resultType":"vector", "result":[{
        "metric":{"app":"api", "service_name":"api"}, "value":[40, "3"]}]}}
    ledger = {"samples":[{"metric":{"app":"api"}, "points":[[40.0, "3"]]}], "warnings":[]}
    row = {"request":{"method":"GET", "path":"/loki/api/v1/query"}, "expected_outcome":"query-result",
           "expected_status":200, "independent_expected":ledger,
           "oracle":loki_experimental_normalized(body), "candidate":loki_experimental_normalized(body),
           "oracle_semantic":ledger, "candidate_semantic":ledger,
           "raw_oracle":{"status":200, "body":body}, "raw_candidate":{"status":200, "body":body}}
    report = {"upstream_source":"7a40404f32b3e6464c9cfc6cc7dd75a40f3931da", "cases":[row]}
    assert not experimental_query_binding_errors("loki-experimental-query-conformance.json", report)
    discovery = copy.deepcopy(report)
    discovered = {"status":"success", "data":{"resultType":"vector", "result":[{"metric":{}, "value":[40, "1"]}]}}
    changed = discovery["cases"][0]
    changed.update(id="discovery/custom-logfmt-field", request={"params":[["time", "40"]]},
                   independent_expected=loki_experimental_semantic(discovered))
    for side in ("oracle", "candidate"):
        changed[side] = loki_experimental_normalized(discovered)
        changed[side + "_semantic"] = changed["independent_expected"]
        changed["raw_" + side] = {"status":200, "body":discovered}
    assert not experimental_query_binding_errors("loki-experimental-query-conformance.json", discovery)
    nanos_discovery = copy.deepcopy(discovery)
    nanos_case = nanos_discovery["cases"][0]
    nanos_case["request"]["params"] = [["time", "1791287680000000000"]]
    nanos_case["independent_expected"]["samples"][0]["points"][0][0] = 1791287680.0
    for side in ("oracle", "candidate"):
        nanos_case[side]["data"]["result"][0]["value"][0] = 1791287680
        nanos_case["raw_" + side]["body"]["data"]["result"][0]["value"][0] = 1791287680
    assert not experimental_query_binding_errors("loki-experimental-query-conformance.json", nanos_discovery)
    for wrong in ("1791287681000000000", "nan", "", "9" * 500):
        altered = copy.deepcopy(nanos_discovery)
        altered["cases"][0]["request"]["params"] = [["time", wrong]]
        assert experimental_query_binding_errors("loki-experimental-query-conformance.json", altered)
    changed["independent_expected"]["samples"][0]["points"][0][1] = "2"
    for side in ("oracle", "candidate"):
        changed[side]["data"]["result"][0]["value"][1] = "2.000000"
        changed["raw_" + side]["body"]["data"]["result"][0]["value"][1] = "2"
    assert experimental_query_binding_errors("loki-experimental-query-conformance.json", discovery)
    for mutation in ("raw-count", "claimed-count", "raw-status", "non-ledger-label"):
        altered = copy.deepcopy(report)
        changed = altered["cases"][0]
        if mutation == "raw-count":
            changed["raw_candidate"]["body"]["data"]["result"][0]["value"][1] = "4"
        elif mutation == "claimed-count":
            changed["independent_expected"]["samples"][0]["points"][0][1] = "4"
        elif mutation == "non-ledger-label":
            changed["raw_candidate"]["body"]["data"]["result"][0]["metric"]["service_name"] = "corrupted"
        else:
            changed["raw_candidate"]["status"] = 500
        assert experimental_query_binding_errors("loki-experimental-query-conformance.json", altered)
    complete_artifacts = [{"name": name} for name in REQUIRED]
    complete_artifacts += [{"name": "promql-3.14.0-qualification.json", "configuration": mode}
                           for mode in ("default", "experimental")]
    assert not missing_suites(complete_artifacts, REQUIRED_TARGETS)
    assert "promql-full-experimental" in missing_suites(complete_artifacts[:-1], REQUIRED_TARGETS)
    profile_target = "//crates/profiles:pyroscope_deployment_docker_test"
    assert profile_target in missing_suites(complete_artifacts, REQUIRED_TARGETS - {profile_target})
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
    curated = {"suite": "diff_mimir", "run": 1782, "skipped": 18, "cases": [
        *[{"id": f"executed:{ordinal}", "status": "matched"} for ordinal in range(1781)],
        {"id": "declared-version-difference", "status": "expected_divergence"},
        *[{"id": case_id, "status": "skipped", "detail": "declared adapter exclusion"} for case_id in sorted(CURATED_ADAPTER_EXCLUSIONS)],
    ]}
    counts, errors = qualification_counts("diff_mimir-report.json", curated)
    assert not errors and counts == {"matched": 1781, "expected-divergence": 1, "adapter-excluded": 18}
    assert not is_complete(counts)
    assert valid_execution_evidence(dict(bound, artifacts=[{"counts": counts}]), allow_partial=True)
    for change in ("extra-exclusion", "missing-case", "duplicate-case", "missing-reason"):
        altered = copy.deepcopy(curated)
        if change == "extra-exclusion":
            altered["cases"][0].update(status="skipped", detail="unexpected omission")
            altered.update(run=1781, skipped=19)
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
    assert counts == {"matched": 1, "uncovered": 112} and not is_complete(counts)
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
                           ("test.log", "executed fixture\n"), ("raw/report.json", "{}"),
                           ("raw/SHA256SUMS", "nested upstream manifest\n")]:
            (directory / name).write_text(body)
        files = sorted(path for path in directory.rglob("*") if path.is_file())
        manifest = "".join(f"{digest(path)}  ./{path.relative_to(directory)}\n" for path in files)
        (directory / "SHA256SUMS").write_text(manifest)
        assert not verify_manifest(directory)
        without_nested_manifest = "\n".join(line for line in manifest.splitlines()
                                              if not line.endswith("./raw/SHA256SUMS")) + "\n"
        (directory / "SHA256SUMS").write_text(without_nested_manifest)
        assert any("omitted" in error for error in verify_manifest(directory))
        (directory / "SHA256SUMS").write_text(manifest)
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
    template_rows = []
    for case_id, (expression, query, golden) in loki_template_cases().items():
        ledger = loki_template_expected(case_id, golden, 1_000_000_000)
        template_rows.append({"id":case_id, "name":case_id, "expression":expression, "request":{"method":"GET", "path":"/loki/api/v1/query_range", "tenant":"parsers", "query":query}, "oracle":ledger, "candidate":ledger, "independent_expected":ledger, "raw_oracle":ledger, "raw_candidate":ledger, "status":"matched"})
    template_report = {"planned":len(template_rows), "timeline_base_ns":1_000_000_000, "upstream_source":"7a40404f32b3e6464c9cfc6cc7dd75a40f3931da", "cases":template_rows}
    assert not loki_template_binding_errors(template_report)
    for mutation in ("duplicate", "query", "expression", "ledger", "http-float", "http-bool", "missing-seed", "duplicate-seed", "wrong-level", "wrong-timestamp", "wrong-raw-line", "missing-anchor", "float-anchor"):

        changed = copy.deepcopy(template_report)
        row = changed["cases"][0]
        if mutation == "duplicate":
            changed["cases"][-1] = copy.deepcopy(row)
        elif mutation in ("query", "expression"):
            if mutation == "query":
                row["request"]["query"] = "{}"
            else:
                row["expression"] = "missing"
        elif mutation == "ledger":
            row["independent_expected"]["body"]["data"]["result"][0]["values"][0][1] = "wrong"
        elif mutation in ("http-float", "http-bool"):
            row["raw_candidate"]["status"] = 200.0 if mutation == "http-float" else True
        elif mutation in ("missing-anchor", "float-anchor"):
            if mutation == "missing-anchor":
                del changed["timeline_base_ns"]
            else:
                changed["timeline_base_ns"] = 1_000_000_000.0
        else:
            if mutation == "wrong-raw-line":
                row = next(item for item in changed["cases"] if item["id"] == "line")
            ledger = row["independent_expected"]
            streams = ledger["body"]["data"]["result"]
            if mutation == "missing-seed":
                streams[1]["values"].pop()
            elif mutation == "duplicate-seed":
                streams[1]["values"][-1] = list(streams[1]["values"][0])
            elif mutation == "wrong-level":
                streams[0]["stream"]["detected_level"] = "unknown"
            elif mutation == "wrong-timestamp":
                streams[0]["values"][0][0] = str(int(streams[0]["values"][0][0]) + 1)
            else:
                streams[0]["values"][0][1] = "wrong"
            # Corrupt all three compared responses identically: pairwise
            # equality alone must never satisfy the independent fixture.
            row["oracle"] = copy.deepcopy(ledger)
            row["candidate"] = copy.deepcopy(ledger)
        assert loki_template_binding_errors(changed), mutation

    upstream = {"image_tag": image_pin[3], "image_id": image_pin[4]}
    revision = json.loads((ROOT / "qualification/query-language-inventory.json").read_text())["surfaces"]["pyroscope"]["pin"]["revision"]
    for name, settings in [
        ("pyroscope-v2-fields.json", {"oracle": {"query-frontend.async-queries-enabled": True}, "candidate": {"query_architecture": "v2", "async_queries_enabled": True}}),
        ("pyroscope-v1-aggregation.json", {"query_architecture": "v1", "oracle_async_enabled": False, "candidate_async_enabled": False,
                    "oracle_query_analysis_series_enabled": False, "candidate_query_analysis_series_enabled": False}),
        ("pyroscope-v2-async-disabled.json", {"query_architecture": "v2", "oracle_async_enabled": False, "candidate_async_enabled": False,
                    "oracle_query_analysis_series_enabled": False, "candidate_query_analysis_series_enabled": False}),
        ("pyroscope-populated-rpcs.json", None),
    ]:
        report = {"suite": name.removesuffix(".json"), "status": "passed", "upstream": dict(upstream), "upstream_revision": revision, "storage": "v2", "settings": settings}
        if settings is None:
            report["upstream"].update(source_revision=revision, storage="v1", query_analysis_series_enabled=True, candidate_query_analysis_series_enabled=True, self_profiling_disable_push=True)
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
    targets = set()

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
                if not integrity_errors[str(binding.parent)]:
                    targets.add((binding.parent / "target").read_text().strip())
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
            configuration = None
            if path.name == "promql-3.14.0-qualification.json":
                experimental = report.get("experimental_functions")
                configuration = "experimental" if experimental is True else "default"
                try:
                    target = (binding_directory / "target").read_text().strip() if binding_directory else None
                except OSError:
                    target = None
                if (type(experimental) is not bool or target != PROMQL_TARGETS.get(experimental)
                        or report.get("enable_type_and_unit_labels") is not False):
                    errors.append("full PromQL configuration differs from selected suite")
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
                namespace = f"storage/transitions/{path.stem}" if path.name.endswith("-query-transitions.json") else f"traceql/metrics/{path.stem}" if metric_result or path.name.startswith("tempo-numeric-") or path.name.startswith("tempo-live-exemplar-") or path.name.startswith("tempo-typed-group-") or path.name.startswith("tempo-field-arithmetic-") else "pyroscope/v2" if path.name == "pyroscope-v2-fields.json" else "pyroscope/v1-aggregation" if path.name == "pyroscope-v1-aggregation.json" else "pyroscope/v2-async-disabled" if path.name == "pyroscope-v2-async-disabled.json" else {"loki-experimental-query-conformance.json":"logql/experimental", "tempo-pipeline-hint-conformance.json":"traceql/pipeline", "tempo-instant-bounds-conformance.json":"traceql/instant", "diff_mimir-report.json":"promql/mimir", "diff_prometheus-report.json":"promql/prometheus", "backup-query-invariance.json":"storage/restore"}.get(path.name, "pyroscope/rpc")
                for ordinal, case in enumerate(report["cases"], 1):
                    identity = case.get("stableid", case.get("id", ordinal))
                    reference = f"{namespace}/{identity}"
                    record_executed(reference, case_verdict(case), case, path.name, report, path)
                    if case.get("expected_rejection") or case.get("classification") == "paired-expected-error":
                        rejected_cases.add(reference)
            artifacts.append({"path": str(path), "name": path.name, "sha256": digest(path),
                              "configuration": configuration,
                              "counts": counts, "discovered": sum(counts.values()),
                              "validation_errors": errors,
                              "complete_corpus": not errors and is_complete(counts)})
    missing = missing_suites(artifacts, targets)
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
        "source_availability": {language: {feature["id"]: feature["availability"]
                                             for feature in surface["features"]}
                                for language, surface in inventory["surfaces"].items()},
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
