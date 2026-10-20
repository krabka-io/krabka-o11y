"""Validate Grafana Prometheus client replay against an independent fixture."""

import copy
import json
import math
import re
import urllib.parse

LABELS = {"__name__": "http_requests_total", "job": "api", "method": "GET", "code": "200"}
RATE_LABELS = {key: value for key, value in LABELS.items() if key != "__name__"}
SELECTOR = 'http_requests_total{method="GET"}'
RATE = 'rate(http_requests_total{method="GET"}[30s])'
CASES = {
    "instant-selector": (SELECTOR, False, 45000, [120.0], LABELS),
    "instant-sum": ("sum(http_requests_total)", False, 45000, [128.0], {}),
    "instant-rate": (RATE, False, 45000, [3.0], RATE_LABELS),
    "range-selector": (SELECTOR, True, 0, [0.0, 30.0, 75.0, 120.0], LABELS),
    "range-sum": ("sum(http_requests_total)", True, 0, [0.0, 33.0, 80.0, 128.0], {}),
    # Zero extrapolation caps the left padding at ten seconds at t=30.
    "range-rate": (RATE, True, 30000, [2.5, 3.0], RATE_LABELS),
}
FIXTURE = {"timestamps_ms": [0, 15000, 30000, 45000], "GET": [0, 30, 75, 120], "POST": [0, 3, 5, 8]}


def typed_equal(left, right):
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(typed_equal(value, right[key]) for key, value in left.items())
    if isinstance(left, list):
        return len(left) == len(right) and all(typed_equal(a, b) for a, b in zip(left, right))
    return left == right


def number(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("non-finite or nonnumeric sample")
    return value


def ledger(case):
    _, _, start, values, labels = case
    return {"labels": labels, "samples": [[start + index * 15000, value] for index, value in enumerate(values)]}


def query_response(case, response):
    _, is_range, _, _, _ = case
    if response["status"] != "success" or "warnings" in response or "infos" in response:
        raise ValueError("backend query did not succeed without annotations")
    result = response["data"]["result"]
    if response["data"]["resultType"] != ("matrix" if is_range else "vector") or len(result) != 1:
        raise ValueError("backend result type or series count differs")
    points = result[0]["values"] if is_range else [result[0]["value"]]
    samples = []
    for point in points:
        if len(point) != 2 or type(point[1]) is not str:
            raise ValueError("backend sample tuple differs")
        samples.append([number(point[0]) * 1000, number(float(point[1]))])
    return {"labels": result[0]["metric"], "samples": samples}


def equal_ledger(expected, actual):
    if not typed_equal(expected["labels"], actual["labels"]) or len(expected["samples"]) != len(actual["samples"]):
        return False
    return all(len(point) == 2 and all(number(a) == number(b) for a, b in zip(golden, point))
               for golden, point in zip(expected["samples"], actual["samples"]))


def frame_response(response, expected):
    result = response["body"]["results"]["A"]
    frames = result["frames"]
    if response["status"] != 200 or "error" in result or len(frames) != 1:
        raise ValueError("Grafana query did not return one successful frame")
    frame = frames[0]
    fields, columns = frame["schema"]["fields"], frame["data"]["values"]
    if len(fields) != 2 or len(columns) != 2:
        raise ValueError("Grafana frame column count differs")
    time_index = next(index for index, field in enumerate(fields) if field["type"] == "time")
    value_index = next(index for index, field in enumerate(fields) if field["type"] == "number")
    if len(columns[time_index]) != len(columns[value_index]):
        raise ValueError("Grafana frame timestamp/value lengths differ")
    actual = {"labels": fields[value_index].get("labels", {}),
              "samples": list(map(list, zip(columns[time_index], columns[value_index])))}
    if not equal_ledger(expected, actual):
        raise ValueError("Grafana frame differs from independent ledger")
    meta = copy.deepcopy(frame["schema"].get("meta", {}))
    meta.pop("stats", None)
    meta.pop("executedQueryString", None)
    return {"fields": fields, "data": frame["data"], "meta": meta}


def binding_errors(report, root):
    try:
        rows = report["cases"]
        if (report["suite"] != "grafana-prometheus-client-replay" or report["status"] != "passed"
                or type(report["planned"]) is not int or report["planned"] != len(CASES)
                or len(rows) != len(CASES) or {row["id"] for row in rows} != set(CASES)
                or not typed_equal(report["fixture"], FIXTURE)):
            raise ValueError("incomplete client case set or changed input fixture")
        pin = re.search(r'\("prometheus", "[^"]+", "[^"]+", "([^"]+)"', (root / "MODULE.bazel").read_text())
        if pin is None or report.get("prometheus_image_tag") != pin[1]:
            raise ValueError("Prometheus client oracle differs from pin")
        for row in rows:
            case = CASES[row["id"]]
            expression, is_range, start, _, _ = case
            expected = ledger(case)
            if row["expression"] != expression or row["status"] != "matched" or row.get("error") is not None:
                raise ValueError("client execution did not match source query")
            if not typed_equal(expected, row["independent_expected"]):
                raise ValueError("independent ledger differs from input arithmetic")
            for side, uid in (("oracle", "real-prom"), ("candidate", "krabka-prom")):
                request = {"from": str(start), "to": "45000", "queries": [{
                    "refId": "A", "datasource": {"type": "prometheus", "uid": uid}, "expr": expression,
                    "instant": not is_range, "range": is_range, "queryType": "range" if is_range else "instant",
                    "intervalMs": 15000, "maxDataPoints": 1000}]}
                if not typed_equal(request, row["dashboard_requests"][side]):
                    raise ValueError("dashboard input differs from source fixture")
            requests = row["generated_requests"]
            if len(requests["oracle"]) != 1 or not typed_equal(requests["oracle"], requests["candidate"]):
                raise ValueError("plugin requests differ between datasources")
            request = requests["oracle"][0]
            path = "/api/v1/query_range" if is_range else "/api/v1/query"
            if (request["method"] != "POST" or request["uri"] != path or request["tenant"] != "grafana"
                    or request["content_type"] != "application/x-www-form-urlencoded"):
                raise ValueError("generated method/path/tenant/content type differs")
            params = urllib.parse.parse_qs(request["body"], keep_blank_values=True, strict_parsing=True)
            expected_params = {"query": [expression]}
            if is_range:
                expected_params.update(start=[str(start // 1000)], end=["45"], step=["15"])
            else:
                expected_params["time"] = ["45"]
            if params != expected_params:
                raise ValueError("generated expression/time/step differs")
            for side in ("oracle", "candidate"):
                if not equal_ledger(expected, query_response(case, row[side])):
                    raise ValueError("backend answer differs from independent ledger")
            oracle_frame = frame_response(row["grafana_responses"]["oracle"], expected)
            candidate_frame = frame_response(row["grafana_responses"]["candidate"], expected)
            if not typed_equal(oracle_frame, candidate_frame):
                raise ValueError("Grafana frame schema, values or semantic metadata differ")
    except (AttributeError, KeyError, TypeError, ValueError, IndexError, StopIteration, OverflowError) as error:
        return [f"Grafana Prometheus client replay invalid: {error}"]
    return []


def self_check(root):
    rows = []
    for identity, case in CASES.items():
        expression, is_range, start, values, labels = case
        inputs = {side: {"from": str(start), "to": "45000", "queries": [{
            "refId": "A", "datasource": {"type": "prometheus", "uid": uid}, "expr": expression,
            "instant": not is_range, "range": is_range, "queryType": "range" if is_range else "instant",
            "intervalMs": 15000, "maxDataPoints": 1000}]}
                  for side, uid in (("oracle", "real-prom"), ("candidate", "krabka-prom"))}
        params = {"query": expression, **({"start": start // 1000, "end": 45, "step": 15} if is_range else {"time": 45})}
        request = {"method": "POST", "uri": "/api/v1/query_range" if is_range else "/api/v1/query", "tenant": "grafana",
                   "body": urllib.parse.urlencode(params), "content_type": "application/x-www-form-urlencoded"}
        points = [[(start + index * 15000) / 1000, str(value)] for index, value in enumerate(values)]
        answer = {"status": "success", "data": {"resultType": "matrix" if is_range else "vector",
                  "result": [{"metric": labels, "values" if is_range else "value": points if is_range else points[0]}]}}
        frame = {"status": 200, "body": {"results": {"A": {"frames": [{"schema": {"fields": [
            {"name": "Time", "type": "time"}, {"name": "Value", "type": "number", "labels": labels}]},
            "data": {"values": [[start + index * 15000 for index in range(len(values))], values]}}]}}}}
        rows.append({"id": identity, "expression": expression, "status": "matched", "independent_expected": ledger(case),
            "dashboard_requests": inputs, "generated_requests": {side: [copy.deepcopy(request)] for side in ("oracle", "candidate")},
            "oracle": copy.deepcopy(answer), "candidate": copy.deepcopy(answer),
            "grafana_responses": {side: copy.deepcopy(frame) for side in ("oracle", "candidate")}})
    pin = re.search(r'\("prometheus", "[^"]+", "[^"]+", "([^"]+)"', (root / "MODULE.bazel").read_text())[1]
    report = {"suite": "grafana-prometheus-client-replay", "status": "passed", "planned": 6,
              "prometheus_image_tag": pin, "fixture": FIXTURE, "cases": rows}
    assert not binding_errors(report, root)
    for mutation in ("missing", "duplicate", "request", "tenant", "bounds", "annotation", "both-wrong", "frame", "both-frames", "type", "fixture", "rate", "nan"):
        changed = copy.deepcopy(report)
        row = changed["cases"][0]
        if mutation == "missing": changed["cases"].pop()
        elif mutation == "duplicate": changed["cases"][-1] = copy.deepcopy(row)
        elif mutation in ("request", "tenant", "bounds"):
            for side in ("oracle", "candidate"):
                request = row["generated_requests"][side][0]
                request[{"request": "method", "tenant": "tenant", "bounds": "body"}[mutation]] = "wrong"
        elif mutation == "annotation": row["oracle"]["warnings"] = ["changed warning"]
        elif mutation in ("both-wrong", "nan"):
            for side in ("oracle", "candidate"):
                row[side]["data"]["result"][0]["value"][1] = "121" if mutation == "both-wrong" else "NaN"
        elif mutation in ("frame", "both-frames", "type"):
            for side in (("candidate",) if mutation == "frame" else ("oracle", "candidate")):
                frame = row["grafana_responses"][side]["body"]["results"]["A"]["frames"][0]
                if mutation == "type": frame["schema"]["fields"][1]["type"] = "string"
                else: frame["data"]["values"][1][0] = 121
        elif mutation == "fixture": changed["fixture"]["GET"][1] = 31
        else:
            row = changed["cases"][-1]
            row["independent_expected"]["samples"][0][1] = 3.0
        assert binding_errors(changed, root), mutation
