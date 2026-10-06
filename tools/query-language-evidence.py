#!/usr/bin/env python3
"""Verify reviewed, source-bound feature witnesses; never infer coverage from text.

The registry lists exact executable cases and a reason for each evidence role.
Source hashes and case identities bind those reviews to the current fixtures.
Mappings establish a test exists; only bound execution reports establish it passed.
"""

import argparse
import copy
import hashlib
import json
import pathlib
import re
import runpy
import sys


ROOT = pathlib.Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "qualification/query-language-evidence.json"
INVENTORY = ROOT / "qualification/query-language-inventory.json"
ROLES = ("positive", "negative", "composition")
OUTCOMES = {"query-result", "query-rejection", "semantic-negative"}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_registry(registry, inventory):
    if registry.get("schema_version") != 1:
        raise ValueError("unexpected feature evidence registry schema")
    cases = registry.get("cases")
    mappings = registry.get("features")
    if not isinstance(cases, dict) or not isinstance(mappings, dict):
        raise ValueError("registry cases and features must be objects")
    feature_ids = {f"{language}/{feature['id']}"
                   for language, surface in inventory["surfaces"].items()
                   for feature in surface["features"]}
    for reference, case in cases.items():
        if not isinstance(reference, str) or not reference:
            raise ValueError("empty evidence case reference")
        if case.get("language") not in inventory["surfaces"]:
            raise ValueError(f"unknown evidence language: {reference}")
        report = case.get("report")
        if not isinstance(report, str) or pathlib.Path(report).name != report or not report.endswith(".json"):
            raise ValueError(f"invalid report name: {reference}")
        if case.get("expected_outcome") not in OUTCOMES:
            raise ValueError(f"missing explicit case outcome: {reference}")
        source = case.get("source", {})
        relative = source.get("path")
        if not isinstance(relative, str):
            raise ValueError(f"missing source path: {reference}")
        path = (ROOT / relative).resolve()
        if not path.is_relative_to(ROOT.resolve()) or not path.is_file():
            raise ValueError(f"invalid evidence source path: {reference}")
        if source.get("sha256") != digest(path):
            raise ValueError(f"evidence source changed; review mapping: {reference}")
        identity = case.get("identity")
        if not isinstance(identity, dict) or not identity:
            raise ValueError(f"missing exact runtime case identity: {reference}")
        if (not any(key in identity for key in ("query", "expression"))
                and not all(key in identity for key in ("method", "name"))
                and not (isinstance(identity.get("id"), str) and isinstance(identity.get("request"), dict))):
            raise ValueError(f"case needs exact query or RPC/name identity: {reference}")
        exact = case.get("exact_case_values", {})
        if not isinstance(exact, dict) or any(not isinstance(field, str) or not field for field in exact):
            raise ValueError(f"invalid exact semantic field guards: {reference}")
        for support in source.get("supporting_sources", []):
            support_path = (ROOT / support["path"]).resolve()
            if not support_path.is_relative_to(ROOT.resolve()) or not support_path.is_file() or support.get("sha256") != digest(support_path):
                raise ValueError(f"supporting grammar/comparator source changed: {reference}")
        if "source_header" in source:
            headers = [line.strip() for line in path.read_text().splitlines()
                       if line.lstrip().startswith(("eval ", "eval_fail "))]
            ordinal = source.get("ordinal")
            if type(ordinal) is not int or ordinal < 1 or ordinal > len(headers) or headers[ordinal - 1] != source["source_header"]:
                raise ValueError(f"source evaluation moved or changed: {reference}")
            if not headers[ordinal - 1].endswith(" " + identity["query"]):
                raise ValueError(f"source query differs from runtime identity: {reference}")
    used = set()
    for feature_id, evidence in mappings.items():
        if feature_id not in feature_ids:
            raise ValueError(f"unknown mapped feature: {feature_id}")
        if not isinstance(evidence, dict) or set(evidence) != set(ROLES):
            raise ValueError(f"feature needs all evidence roles: {feature_id}")
        language = feature_id.partition("/")[0]
        for role, witnesses in evidence.items():
            if not isinstance(witnesses, list):
                raise ValueError(f"invalid {role} witnesses: {feature_id}")
            references = []
            for witness in witnesses:
                reference = witness.get("case")
                reason = witness.get("reason")
                if reference not in cases or cases[reference]["language"] != language:
                    raise ValueError(f"nonexistent or cross-language witness: {feature_id}/{role}/{reference}")
                if not isinstance(reason, str) or len(reason.strip()) < 20:
                    raise ValueError(f"missing causal review rationale: {feature_id}/{role}")
                if role != "negative" and cases[reference]["expected_outcome"] != "query-result":
                    raise ValueError(f"negative/rejected query used as {role}: {feature_id}/{reference}")
                references.append(reference)
                used.add(reference)
            if len(references) != len(set(references)):
                raise ValueError(f"duplicate witness: {feature_id}/{role}")
    if set(cases) != used:
        raise ValueError(f"orphan reviewed cases: {sorted(set(cases) - used)}")


def mapped_inventory(inventory, registry):
    result = copy.deepcopy(inventory)
    for language, surface in result["surfaces"].items():
        for feature in surface["features"]:
            mapping = registry["features"].get(f"{language}/{feature['id']}", {})
            feature["evidence"] = {role: [witness["case"] for witness in mapping.get(role, [])]
                                   for role in ROLES}
            present = sum(bool(feature["evidence"][role]) for role in ROLES)
            feature["status"] = "mapped" if present == 3 else "partial" if present else "uncovered"
            feature["gaps"] = {role: "No reviewed source-bound executable witness for this feature and role."
                               for role in ROLES if not feature["evidence"][role]}
    return result


def typed_equal(expected, actual):
    """Compare complete JSON values without Python's bool/int coercion."""
    if type(expected) is not type(actual):
        return False
    if isinstance(expected, dict):
        return (expected.keys() == actual.keys()
                and all(typed_equal(value, actual[key]) for key, value in expected.items()))
    if isinstance(expected, list):
        return (len(expected) == len(actual)
                and all(typed_equal(left, right) for left, right in zip(expected, actual)))
    return expected == actual


def subset_matches(expected, actual):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(key in actual and subset_matches(value, actual[key])
                                               for key, value in expected.items())
    # Types matter: Python otherwise treats True as the integer 1.
    return typed_equal(expected, actual)


def runtime_errors(registry, reference, report_name, case, report):
    descriptor = registry["cases"].get(reference)
    if descriptor is None:
        return []
    errors = []
    if descriptor["report"] != report_name:
        errors.append("reviewed case report differs")
    if not subset_matches(descriptor.get("required_report_values", {}), report):
        errors.append("reviewed case build/engine configuration differs")
    if not subset_matches(descriptor["identity"], case):
        errors.append("reviewed case query/request identity differs")
    for field, value in descriptor.get("exact_case_values", {}).items():
        if field not in case or not typed_equal(value, case[field]):
            errors.append(f"reviewed exact semantic value differs: {field}")
    rejection = (case.get("expected_rejection") is True or report.get("expected_outcome") == "query-rejection"
                 or case.get("classification") == "paired-expected-error")
    if rejection != (descriptor["expected_outcome"] == "query-rejection"):
        errors.append("reviewed case rejection/result outcome differs")
    for field in descriptor.get("required_case_fields", []):
        if field not in case:
            errors.append(f"reviewed case missing semantic comparison field: {field}")
    for required in descriptor.get("ast_requirements", []):
        metadata = case.get("features", {})
        if metadata.get("parsed") is not True or not any(
                subset_matches(required, node) for node in metadata.get("nodes", [])):
            errors.append("reviewed parsed AST witness missing or differs")
    return errors



def typed_tree(tree):
    """Validate serialized AST signatures and reproduce its exact rendered query."""
    if not isinstance(tree, dict) or set(tree) != {"node"} or not isinstance(tree["node"], dict):
        raise ValueError("invalid typed AST shape")
    node = tree["node"]
    kind = node.get("node")
    children = []
    quote = lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    def labels(items):
        if not isinstance(items, list):
            raise ValueError("invalid selector clauses")
        result = []
        for item in items:
            if (not isinstance(item, dict) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", item.get("key", ""))
                    or item.get("op") not in ("Eq", "Neq", "Regex", "NotRegex")
                    or not isinstance(item.get("value"), str)):
                raise ValueError("invalid typed label matcher")
            op = {"Eq": "=", "Neq": "!=", "Regex": "=~", "NotRegex": "!~"}[item["op"]]
            result.append(item["key"] + op + quote(item["value"]))
        return ",".join(result)

    def field(item, scalar):
        if (not isinstance(item, dict) or item.get("scalar_type") != scalar
                or item.get("scope") not in ("", "resource", "span", "event", "link", "instrumentation")
                or not re.fullmatch(r"[A-Za-z_:.][A-Za-z_0-9:.]*", item.get("key", ""))):
            raise ValueError("invalid typed TraceQL field")
        scope, key = item["scope"], item["key"]
        if ":" in key:
            allowed = {"String": {"span:name", "span:id", "event:name", "link:traceID", "link:spanID", "instrumentation:name", "instrumentation:version"},
                       "Duration": {"span:duration", "event:timeSinceStart"}}
            if scope or key not in allowed.get(scalar, set()):
                raise ValueError("invalid scoped intrinsic type")
            return key
        if scalar == "Duration" and (scope or key != "duration"):
            raise ValueError("invalid duration field")
        if scalar == "String" and scope in ("", "span") and key in ("duration", "status", "kind"):
            raise ValueError("intrinsic field has a different scalar type")
        return scope + "." + key if scope else key if key in ("name", "duration", "status", "kind") else "." + key

    if kind in ("prom_scalar", "prom_metric", "log_stream", "profile_selector"):
        if kind == "prom_scalar":
            if type(node.get("value")) is not int:
                raise ValueError("invalid scalar literal")
            value_type, text = "PromScalar", str(node["value"])
        elif kind == "prom_metric":
            if not re.fullmatch(r"[A-Za-z_:][A-Za-z_0-9:]*", node.get("name", "")):
                raise ValueError("invalid metric name")
            clauses = labels(node.get("labels"))
            value_type, text = "PromVector", node["name"] + ("{" + clauses + "}" if clauses else "")
        else:
            clauses = labels(node.get("labels"))
            if not clauses:
                raise ValueError("empty selector")
            value_type, text = ("LogStream", "{" + clauses + "}") if kind == "log_stream" else ("ProfileSelector", clauses)
    elif kind in ("prom_vector", "prom_range", "prom_rate", "log_count", "apply"):
        child_type, text, _, _ = typed_tree(node.get("input"))
        children.append(node["input"])
        if kind == "apply":
            constructor = node.get("constructor")
            if isinstance(constructor, str):
                name, args = constructor, None
            elif isinstance(constructor, dict) and len(constructor) == 1:
                name, args = next(iter(constructor.items()))
            else:
                raise ValueError("invalid typed constructor")
            if name in ("PromAbs", "PromClampMin", "PromAdd", "PromSum") and child_type == "PromVector":
                if name in ("PromClampMin", "PromAdd") and type(args) is not int:
                    raise ValueError("invalid scalar constructor argument")
                if name == "PromSum":
                    groups = args.get("by") if isinstance(args, dict) else None
                    if not isinstance(groups, list) or any(not isinstance(group, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", group) for group in groups):
                        raise ValueError("invalid grouping labels")
                    text = "sum" + (" by(" + ",".join(groups) + ")" if groups else "") + "(" + text + ")"
                else:
                    text = f"abs({text})" if name == "PromAbs" else f"clamp_min({text},{args})" if name == "PromClampMin" else f"({text})+{args}"
            elif name in ("LogSum", "LogMax", "LogAvg", "LogAdd") and child_type == "LogVector":
                if name == "LogAdd" and type(args) is not int:
                    raise ValueError("invalid scalar constructor argument")
                text = f"({text})+{args}" if name == "LogAdd" else name[3:].lower() + "(" + text + ")"
            elif name in ("TraceAnd", "TraceOr") and child_type == "TracePredicate":
                other_type, other_text, _, _ = typed_tree(args)
                if other_type != child_type:
                    raise ValueError("TraceQL boolean child type differs")
                children.append(args)
                text = "(" + text + ") " + ("&&" if name == "TraceAnd" else "||") + " (" + other_text + ")"
            elif name == "ProfileAnd" and child_type == "ProfileSelector":
                text += "," + labels([args])
            elif name == "Paren" and child_type != "ProfileSelector":
                text = "(" + text + ")"
            else:
                raise ValueError("typed constructor signature differs")
            value_type = child_type
        elif kind == "prom_vector" and child_type == "PromScalar":
            value_type, text = "PromVector", "vector(" + text + ")"
        elif kind == "prom_rate" and child_type == "PromRange":
            value_type, text = "PromVector", "rate(" + text + ")"
        elif kind in ("prom_range", "log_count") and child_type == ("PromVector" if kind == "prom_range" else "LogStream"):
            seconds = node.get("seconds")
            if type(seconds) is not int or seconds <= 0:
                raise ValueError("invalid range duration")
            if kind == "log_count":
                value_type, text = "LogVector", f"count_over_time({text}[{seconds}s])"
            else:
                value_type = "PromRange"
                text = f"{text}[{seconds}s]" if node["input"]["node"]["node"] == "prom_metric" else f"({text})[{seconds}s:1s]"
        else:
            raise ValueError("typed node child signature differs")
    elif kind in ("trace_string", "trace_duration", "trace_field_comparison", "trace_scaled_duration_comparison"):
        op = node.get("op")
        operators = {"Eq":"=", "Neq":"!=", "Gt":">", "Gte":">=", "Lt":"<", "Lte":"<=", "Regex":"=~", "NotRegex":"!~"}
        if op not in operators:
            raise ValueError("invalid comparison operator")
        if kind == "trace_string":
            if op not in ("Eq", "Neq", "Regex", "NotRegex") or not isinstance(node.get("value"), str):
                raise ValueError("invalid string comparison")
            left, right = field(node.get("field"), "String"), quote(node["value"])
        elif kind == "trace_duration":
            if op in ("Regex", "NotRegex") or type(node.get("nanos")) is not int or node.get("field", {}).get("key") != "duration":
                raise ValueError("invalid duration comparison")
            left, right = field(node["field"], "Duration"), str(node["nanos"]) + "ns"
        else:
            scalar = node.get("left", {}).get("scalar_type")
            if scalar not in ("String", "Duration") or op in ("Regex", "NotRegex") or (scalar == "String" and op not in ("Eq", "Neq")):
                raise ValueError("invalid field comparison")
            left, right = field(node.get("left"), scalar), field(node.get("right"), scalar)
            if kind == "trace_scaled_duration_comparison":
                factor = node.get("factor")
                if scalar != "Duration" or type(factor) is not int or not -(1 << 63) <= factor < (1 << 63):
                    raise ValueError("invalid scaled duration comparison")
                left = f"({left} * {factor})"
        value_type, text = "TracePredicate", left + " " + operators[op] + " " + right
    else:
        raise ValueError("unknown typed AST node")
    parsed_children = [typed_tree(child) for child in children]
    depth = max((child[2] + 1 for child in parsed_children), default=1 if kind == "trace_scaled_duration_comparison" else 0)
    parents = []
    def collect(child):
        child_type, child_text, _, _ = typed_tree(child)
        child_node = child["node"]
        nested = [child_node["input"]] if "input" in child_node else []
        constructor = child_node.get("constructor")
        if isinstance(constructor, dict) and any(key in constructor for key in ("TraceAnd", "TraceOr")):
            nested.append(next(iter(constructor.values())))
        for descendant in nested:
            collect(descendant)
        if child_type == value_type and child_text not in parents:
            parents.append(child_text)
    for child in children:
        collect(child)
    return value_type, text, depth, parents

def self_check():
    registry = {"cases": {"promql/functions.test/1": {
        "report": "corpus.json", "identity": {"query": "abs(metric)", "expected_rejection": False},
        "expected_outcome": "query-result", "required_case_fields": ["status"]}}}
    case = {"query": "abs(metric)", "expected_rejection": False, "status": "matched"}
    assert not runtime_errors(registry, "promql/functions.test/1", "corpus.json", case, {})
    for wrong in ({**case, "query": "abs(other)"}, {**case, "expected_rejection": True},
                  {"query": "abs(metric)", "expected_rejection": False}):
        assert runtime_errors(registry, "promql/functions.test/1", "corpus.json", wrong, {})
    assert runtime_errors(registry, "promql/functions.test/1", "wrong.json", case, {})
    assert not subset_matches({"value": 1}, {"value": True})
    assert not subset_matches({"values": [True]}, {"values": [1]})
    assert not typed_equal({"status": 400}, {"status": 400.0})
    assert not typed_equal({"error": {"status": 400}}, {"error": {"status": 400}, "extra": None})
    empty_registry = {"cases": {"pyroscope/empty": {
        "report": "rpc.json", "identity": {"request": {"labelSelector": "missing"}},
        "expected_outcome": "semantic-negative", "exact_case_values": {"oracle": {}, "candidate": {}}}}}
    empty = {"request": {"labelSelector": "missing"}, "oracle": {}, "candidate": {}}
    assert not runtime_errors(empty_registry, "pyroscope/empty", "rpc.json", empty, {})
    for changed in ({**empty, "oracle": {"series": []}},
                    {**empty, "candidate": {"error_code": "internal", "status": 500}},
                    {**empty, "candidate": []}, {"request": empty["request"], "oracle": {}}):
        assert runtime_errors(empty_registry, "pyroscope/empty", "rpc.json", changed, {})
    assert not subset_matches({"request": {"profileTypeID": "cpu"}}, {"request": {"profileTypeID": "heap"}})
    inventory = {"surfaces": {"promql": {"features": [{"id": "function.abs"}]}}}
    partial = mapped_inventory(inventory, {"features": {"promql/function.abs": {
        "positive": [{"case": "known"}], "negative": [], "composition": []}}})
    assert partial["surfaces"]["promql"]["features"][0]["status"] == "partial"
    assert set(partial["surfaces"]["promql"]["features"][0]["gaps"]) == {"negative", "composition"}
    metric = {"node":{"node":"prom_metric", "name":"request_total", "labels":[]}}
    scalar = {"node":{"node":"prom_scalar", "value":1}}
    vector = {"node":{"node":"prom_vector", "input":scalar}}
    range_tree = {"node":{"node":"prom_range", "input":vector, "seconds":60}}
    rate = {"node":{"node":"prom_rate", "input":range_tree}}
    assert typed_tree(rate) == ("PromVector", "rate((vector(1))[60s:1s])", 3, ["vector(1)"])
    wrapper = {"node":{"node":"apply", "constructor":"PromAbs", "input":metric}}
    assert typed_tree(wrapper) == ("PromVector", "abs(request_total)", 1, ["request_total"])
    for bad in ({"node":{"node":"prom_rate", "input":scalar}},
                {"node":{"node":"apply", "constructor":"LogSum", "input":metric}},
                {"node":{"node":"prom_metric", "name":"x{injected}", "labels":[]}}):
        try:
            typed_tree(bad)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid typed signature accepted")
    duration = lambda key: {"scope":"", "key":key, "scalar_type":"Duration"}
    comparison = {"node":{"node":"trace_field_comparison", "left":duration("span:duration"), "op":"Gt", "right":duration("event:timeSinceStart")}}
    assert typed_tree(comparison) == ("TracePredicate", "span:duration > event:timeSinceStart", 0, [])
    scaled = {"node":{"node":"trace_scaled_duration_comparison", "left":duration("duration"), "factor":2, "op":"Gt", "right":duration("duration")}}
    assert typed_tree(scaled) == ("TracePredicate", "(duration * 2) > duration", 1, [])
    for corrupt in ({**comparison["node"], "op":"Regex"},
                    {**comparison["node"], "right":duration("name")},
                    {**scaled["node"], "factor":True},
                    {**scaled["node"], "factor":1 << 63}):
        try:
            typed_tree({"node":corrupt})
        except ValueError:
            pass
        else:
            raise AssertionError("invalid duration field AST accepted")
    print("reviewed query evidence negative controls passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="apply reviewed mappings to inventory")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    inventory = json.loads(INVENTORY.read_text())
    registry = json.loads(REGISTRY.read_text())
    check_registry(registry, inventory)
    updated = mapped_inventory(inventory, registry)
    if args.apply:
        INVENTORY.write_text(json.dumps(updated, indent=2) + "\n")
    elif inventory != updated:
        raise ValueError("inventory differs from reviewed registry; use --apply after review")
    # Reuse the canonical denominator check after applying evidence.
    runpy.run_path(str(ROOT / "tools/query-language-inventory.py"))["check"](updated)
    print(f"reviewed evidence: {len(registry['cases'])} cases, {len(registry['features'])} feature mappings")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(f"query-language evidence: {error}", file=sys.stderr)
        sys.exit(1)
