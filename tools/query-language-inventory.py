#!/usr/bin/env python3
"""Extract pinned upstream query surfaces; validate their explicit evidence offline.

--refresh downloads the six pinned files and applies the reviewed registry to
the complete inventory. --check performs offline schema,
provenance, graph, count, and evidence validation; it does not rerun upstream
extraction or establish that mapped tests passed. --self-check tests extraction.
"""

import argparse
import hashlib
import json
import pathlib
import re
import runpy
import sys
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parent.parent
ARTIFACT = ROOT / "qualification" / "query-language-inventory.json"
PINS = {
    "promql": {
        "repository": "prometheus/prometheus",
        "version": "3.14.0",
        "revision": "d7598b7141418fa35be2b5ec5d0fefb634199610",
        "license": "Apache-2.0",
        "feature_flags": ["promql-experimental-functions"],
        "files": {
            "promql/parser/functions.go": "07f80297bec01b10d2a982f461900b1025ea66af91c4905154a11a42b43aaf00",
            "promql/parser/generated_parser.y": "299268003cee3f5d1329294c894db7a769848e6d7b6c29bfc3eb231b738b350e",
        },
    },
    "logql": {
        "repository": "grafana/loki",
        "version": "3.7.7",
        "revision": "7a40404f32b3e6464c9cfc6cc7dd75a40f3931da",
        "license": "AGPL-3.0",
        "feature_flags": [],
        "files": {
            "pkg/logql/syntax/syntax.y": "cf3e7a35661a4301778efcc75b95616b3d35ed9bc80fa99e517acf681903ccd0",
        },
    },
    "traceql": {
        "repository": "grafana/tempo",
        "version": "3.0.3",
        "revision": "1900ed7bb5cad1a3edc285783d7d4ac4278337dc",
        "license": "AGPL-3.0",
        "feature_flags": [],
        "files": {
            "pkg/traceql/expr.y": "91b27194068e800ed52940139352e111eef7176af6f1ddabef48d1a8d27be5b2",
        },
    },
    "pyroscope": {
        "repository": "grafana/pyroscope",
        "version": "2.3.1",
        "revision": "7aeaa0ff91e83538b3ff0d09bfefb168bddc022d",
        "license": "AGPL-3.0",
        "feature_flags": [],
        "files": {
            "api/querier/v1/querier.proto": "f36e6f99c00af205454d68c8bbf99b29ec8d3b0b7a401e932469d0f3e2df21e2",
            "api/types/v1/types.proto": "f1d0447e246cadb0021e537f6b918d72255e1bfcb632ed11f8e558a2c8bb4f0d",
        },
    },
}
# Canonical extracted structures for the exact source hashes above. Offline
# validation must reject a smaller denominator even if its mappings also shrink.
STRUCTURE_HASHES = {
    "promql": "52d2d84ff13edf2f68677e7edcade8f6301daf87e33d2328bf0683859adbe106",
    "logql": "422278af64ccf02dc1a697e64cec4af3ef2bf94d3497c533096e7f15e10f7e4e",
    "traceql": "452475746dc23c55a214dda535e3c714dc66be07ed9276615d53c0c7438c8591",
    "pyroscope": "9331800b2cc8726153af87c8d20dc900e653dadedfdf683c1e9963d476e19d3d",
}


def mask_comments_and_strings(text):
    """Keep offsets/newlines while hiding braces and delimiters inside literals."""
    out = list(text)
    index = 0
    while index < len(text):
        end = index
        if text.startswith("//", index):
            end = text.find("\n", index)
            end = len(text) if end < 0 else end
        elif text.startswith("/*", index):
            end = text.find("*/", index + 2)
            if end < 0:
                raise ValueError("unterminated source comment")
            end += 2
        elif text[index] in "\"'`":
            quote = text[index]
            end = index + 1
            while end < len(text):
                if text[end] == "\\" and quote != "`":
                    end += 2
                    continue
                if text[end] == quote:
                    end += 1
                    break
                end += 1
            else:
                raise ValueError("unterminated source string")
        if end > index:
            for pos in range(index, min(end, len(text))):
                if out[pos] != "\n":
                    out[pos] = " "
            index = end
        else:
            index += 1
    return "".join(out)


def blocks(text, kind):
    masked = mask_comments_and_strings(text)
    for match in re.finditer(r"(?m)^" + kind + r"\s+(\w+)\s*\{", masked):
        depth = 1
        end = match.end()
        while end < len(masked) and depth:
            depth += (masked[end] == "{") - (masked[end] == "}")
            end += 1
        if depth:
            raise ValueError(f"unterminated {kind} {match.group(1)}")
        yield match.group(1), text[match.end() : end - 1]


def functions(text):
    entries = []
    for match in re.finditer(
        r'(?ms)^\t"([A-Za-z_]\w*)"\s*:\s*\{(.*?)^\t\},', text
    ):
        name, body = match.groups()
        args = re.search(r"ArgTypes:\s*\[\]ValueType\{([^}]*)\}", body)
        result = re.search(r"ReturnType:\s*(\w+)", body)
        variadic = re.search(r"Variadic:\s*(-?\d+)", body)
        if args is None or result is None:
            raise ValueError(f"incomplete function signature: {name}")
        entries.append(
            {
                "name": name,
                "arg_types": re.findall(r"ValueType\w+", args.group(1)),
                "return_type": result.group(1),
                "variadic": int(variadic.group(1)) if variadic else 0,
                "experimental": bool(re.search(r"Experimental:\s*true", body)),
            }
        )
    if not entries:
        raise ValueError("function registry is empty")
    return sorted(entries, key=lambda entry: entry["name"])


def grammar(text, entry):
    parts = text.split("%%")
    if len(parts) < 2:
        raise ValueError("grammar has no rules section")
    declarations = mask_comments_and_strings(parts[0])
    terminals = set()
    for declaration in re.findall(
        r"(?m)^%(?:token|left|right|nonassoc)\b([^%]*)", declarations
    ):
        declaration = re.sub(r"<[^>]*>", "", declaration)
        terminals.update(re.findall(r"\b[A-Za-z_]\w*\b", declaration))
    body = mask_comments_and_strings(parts[1])
    chars = list(body)
    depth = 0
    for index, char in enumerate(body):
        if char == "{":
            depth += 1
        if depth and char != "\n":
            chars[index] = " "
        if char == "}":
            depth -= 1
        if depth < 0:
            raise ValueError("unbalanced grammar action")
    if depth:
        raise ValueError("unterminated grammar action")
    body = "".join(chars)
    starts = list(re.finditer(r"(?m)^([A-Za-z_]\w*)\s*:", body))
    productions = []
    for index, match in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(body)
        rhs = body[match.end() : end].strip().rstrip(";").strip()
        for alternative, symbols in enumerate(rhs.split("|"), 1):
            productions.append(
                {
                    "id": f"{match.group(1)}#{alternative}",
                    "lhs": match.group(1),
                    "symbols": re.findall(r"\b[A-Za-z_]\w*\b", symbols),
                }
            )
    return public_grammar(entry, sorted(terminals), productions)


def public_grammar(entry, terminals, productions):
    nonterminals = {production["lhs"] for production in productions}
    if entry not in nonterminals:
        raise ValueError(f"missing public grammar entry {entry}")
    reachable = set()
    pending = [entry]
    while pending:
        name = pending.pop()
        if name in reachable:
            continue
        reachable.add(name)
        for production in productions:
            if production["lhs"] == name:
                pending.extend(set(production["symbols"]) & nonterminals - reachable)
    selected = [item for item in productions if item["lhs"] in reachable]
    symbols = {symbol for item in selected for symbol in item["symbols"]}
    return {
        "entry": entry,
        "declared_named_terminals": sorted(terminals),
        "all_productions": productions,
        "query_named_terminals": sorted(symbols & set(terminals)),
        "query_nonterminals": sorted(reachable),
        "query_production_ids": [item["id"] for item in selected],
        "non_query_nonterminals": sorted(nonterminals - reachable),
    }


def profile_requests(sources):
    messages = {}
    enums = {}
    for path, text in sources.items():
        prefix = "types.v1." if "/types/" in path else ""
        for name, body in blocks(text, "message"):
            fields = []
            for match in re.finditer(
                r"(?m)^\s*(?:(repeated|optional|required)\s+)?"
                r"([.A-Za-z_]\w*(?:\.\w+)*)\s+(\w+)\s*=\s*(\d+)\s*(?:\[|;)",
                mask_comments_and_strings(body),
            ):
                cardinality, field_type, field_name, number = match.groups()
                fields.append(
                    {
                        "name": field_name,
                        "type": field_type,
                        "number": int(number),
                        "cardinality": cardinality or "singular",
                    }
                )
            messages[prefix + name] = {
                "source": path,
                "package_prefix": prefix,
                "fields": fields,
            }
        for name, body in blocks(text, "enum"):
            enums[prefix + name] = [
                {"name": label, "number": int(number)}
                for label, number in re.findall(
                    r"(?m)^\s*(\w+)\s*=\s*(\d+)\s*;",
                    mask_comments_and_strings(body),
                )
            ]
    service = dict(blocks(sources["api/querier/v1/querier.proto"], "service"))[
        "QuerierService"
    ]
    rpcs = [
        {"method": method, "request": request, "response": response}
        for method, request, response in re.findall(
            r"\brpc\s+(\w+)\s*\(\s*([\w.]+)\s*\)\s*returns\s*"
            r"\(\s*([\w.]+)\s*\)",
            mask_comments_and_strings(service),
        )
    ]
    pending = [rpc["request"] for rpc in rpcs]
    reachable = set()
    used_enums = set()
    while pending:
        name = pending.pop()
        if name in reachable:
            continue
        message = messages[name]
        reachable.add(name)
        for field in message["fields"]:
            field_type = field["type"]
            resolved = (
                field_type
                if "." in field_type
                else message["package_prefix"] + field_type
            )
            if resolved in messages:
                pending.append(resolved)
            elif resolved in enums:
                used_enums.add(resolved)
    return {
        "rpcs": sorted(rpcs, key=lambda rpc: rpc["method"]),
        "request_messages": {name: messages[name] for name in sorted(reachable)},
        "request_enums": {name: enums[name] for name in sorted(used_enums)},
    }


def feature_ids(surface):
    ids = []
    for function in surface.get("functions", []):
        ids.append("function." + function["name"])
    if "grammar" in surface:
        data = surface["grammar"]
        for kind, field in [
            ("terminal", "query_named_terminals"),
            ("nonterminal", "query_nonterminals"),
            ("production", "query_production_ids"),
        ]:
            ids.extend(f"{kind}.{name}" for name in data[field])
    if "requests" in surface:
        data = surface["requests"]
        ids.extend("rpc." + rpc["method"] for rpc in data["rpcs"])
        for name, message in data["request_messages"].items():
            ids.append("message." + name)
            ids.extend(f"field.{name}.{field['name']}" for field in message["fields"])
        for name, variants in data["request_enums"].items():
            ids.extend(f"enum.{name}.{variant['name']}" for variant in variants)
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate extracted feature ids")
    return sorted(ids)


def build(download):
    surfaces = {}
    for language, pin in PINS.items():
        sources = {}
        provenance = []
        for path, expected_hash in pin["files"].items():
            url = f"https://raw.githubusercontent.com/{pin['repository']}/{pin['revision']}/{path}"
            content = download(url)
            if hashlib.sha256(content).hexdigest() != expected_hash:
                raise ValueError(f"source hash differs for {language}: {path}")
            sources[path] = content.decode("utf-8")
            provenance.append(
                {
                    "path": path,
                    "sha256": expected_hash,
                    "url": f"https://github.com/{pin['repository']}/blob/{pin['revision']}/{path}",
                }
            )
        surface = {"pin": pin, "provenance": provenance}
        if language == "pyroscope":
            surface["requests"] = profile_requests(sources)
        else:
            path = next(path for path in sources if path.endswith(".y"))
            surface["grammar"] = grammar(sources[path], "expr" if language == "promql" else "root")
            if language == "promql":
                surface["functions"] = functions(sources["promql/parser/functions.go"])
        surface["features"] = [
            {
                "id": name,
                "status": "uncovered",
                "evidence": {"positive": [], "negative": [], "composition": []},
            }
            for name in feature_ids(surface)
        ]
        surfaces[language] = surface
    return {
        "schema_version": 1,
        "scope": "Pinned public-query syntax, function signatures and profile query requests; evidence mappings do not establish passing execution.",
        "surfaces": surfaces,
    }


def check(artifact):
    if artifact.get("schema_version") != 1 or set(artifact.get("surfaces", {})) != set(PINS):
        raise ValueError("unexpected inventory schema or languages")
    classifier = (ROOT / "crates/promql/src/conformance/case_features.rs").read_text()
    constant = classifier.partition("const EXPERIMENTAL_FUNCTIONS: &[&str] = &[")[2].partition("];")[0]
    declared = re.findall(r'"([a-z_]+)"', constant)
    experimental = [function["name"] for function in artifact["surfaces"]["promql"]["functions"] if function["experimental"]]
    if len(declared) != len(set(declared)) or set(declared) != set(experimental):
        raise ValueError("Rust disabled-feature classifier differs from pinned function registry")
    for language, surface in artifact["surfaces"].items():
        pin = PINS[language]
        if surface["pin"] != pin:
            raise ValueError(f"{language}: source pins differ")
        expected_provenance = [
            {
                "path": path,
                "sha256": digest,
                "url": f"https://github.com/{pin['repository']}/blob/{pin['revision']}/{path}",
            }
            for path, digest in pin["files"].items()
        ]
        if surface["provenance"] != expected_provenance:
            raise ValueError(f"{language}: source provenance differs")
        structure = {
            key: surface[key]
            for key in ("grammar", "functions", "requests")
            if key in surface
        }
        digest = hashlib.sha256(
            json.dumps(structure, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if digest != STRUCTURE_HASHES[language]:
            raise ValueError(f"{language}: pinned extracted denominator differs")
        if "grammar" in surface:
            data = surface["grammar"]
            if public_grammar(data["entry"], data["declared_named_terminals"], data["all_productions"]) != data:
                raise ValueError(f"{language}: public grammar graph differs")
        features = surface["features"]
        if [feature["id"] for feature in features] != feature_ids(surface):
            raise ValueError(f"{language}: feature classifications omit or duplicate inventory entries")
        for feature in features:
            evidence = feature["evidence"]
            if set(evidence) != {"positive", "negative", "composition"} or not all(
                isinstance(items, list)
                and all(isinstance(item, str) and item for item in items)
                for items in evidence.values()
            ):
                raise ValueError(f"{language}: invalid explicit evidence for {feature['id']}")
            status = feature["status"]
            if status not in {"uncovered", "partial", "mapped"}:
                raise ValueError(f"{language}: invalid status for {feature['id']}")
            if status == "uncovered" and any(evidence.values()):
                raise ValueError(f"{language}: uncovered feature has evidence: {feature['id']}")
            if status == "mapped" and not all(evidence.values()):
                raise ValueError(f"{language}: mapped feature needs positive, negative and composition evidence")
            if status == "partial" and (not any(evidence.values()) or all(evidence.values())):
                raise ValueError(f"{language}: partial feature must retain missing evidence roles")
    registry_path = ROOT / "qualification/query-language-evidence.json"
    if registry_path.exists():
        helpers = runpy.run_path(str(ROOT / "tools/query-language-evidence.py"))
        registry = json.loads(registry_path.read_text())
        helpers["check_registry"](registry, artifact)
        if artifact != helpers["mapped_inventory"](artifact, registry):
            raise ValueError("inventory evidence differs from reviewed source-bound registry")


def self_check():
    source = '''%token <value> REAL OTHER UNUSED
%%
root: child { if true { text := "fake: | { }" }; /* bogus: */ }
child: REAL | OTHER { // fake: | }
 return nil
}
fixture: UNUSED;
'''
    data = grammar(source, "root")
    assert data["query_nonterminals"] == ["child", "root"]
    assert data["query_named_terminals"] == ["OTHER", "REAL"]
    assert data["query_production_ids"] == ["root#1", "child#1", "child#2"]
    proto = '''service QuerierService {
  rpc Stats(Empty) returns (Response) {}
  rpc Select(Request) returns (Response) {}
}
message Empty {}
message Request {
  // string ignored = 99;
  optional Child child = 1 [example = {yaml: "} {"}];
}
message Child {
  string value = 1;
}
message Response {
  string not_a_request = 1;
}
'''
    requests = profile_requests({"api/querier/v1/querier.proto": proto})
    assert requests["request_messages"]["Empty"]["fields"] == []
    assert set(requests["request_messages"]) == {"Empty", "Request", "Child"}
    assert requests["request_messages"]["Request"]["fields"][0]["name"] == "child"
    assert requests["request_messages"]["Child"]["fields"][0]["name"] == "value"
    assert "Response" not in requests["request_messages"]
    if ARTIFACT.exists():
        artifact = json.loads(ARTIFACT.read_text())
        check(artifact)
        check_oracle_pins()
        del artifact["surfaces"]["pyroscope"]["requests"]["rpcs"][0]
        try:
            check(artifact)
        except ValueError:
            pass
        else:
            raise AssertionError("removed RPC was accepted")
    print("query-language extractor self-check passed")


def check_oracle_pins():
    source = (ROOT / "bazel/images/images.bzl").read_text()
    products = {"promql": "prometheus", "logql": "loki", "traceql": "tempo", "pyroscope": "pyroscope"}
    for language, product in products.items():
        match = re.search(r'"' + product + r'": struct\((.*?)\n\s*\),', source, re.S)
        if not match:
            raise ValueError(f"missing {product} oracle")
        block = match.group(1)
        revision = re.search(r'revision = "([a-f0-9]{40})"', block)
        image = re.search(r'image = "[^" ]+:v?([^" ]+)"', block)
        pin = PINS[language]
        if not revision or not image or revision.group(1) != pin["revision"] or image.group(1) != pin["version"]:
            raise ValueError(f"{language}: inventory and executable oracle pins differ")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--check", action="store_true", help="validate checked-in artifact offline (default)")
    actions.add_argument("--refresh", action="store_true", help="fetch exact pinned sources and retain reviewed source-bound evidence")
    actions.add_argument("--self-check", action="store_true", help="exercise extraction edge cases without network")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    if args.refresh:
        def download(url):
            with urllib.request.urlopen(url, timeout=30) as response:
                return response.read()
        artifact = build(download)
        registry_path = ROOT / "qualification/query-language-evidence.json"
        if registry_path.exists():
            helpers = runpy.run_path(str(ROOT / "tools/query-language-evidence.py"))
            registry = json.loads(registry_path.read_text())
            helpers["check_registry"](registry, artifact)
            artifact = helpers["mapped_inventory"](artifact, registry)
        check(artifact)
        ARTIFACT.write_text(json.dumps(artifact, indent=2) + "\n")
    else:
        artifact = json.loads(ARTIFACT.read_text())
        check(artifact)
        check_oracle_pins()
    for language, surface in artifact["surfaces"].items():
        features = surface["features"]
        uncovered = sum(feature["status"] == "uncovered" for feature in features)
        partial = sum(feature["status"] == "partial" for feature in features)
        mapped = sum(feature["status"] == "mapped" for feature in features)
        print(f"{language} {surface['pin']['version']}: {len(features)} inventoried features, {mapped} mapped, {partial} partial, {uncovered} uncovered")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(f"query-language inventory: {error}", file=sys.stderr)
        sys.exit(1)
