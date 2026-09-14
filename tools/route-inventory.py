#!/usr/bin/env python3
"""Generate the checked-in HTTP and Profiles protobuf API inventory."""

import argparse
import hashlib
import json
import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parent.parent
INVENTORY = ROOT / "docs" / "api" / "routes.json"
SIGNAL_CRATES = {
    "metrics": ("promql", "metrics", "metrics-service"),
    "logs": ("observability",),
    "traces": ("traces",),
    "profiles": ("profiles",),
}
SHARED_ROUTERS = {
    "readiness_router": (
        "observability",
        "crates/observability/src/readiness/readiness_router.rs",
    ),
}
METHODS = ("get", "post", "put", "delete", "patch")


def matching_brace(text, start):
    depth = 0
    quote = None
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char == '"':
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
    raise ValueError("unclosed brace")


def without_test_modules(text):
    pattern = re.compile(r"#\[cfg\(test\)\]\s*mod\s+\w+\s*\{")
    while match := pattern.search(text):
        end = matching_brace(text, text.index("{", match.start()))
        text = text[: match.start()] + text[end + 1 :]
    return text


def call_arguments(text, marker, start):
    opening = start + len(marker) - 1
    depth = 1
    quote = None
    escaped = False
    for index in range(opening + 1, len(text)):
        char = text[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char == '"':
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth == 0:
                return text[opening + 1 : index]
    raise ValueError(f"unclosed {marker} call")


def split_first_argument(arguments):
    depth = 0
    quote = None
    escaped = False
    for index, char in enumerate(arguments):
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char == '"':
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        elif char == "," and depth == 0:
            return arguments[:index].strip(), arguments[index + 1 :]
    raise ValueError("route call has no handler argument")


def rust_routes(signal, crate):
    source_root = ROOT / "crates" / crate / "src"
    for source in sorted(source_root.rglob("*.rs")):
        relative_source = source.relative_to(source_root)
        if source.name == "tests.rs" or "tests" in relative_source.parts:
            continue
        text = source.read_text()
        relative = source.relative_to(ROOT).as_posix()
        if crate == "observability":
            for path in re.findall(r'role_ring_path:\s*Some\("([^"\\]+)"\)', text):
                yield signal, "GET", path, relative
        if ".route(" not in text and ".route_service(" not in text:
            continue
        text = without_test_modules(text)
        for marker, service in ((".route(", False), (".route_service(", True)):
            cursor = 0
            while (start := text.find(marker, cursor)) >= 0:
                arguments = call_arguments(text, marker, start)
                path_expr, handler = split_first_argument(arguments)
                cursor = start + len(marker)
                path_match = re.fullmatch(r'"([^"\\]+)"', path_expr)
                if not path_match:
                    if relative.endswith("with_role_ops_routes.rs") and path_expr == "path":
                        continue
                    raise ValueError(f"{relative}: route path is not a string literal: {path_expr}")
                methods = ("POST",) if service else tuple(
                    method.upper()
                    for method in METHODS
                    if re.search(rf"(?:^|[.:])\s*{method}\s*\(", handler)
                )
                if not methods and relative.endswith("tempo_query_routes.rs"):
                    methods = (
                        ("GET", "POST", "DELETE", "PATCH")
                        if path_match.group(1) == "/api/overrides"
                        else ("GET",)
                    )
                if not methods:
                    raise ValueError(f"{relative}: cannot determine method for {path_expr}")
                for method in methods:
                    yield signal, method, path_match.group(1), relative


def merges_router(text, router):
    return re.search(
        rf"\.merge\(\s*(?:[\w:]+::)?{re.escape(router)}\s*\(",
        without_test_modules(text),
    ) is not None


def shared_routes(signal, crates):
    for router, (provider_crate, provider_source) in SHARED_ROUTERS.items():
        if provider_crate in crates:
            continue
        uses_router = any(
            router in text and merges_router(text, router)
            for crate in crates
            for source in (ROOT / "crates" / crate / "src").rglob("*.rs")
            if source.name != "tests.rs" and "tests" not in source.parts
            for text in (source.read_text(),)
        )
        if uses_router:
            yield from (
                route
                for route in rust_routes(signal, provider_crate)
                if route[3] == provider_source
            )


def profile_connect_routes():
    proto_root = ROOT / "crates" / "profiles" / "proto"
    for source in sorted(proto_root.rglob("*.proto")):
        text = source.read_text()
        package = re.search(r"^package\s+([\w.]+);", text, re.MULTILINE)
        if not package:
            continue
        for service in re.finditer(r"service\s+(\w+)\s*\{", text):
            opening = text.index("{", service.start())
            body = text[opening + 1 : matching_brace(text, opening)]
            for rpc in re.finditer(r"\brpc\s+(\w+)\s*\(", body):
                yield (
                    "profiles",
                    "POST",
                    f"/{package.group(1)}.{service.group(1)}/{rpc.group(1)}",
                    source.relative_to(ROOT).as_posix(),
                )


def proto_definitions(text, source):
    package_match = re.search(r"^package\s+([\w.]+);", text, re.MULTILINE)
    if not package_match:
        return {"services": [], "messages": [], "enums": []}
    package = package_match.group(1)
    services = []
    for service in re.finditer(r"^\s*service\s+(\w+)\s*\{", text, re.MULTILINE):
        opening = text.index("{", service.start())
        body = text[opening + 1 : matching_brace(text, opening)]
        methods = [
            {
                "name": match.group(1),
                "request": match.group(3),
                "request_stream": bool(match.group(2)),
                "response": match.group(5),
                "response_stream": bool(match.group(4)),
            }
            for match in re.finditer(
                r"\brpc\s+(\w+)\s*\(\s*(stream\s+)?([\w.]+)\s*\)\s*"
                r"returns\s*\(\s*(stream\s+)?([\w.]+)\s*\)",
                body,
            )
        ]
        services.append(
            {
                "name": f"{package}.{service.group(1)}",
                "source": source,
                "methods": methods,
            }
        )

    messages = []
    field_pattern = re.compile(
        r"^\s*(?:(optional|required|repeated)\s+)?"
        r"(map\s*<[^;=]+>|[\w.]+)\s+(\w+)\s*=\s*(\d+)"
        r"(?:\s*\[[^\]]*\])?\s*;",
        re.MULTILINE,
    )
    for message in re.finditer(r"^\s*message\s+(\w+)\s*\{", text, re.MULTILINE):
        opening = text.index("{", message.start())
        body = text[opening + 1 : matching_brace(text, opening)]
        oneofs = []
        for oneof in re.finditer(r"^\s*oneof\s+(\w+)\s*\{", body, re.MULTILINE):
            oneof_opening = body.index("{", oneof.start())
            oneofs.append(
                (
                    oneof_opening,
                    matching_brace(body, oneof_opening),
                    oneof.group(1),
                )
            )
        fields = []
        for field in field_pattern.finditer(body):
            item = {
                "name": field.group(3),
                "number": int(field.group(4)),
                "type": re.sub(r"\s+", "", field.group(2)),
                "label": field.group(1) or "singular",
            }
            if oneof := next(
                (
                    name
                    for start, end, name in oneofs
                    if start < field.start() < end
                ),
                None,
            ):
                item["oneof"] = oneof
            fields.append(item)
        messages.append(
            {
                "name": f"{package}.{message.group(1)}",
                "source": source,
                "fields": fields,
            }
        )

    enums = []
    for enum in re.finditer(r"^\s*enum\s+(\w+)\s*\{", text, re.MULTILINE):
        opening = text.index("{", enum.start())
        body = text[opening + 1 : matching_brace(text, opening)]
        enums.append(
            {
                "name": f"{package}.{enum.group(1)}",
                "source": source,
                "values": [
                    {"name": name, "number": int(number)}
                    for name, number in re.findall(
                        r"\b([A-Z][A-Z0-9_]*)\s*=\s*(-?\d+)\s*;", body
                    )
                ],
            }
        )
    return {"services": services, "messages": messages, "enums": enums}


def profile_proto_inventory():
    result = {"files": [], "services": [], "messages": [], "enums": []}
    proto_root = ROOT / "crates" / "profiles" / "proto"
    for path in sorted(proto_root.rglob("*.proto")):
        text = path.read_text()
        source = path.relative_to(ROOT).as_posix()
        result["files"].append(
            {"source": source, "sha256": hashlib.sha256(text.encode()).hexdigest()}
        )
        definitions = proto_definitions(text, source)
        for kind in ("services", "messages", "enums"):
            result[kind].extend(definitions[kind])
    return result


def inventory():
    routes = {}
    for signal, crates in SIGNAL_CRATES.items():
        for crate in crates:
            for route_signal, method, path, source in rust_routes(signal, crate):
                routes.setdefault((route_signal, method, path), set()).add(source)
        for route_signal, method, path, source in shared_routes(signal, crates):
            routes.setdefault((route_signal, method, path), set()).add(source)
    for signal, method, path, source in profile_connect_routes():
        routes.setdefault((signal, method, path), set()).add(source)
    return {
        "schema_version": 2,
        "generated_by": "tools/route-inventory.py",
        "routes": [
            {
                "signal": signal,
                "method": method,
                "path": path,
                "sources": sorted(sources),
            }
            for (signal, method, path), sources in sorted(routes.items())
        ],
        "protobuf": profile_proto_inventory(),
    }


def rendered_inventory():
    return json.dumps(inventory(), indent=2) + "\n"


def self_test():
    sample = '''
Router::new()
    .route("/read", get(read).post(write))
    .route_service("/rpc.Service/Call", service);
#[cfg(test)]
mod tests {
    fn test_router() { Router::new().route("/test-only", get(handler)); }
}
'''
    stripped = without_test_modules(sample)
    assert "/test-only" not in stripped
    args = call_arguments(stripped, ".route(", stripped.index(".route("))
    path, handler = split_first_argument(args)
    assert path == '"/read"'
    assert all(
        re.search(rf"(?:^|[.:])\s*{method}\s*\(", handler)
        for method in ("get", "post")
    )
    assert merges_router("Router::new().merge(readiness_router(state))", "readiness_router")
    assert merges_router(
        "Router::new().merge(krabka_observability::readiness_router(state))",
        "readiness_router",
    )
    assert not merges_router(sample, "readiness_router")
    proto = proto_definitions(
        """
package example.v1;
service Example { rpc Watch(stream Request) returns (stream Response) {} }
message Request {
  repeated string names = 1;
  oneof selector {
    string label = 2;
    int64 id = 3;
  }
}
message Response {}
enum State { STATE_UNSPECIFIED = 0; STATE_READY = 1; }
""",
        "example.proto",
    )
    assert proto["services"][0]["methods"] == [
        {
            "name": "Watch",
            "request": "Request",
            "request_stream": True,
            "response": "Response",
            "response_stream": True,
        }
    ]
    assert proto["messages"][0]["fields"] == [
        {"name": "names", "number": 1, "type": "string", "label": "repeated"},
        {
            "name": "label",
            "number": 2,
            "type": "string",
            "label": "singular",
            "oneof": "selector",
        },
        {
            "name": "id",
            "number": 3,
            "type": "int64",
            "label": "singular",
            "oneof": "selector",
        },
    ]
    assert proto["enums"][0]["values"][1] == {"name": "STATE_READY", "number": 1}
    print("route inventory self-test passed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    rendered = rendered_inventory()
    if args.check:
        current = INVENTORY.read_text() if INVENTORY.exists() else ""
        if current != rendered:
            print(
                "docs/api/routes.json is stale; run "
                "`tools/route-inventory.py > docs/api/routes.json`",
                file=sys.stderr,
            )
            raise SystemExit(1)
        data = inventory()
        print(
            "route inventory is current "
            f"({len(data['routes'])} routes; "
            f"{len(data['protobuf']['services'])} protobuf services; "
            f"{len(data['protobuf']['messages'])} messages)"
        )
        return
    print(rendered, end="")


if __name__ == "__main__":
    main()
