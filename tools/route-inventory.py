#!/usr/bin/env python3
"""Generate the checked-in inventory of Krabka's served HTTP routes."""

import argparse
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
        "schema_version": 1,
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
        print(f"route inventory is current ({len(inventory()['routes'])} routes)")
        return
    print(rendered, end="")


if __name__ == "__main__":
    main()
