#!/usr/bin/env python3
"""Copy the native dependency graph into an independent WASIX source tree."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tomllib
from urllib.parse import parse_qs, urlsplit


DEPENDENCIES = ("dependencies", "dev-dependencies", "build-dependencies")
REGISTRY = "sparse+https://cargo-registry.wasix.org/"
REQWEST_TLS = {"rustls": "rustls-tls", "rustls-no-provider": "rustls-tls-no-provider"}
# Verified against the official sparse index; package code is qualified by the build.
WASIX_VERSIONS = {"tokio": "1.47.0", "reqwest": "0.12.22", "mio": "1.0.3"}
# The published Hyper/h2 forks change manifests only. Keep the native HTTP API
# and use the WASIX Tokio/mio implementations underneath it.
HTTP_OVERLAYS = {"hyper": "1.11.1", "h2": "0.4.19", "hyper-util": "0.1.20"}
GETRANDOM_VERSIONS = {(0, 2): "0.2.15", (0, 3): "0.3.3", (0, 4): "0.4.3"}


def excluded(name):
    return name in {".git", "target", ".tools", "node_modules"} or name.startswith("bazel-")


def copy_source(source, destination):
    """Keep unchanged files; changed content needs a fresh Cargo input timestamp."""
    source, destination = Path(source), Path(destination)
    if destination.is_file() and source.stat().st_size == destination.stat().st_size:
        with source.open("rb") as native, destination.open("rb") as staged:
            if hashlib.file_digest(native, "sha256").digest() == hashlib.file_digest(staged, "sha256").digest():
                return str(destination)
    copied = shutil.copy2(source, destination)
    os.utime(copied, None)
    return copied


def manifests(root):
    for directory, children, files in os.walk(root):
        children[:] = [name for name in children if not excluded(name)]
        if "Cargo.toml" in files:
            yield Path(directory) / "Cargo.toml"


def toml_text(data):
    """Serialize parsed TOML, including nested arrays of tables and dependency maps."""
    def key(value):
        return value if re.fullmatch(r"[\w-]+", value, re.ASCII) else json.dumps(value, ensure_ascii=False)

    def value(item):
        if isinstance(item, str):
            return json.dumps(item, ensure_ascii=False)
        if isinstance(item, bool):
            return str(item).lower()
        if isinstance(item, (int, float)):
            return repr(item)
        if isinstance(item, (datetime.date, datetime.time)):
            return item.isoformat()
        if isinstance(item, list):
            return "[" + ", ".join(value(child) for child in item) + "]"
        if isinstance(item, dict):
            return "{ " + ", ".join(f"{key(k)} = {value(v)}" for k, v in item.items()) + " }"
        raise TypeError(f"Unsupported TOML value: {item!r}")

    lines = []

    def table(items, path=(), header=None):
        if header:
            lines.extend(["", header])
        inline = path and (path[-1] in DEPENDENCIES or path[0] in {"patch", "replace"})
        children = []
        for name, item in items.items():
            array = isinstance(item, list) and item and all(isinstance(v, dict) for v in item)
            if not inline and (isinstance(item, dict) or array):
                children.append((name, item, array))
            else:
                lines.append(f"{key(name)} = {value(item)}")
        for name, item, array in children:
            child_path = (*path, name)
            title = ".".join(key(part) for part in child_path)
            for child in item if array else [item]:
                table(child, child_path, f"[[{title}]]" if array else f"[{title}]")

    table(data)
    return "\n".join(lines).lstrip("\n") + "\n"


def version_matches(requirement, version):
    """Match Cargo's numeric requirements; unfamiliar syntax keeps the original pin."""
    actual = tuple(int(part) for part in version.split("-")[0].split("+")[0].split("."))
    for term in requirement.split(","):
        term = term.strip()
        if term in {"*", "x", "X"}:
            continue
        match = re.fullmatch(r"(>=|<=|>|<|=|\^|~)?\s*(\d+(?:\.\d+)*)(?:\.([*xX]))?(?:[-+][\w.-]+)?", term)
        if not match:
            return False
        operator, digits, wildcard = match.groups()
        parts = tuple(int(part) for part in digits.split("."))
        lower = (*parts, *(0 for _ in range(3 - len(parts))))
        if wildcard or operator == "=":
            valid = actual[:len(parts)] == parts
        elif operator in {">", ">=", "<", "<="}:
            valid = {">": actual > lower, ">=": actual >= lower, "<": actual < lower, "<=": actual <= lower}[operator]
        else:
            index = (0 if len(parts) == 1 else 1) if operator == "~" else next((i for i, n in enumerate(parts) if n), len(parts) - 1)
            upper = (*lower[:index], lower[index] + 1, *(0 for _ in range(2 - index)))
            valid = lower <= actual < upper
        if not valid:
            return False
    return True


def dependency_tables(data):
    for parent in [data, data.get("workspace", {}), *data.get("target", {}).values()]:
        for name in DEPENDENCIES:
            yield parent.get(name, {})
    yield from data.get("patch", {}).values()
    yield data.get("replace", {})


def version_floor(requirement):
    match = re.match(r"[\s=^~>]*(\d+)\.(\d+)(?:\.(\d+))?", requirement)
    return tuple(int(part or 0) for part in match.groups()) if match else None


def fork_requirement(name, requirement):
    floor = version_floor(requirement)
    if floor is None:
        return None
    version = GETRANDOM_VERSIONS.get(floor[:2]) if name == "getrandom" else WASIX_VERSIONS.get(name)
    if version is None:
        return None
    available = tuple(map(int, version.split(".")))
    if floor[0] != available[0] or floor <= available:
        return None
    return version if name == "reqwest" else "=" + version + "+wasix.1"


def needs_registry_overlay(data):
    for table in dependency_tables(data):
        for alias, item in table.items():
            spec = {"version": item} if isinstance(item, str) else item
            name = spec.get("package", alias)
            if fork_requirement(name, spec.get("version", "*")):
                return True
    return False


def rewrite(data, packages, compatibility_source):
    changed = False
    reqwest_aliases = {}
    for table in dependency_tables(data):
        for alias, original in list(table.items()):
            spec = {"version": original} if isinstance(original, str) else dict(original)
            name = spec.get("package", alias)
            requirement = spec.get("version", "*")
            floor = version_floor(requirement)
            if "git" in spec:
                def matches(package):
                    source = package.get("source")
                    if source:
                        parsed = urlsplit(source.removeprefix("git+"))
                        url = parsed._replace(query="", fragment="").geturl()
                        if url.rstrip("/").removesuffix(".git") != spec["git"].rstrip("/").removesuffix(".git"):
                            return False
                        refs = parse_qs(parsed.query)
                        if any(spec[k] not in refs.get(k, []) for k in ("branch", "tag") if k in spec):
                            return False
                        if "rev" in spec and spec["rev"] not in refs.get("rev", []) and not parsed.fragment.startswith(spec["rev"]):
                            return False
                    return package["name"] == name and version_matches(requirement, package["version"])

                candidates = [p for p in packages if matches(p)]
                if len(candidates) == 1:
                    spec = {k: v for k, v in spec.items() if k not in {"git", "rev", "branch", "tag", "registry"}}
                    spec["path"] = str(candidates[0]["staged_manifest"].parent)
            adapted_version = fork_requirement(name, requirement)
            if adapted_version and (compatibility_source or name == "reqwest"):
                spec["version"] = adapted_version
            if name == "reqwest" and floor and floor[:2] == (0, 13):
                reqwest_aliases[alias] = reqwest_aliases.get(alias, False) or spec.get("optional", False)
                if "features" in spec:
                    # reqwest 0.12 names TLS differently; form/query are ungated.
                    spec["features"] = [REQWEST_TLS.get(f, f) for f in spec["features"] if f not in {"form", "query"}]
            if spec != ({"version": original} if isinstance(original, str) else original):
                table[alias] = spec
                changed = True
    for feature, entries in data.get("features", {}).items():
        adapted = []
        for entry in entries:
            dependency, separator, forwarded = entry.partition("/")
            alias = dependency.removesuffix("?")
            if separator and alias in reqwest_aliases:
                if forwarded in {"form", "query"}:
                    if not dependency.endswith("?") and reqwest_aliases[alias]:
                        adapted.append("dep:" + alias)
                    continue
                entry = dependency + "/" + REQWEST_TLS.get(forwarded, forwarded)
            adapted.append(entry)
        if adapted != entries:
            data["features"][feature] = adapted
            changed = True
    return changed


def prepare(metadata_path, output):
    if not metadata_path.is_absolute() or not output.is_absolute():
        raise ValueError("--metadata and --output must be absolute paths")
    metadata = json.loads(metadata_path.read_text())
    native = Path(metadata["workspace_root"]).resolve()
    output = output.resolve()
    git_roots = {}
    registry_roots = {}
    registry_packages = []
    packages = []
    for package in metadata["packages"]:
        manifest = Path(package["manifest_path"]).resolve()
        if str(package.get("source", "")).startswith("registry+") and package["name"] not in {*WASIX_VERSIONS, "getrandom"}:
            if (HTTP_OVERLAYS.get(package["name"]) == package["version"]
                    or needs_registry_overlay(tomllib.loads(manifest.read_text()))):
                staged = output / "registry" / f"{package['name']}-{package['version']}"
                registry_roots[manifest.parent] = staged
                registry_packages.append({**package, "staged_manifest": staged / "Cargo.toml"})
        parts = manifest.parts
        index = next((i for i in range(1, len(parts) - 2) if parts[i - 1:i + 1] == ("git", "checkouts")), None)
        if index is None:
            continue
        root = Path(*parts[:index + 3])
        staged = output / "git" / root.parent.name / root.name
        git_roots[root] = staged
        packages.append({**package, "staged_manifest": staged / manifest.relative_to(root)})
    dependency_roots = {**git_roots, **registry_roots}
    copied = [(native / "crates", output / "crates"), (native / ".cargo", output / ".cargo"), *dependency_roots.items()]
    if native.is_relative_to(output) or any(output.is_relative_to(src) or src.is_relative_to(output) for src, _ in copied):
        raise ValueError("Output must be separate from native source directories and cargo checkouts")
    # Repeated copies must not follow a destination link back into native sources.
    for directory, children, files in os.walk(output):
        children[:] = [name for name in children if not excluded(name)]
        if any((Path(directory) / name).is_symlink() for name in [*children, *files] if not excluded(name)):
            raise ValueError("Staging output must not contain symbolic links")

    output.mkdir(parents=True, exist_ok=True)
    for name in ("Cargo.toml", "Cargo.lock"):
        source = native / name
        if source.exists():
            copy_source(source, output / name)
    files = [(output / "Cargo.toml", None)]
    for source, staged in copied:
        if source.exists():
            shutil.copytree(source, staged, dirs_exist_ok=True, copy_function=copy_source, ignore=lambda _, names: [n for n in names if excluded(n)])
            files.extend((staged / f.relative_to(source), staged if source in dependency_roots else None) for f in manifests(source))
    documents = {manifest: tomllib.loads(manifest.read_text()) for manifest, _ in files}
    # Standalone dependency packages need a boundary before enclosing o11y is considered.
    standalone = {root / "Cargo.toml" for root in dependency_roots.values() if "workspace" not in documents[root / "Cargo.toml"]}
    for manifest in standalone:
        documents[manifest]["workspace"] = {}
    workspaces = {manifest.parent for manifest, data in documents.items() if "workspace" in data}
    changed = 0
    for manifest, dependency_root in files:
        data = documents[manifest]
        modified = rewrite(data, packages, dependency_root is not None) or manifest in standalone
        if manifest == output / "Cargo.toml":
            excludes = data["workspace"].setdefault("exclude", [])
            for name in ["git", *(["registry"] if registry_packages else [])]:
                if name not in excludes:
                    excludes.append(name)
                    modified = True
            patches = data.setdefault("patch", {}).setdefault("crates-io", {})
            for package in registry_packages:
                version_key = re.sub(r"[^A-Za-z0-9_-]", "-", package["version"])
                alias = f"wasix-{package['name']}-{version_key}"
                patch = {"package": package["name"], "version": "=" + package["version"], "path": str(package["staged_manifest"].parent)}
                if alias in patches and patches[alias] != patch:
                    raise ValueError(f"Existing patch conflicts with registry overlay: {alias}")
                patches[alias] = patch
                modified = True
        if dependency_root and "package" in data and "workspace" not in data:
            workspace = next(parent for parent in manifest.parents if parent in workspaces and parent.is_relative_to(dependency_root))
            if data["package"].get("workspace") != str(workspace):
                data["package"]["workspace"] = str(workspace)
                modified = True
        if modified:
            text = toml_text(data)
            if tomllib.loads(text) != data:
                raise ValueError(f"TOML roundtrip failed: {manifest}")
            manifest.write_text(text)
            changed += 1
    for workspace in workspaces:
        (workspace / "Cargo.lock").unlink(missing_ok=True)
        config = workspace / ".cargo" / "config.toml"
        data = tomllib.loads(config.read_text()) if config.exists() else {}
        sources = data.setdefault("source", {})
        sources.setdefault("crates-io", {})["replace-with"] = "wasix"
        sources["wasix"] = {"registry": REGISTRY}
        data.setdefault("net", {})["git-fetch-with-cli"] = True
        config.parent.mkdir(parents=True, exist_ok=True)
        config.write_text(toml_text(data))
    return {"output": str(output), "git_roots": len(git_roots), "registry_packages": len(registry_packages), "rewritten_manifests": changed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.metadata, args.output)))
