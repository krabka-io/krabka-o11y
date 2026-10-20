#!/usr/bin/env python3
"""Validate completed lab outputs before Cargo or the runtime build reuses them."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import time


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def key(site, kind):
    names = ["scripts/lab-cache.py", "scripts/build-lab.sh", "scripts/build-runtime.sh", "wasi/sdk.Cargo.lock"]
    if kind == "runtime":
        names += ["wasi/wasmer-network.patch", "wasi/wasmer-address.patch", "wasi/wasmer-udp-timeout.patch"]
    else:
        names += ["scripts/prepare-wasix.py", "scripts/port-wasix.py",
                  "scripts/prepare-wasix-std.py"]
        names += [str(path.relative_to(site)) for path in sorted((site / "wasi").glob("*")) if path.is_file()]
    inputs = {name: digest(site / name) for name in sorted(set(names))}
    inputs["rust-toolchain.toml"] = digest(site.parent / "rust-toolchain.toml")
    inputs["compilerEnvironment"] = {name: value for name, value in os.environ.items()
                                     if name in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"} or name.startswith("CARGO_PROFILE_")}
    if kind == "optimized":
        inputs["profile"] = [os.environ.get("LAB_OPT_LEVEL", "s"), os.environ.get("LAB_LTO", "")]
    return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()


def runtime_files(sdk):
    paths = [sdk / "Cargo.lock", sdk / "LICENSE"]
    for directory in [sdk / "js/dist", sdk / "js/pkg"]:
        paths += sorted(path for path in directory.rglob("*") if path.is_file())
    return {str(path.relative_to(sdk)): digest(path) for path in paths}


def runtime_valid(site, sdk):
    try:
        record = json.loads((sdk / ".krabka-runtime-build.json").read_text())
        for name in ["Cargo.lock", "LICENSE", "js/dist/index.js", "js/pkg/wasmer_sdk_js_bg.wasm"]:
            if not (sdk / name).is_file():
                return False
        return record["buildKey"] == key(site, "runtime") and record["files"] == runtime_files(sdk)
    except (OSError, ValueError, KeyError):
        return False


def write_record(path, record):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(record, indent=2) + "\n")
    temporary.replace(path)


def seal_runtime(site, sdk):
    path = sdk / ".krabka-runtime-build.json"
    record = json.loads(path.read_text())
    record.update(buildKey=key(site, "runtime"), files=runtime_files(sdk))
    write_record(path, record)


def optimized_files(release):
    return {path.name: {"output": digest(path), "input": digest(path.with_name(path.name.replace(".wasi.wasm", ".rustc.wasm")))}
            for path in sorted(release.glob("*.wasi.wasm"))}


def check_optimized(site, target):
    release = target / "wasm32-wasmer-wasi/release"
    try:
        record = json.loads((target / ".lab-optimized.json").read_text())
        files = record["files"] if record["buildKey"] == key(site, "optimized") else {}
    except (OSError, ValueError, KeyError):
        files = {}
    for path in release.glob("*.wasi.wasm"):
        try:
            actual = {"output": digest(path), "input": digest(path.with_name(path.name.replace(".wasi.wasm", ".rustc.wasm")))}
            valid = files.get(path.name) == actual
        except OSError:
            valid = False
        if not valid:
            print(f"Discarding incomplete or changed optimized module: {path.name}")
            path.unlink()


def seal_optimized(site, target):
    write_record(target / ".lab-optimized.json", {
        "buildKey": key(site, "optimized"),
        "files": optimized_files(target / "wasm32-wasmer-wasi/release"),
    })


def source_stamps(site):
    # Fresh checkouts and platform adaptations change mtimes even when their
    # final content is unchanged. Keep Cargo inputs as old as the cached build.
    tools = site / ".tools"
    path = tools / "source-stamps.json"
    try:
        previous = json.loads(path.read_text())
    except (OSError, ValueError):
        previous = {}
    current = {}
    for root in [tools / "wasi-source", tools / "wasix-rust/lib/rustlib/src/rust/library"]:
        for directory, children, files in os.walk(root):
            children[:] = [name for name in children if name not in {".git", "target"}]
            for name in files:
                source = Path(directory) / name
                if source.is_symlink():
                    continue
                relative = str(source.relative_to(tools))
                content = digest(source)
                old = previous.get(relative, {})
                timestamp = old.get("mtime") if old.get("sha256") == content else time.time_ns()
                os.utime(source, ns=(timestamp, timestamp))
                current[relative] = {"sha256": content, "mtime": timestamp}
    write_record(path, current)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["runtime-key", "runtime-check", "runtime-record", "optimized-check", "optimized-record", "source-stamps"])
    parser.add_argument("--site", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    sdk = args.site / ".tools/wasmer-sdk"
    target = args.site / ".tools/wasi-target"
    if args.command == "runtime-key":
        print(key(args.site, "runtime"))
    elif args.command == "runtime-check":
        raise SystemExit(0 if runtime_valid(args.site, sdk) else 1)
    elif args.command == "runtime-record":
        seal_runtime(args.site, sdk)
    elif args.command == "optimized-check":
        check_optimized(args.site, target)
    elif args.command == "source-stamps":
        source_stamps(args.site)
    else:
        seal_optimized(args.site, target)
