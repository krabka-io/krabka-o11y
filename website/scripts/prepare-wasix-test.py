#!/usr/bin/env python3
"""Run with python3 -B website/scripts/prepare-wasix-test.py."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest


SCRIPT = Path(__file__).with_name("prepare-wasix.py")
SPEC = importlib.util.spec_from_file_location("prepare_wasix", SCRIPT)
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def fingerprints(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob("*") if p.is_file()}


class PrepareWasixTest(unittest.TestCase):
    def test_staged_content_with_old_source_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            native = Path(directory) / "native.rs"
            staged = Path(directory) / "staged.rs"
            native.write_bytes(b"old\n")
            staged.write_bytes(b"old\n")
            source_mtime = 1_600_000_000_000_000_000
            staged_mtime = source_mtime + 1_000_000_000
            os.utime(native, ns=(source_mtime, source_mtime))
            os.utime(staged, ns=(staged_mtime, staged_mtime))
            helper.copy_source(native, staged)
            self.assertEqual(staged.stat().st_mtime_ns, staged_mtime)

            native.write_bytes(b"new\n")
            os.utime(native, ns=(source_mtime, source_mtime))
            copied_after = time.time_ns()
            helper.copy_source(native, staged)
            self.assertEqual(staged.read_bytes(), b"new\n")
            self.assertGreaterEqual(staged.stat().st_mtime_ns, copied_after)
            self.assertLessEqual(staged.stat().st_mtime_ns, time.time_ns())
            self.assertEqual(native.read_bytes(), b"new\n")
            self.assertEqual(native.stat().st_mtime_ns, source_mtime)
            unchanged_mtime = staged.stat().st_mtime_ns
            helper.copy_source(native, staged)
            self.assertEqual(staged.stat().st_mtime_ns, unchanged_mtime)

    def test_roundtrip(self):
        data = tomllib.loads('''
title = "quoted \\\"text\\\"\\n"
date = 2026-10-05
instant = 2026-10-05T10:20:30Z
clock = 10:20:30
ratio = -0.5
[target.'cfg(target_os = "wasi")'.dependencies]
renamed = { package = "original", version = "1", features = ["a"], default-features = false }
[[bin]]
name = "first"
[bin.metadata]
label = "one"
[[bin.metadata.items]]
value = 1
[[bin.metadata.items]]
value = 2
[[bin]]
name = "second"
''')
        self.assertEqual(tomllib.loads(helper.toml_text(data)), data)

    def test_staging(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            native = base / "native"
            engine = base / "cache/git/checkouts/engine-123/abc123"
            store = base / "cache/git/checkouts/store-456/def456"
            registry = base / "cache/registry/src/example"
            write(native / "Cargo.toml", '''
[workspace]
members = ["crates/*"]
exclude = ["benches"]
[workspace.package]
rust-version = "1.98.1"
[workspace.dependencies]
engine_alias = { package = "engine", git = "https://example.org/engine", rev = "abc123", version = "54", features = ["query"] }
reqwest = { version = "0.13", features = ["blocking", "rustls", "form", "query"] }
tokio = { version = "1", default-features = false }
[patch.crates-io]
object_store = { git = "https://example.org/store", rev = "def456" }
unused = { git = "https://example.org/unused", rev = "unknown" }
''')
            write(native / "Cargo.lock", "native lock\n")
            write(native / ".cargo/config.toml", '[build]\nrustflags = ["--cfg", "tokio_unstable"]\n[net]\ngit-fetch-with-cli = false\n')
            write(native / "crates/app/Cargo.toml", '[package]\nname = "app"\nversion = "1.0.0"\n')
            write(native / "crates/app/src/lib.rs", "pub fn native() {}\n")
            write(engine / "Cargo.toml", '''
[workspace]
members = ["core", "support"]
exclude = ["isolated"]
[workspace.package]
homepage = "https://example.org/engine"
[workspace.dependencies]
tokio = { version = "1.52", features = ["rt-multi-thread"] }
object_store = { version = "0.14", features = ["aws"], default-features = false }
random-generation = { package = "getrandom", version = "0.3.4", features = ["wasm_js"] }
''')
            write(engine / "Cargo.lock", "git native lock\n")
            write(engine / "core/Cargo.toml", '''
[package]
name = "engine"
version = "54.1.0"
homepage.workspace = true
[dependencies]
support = { path = "../support" }
reqwest = "0.13"
object_store = { version = "0.14", features = ["tokio"] }
object_store_013 = { package = "object_store", version = "0.13", optional = true }
[target.'cfg(target_os = "wasi")'.dependencies]
tokio = { version = ">=1.52, <2", features = ["fs"] }
''')
            write(engine / "core/src/lib.rs", "pub fn engine() {}\n")
            write(engine / "support/Cargo.toml", '[package]\nname = "support"\nversion = "1.0.0"\nhomepage.workspace = true\n[dependencies]\nreqwest = { version = "0.12", features = ["rustls-tls"] }\n')
            write(engine / "isolated/Cargo.toml", '[package]\nname = "isolated"\nversion = "1.0.0"\n[workspace]\nmembers = ["child"]\n')
            write(engine / "isolated/child/Cargo.toml", '[package]\nname = "child"\nversion = "1.0.0"\n[dependencies]\nreqwest = { version = "0.13", features = ["rustls-no-provider", "json"] }\n')
            write(store / "Cargo.toml", '[package]\nname = "object_store"\nversion = "0.13.2"\n')
            write(registry / "opentelemetry-otlp-0.33.0/Cargo.toml", '''
[package]
name = "opentelemetry-otlp"
version = "0.33.0"
[dependencies]
web-client = { package = "reqwest", version = "^0.13.1", optional = true, features = ["json", "rustls", "query"] }
old-client = { package = "reqwest", version = "0.12", features = ["rustls-tls"] }
[features]
tls = ["web-client?/rustls", "old-client/rustls-tls"]
form = ["web-client/form"]
query = ["web-client?/query"]
''')
            write(registry / "opentelemetry-otlp-0.33.0/src/lib.rs", "pub fn native_exporter() {}\n")
            registry_versions = [("opentelemetry-otlp", "0.33.0")]
            for name, version in helper.HTTP_OVERLAYS.items():
                write(registry / f"{name}-{version}/Cargo.toml", f'[package]\nname = "{name}"\nversion = "{version}"\n[dependencies]\ntokio = "1"\n')
                registry_versions.append((name, version))
            write(registry / "object_store-0.14.2/Cargo.toml", '[package]\nname = "object_store"\nversion = "0.14.2"\n[dependencies]\nreqwest = { version = "0.13", features = ["rustls"] }\n')
            registry_versions.append(("object_store", "0.14.2"))
            write(registry / "referencing-0.58.4/Cargo.toml", '[package]\nname = "referencing"\nversion = "0.58.4"\n[target.\'cfg(target_arch = "wasm32")\'.dependencies]\ngetrandom = { version = "0.3.4", features = ["wasm_js"] }\n')
            write(registry / "compat-platform-1.0.0/Cargo.toml", '''
[package]
name = "compat-platform"
version = "1.0.0"
[dependencies]
tokio = "1.50"
hyper = { version = "1.8.0", features = ["http2"] }
mio = { version = "1.2.0", features = ["os-poll"] }
reqwest = { version = "0.12.23", features = ["rustls-tls"] }
random-old = { package = "getrandom", version = "0.2.15" }
random-new = { package = "getrandom", version = "0.4.3" }
''')
            registry_versions.extend([("referencing", "0.58.4"), ("compat-platform", "1.0.0")])
            for version in ("1.0.0", "2.0.0"):
                write(registry / f"compat-clock-{version}/Cargo.toml", f'[package]\nname = "compat-clock"\nversion = "{version}"\n[dependencies]\ntokio = {{ version = "1.53", features = ["time"] }}\n')
                registry_versions.append(("compat-clock", version))
            for name, version, dependency in [("unchanged", "1.0.0", 'tokio = "1.29"'), ("reqwest", "0.13.5", 'tokio = "1.53"'), ("tokio", "1.53.0", 'reqwest = "0.13"')]:
                write(registry / f"{name}-{version}/Cargo.toml", f'[package]\nname = "{name}"\nversion = "{version}"\n[dependencies]\n{dependency}\n')
                registry_versions.append((name, version))
            for name in (".git", "target", ".tools", "node_modules", "bazel-out"):
                write(engine / name / "omit", "excluded\n")
                write(native / "crates/app" / name / "omit", "excluded\n")
            metadata = base / "metadata.json"
            metadata.write_text(json.dumps({"workspace_root": str(native), "packages": [
                {"name": "engine", "version": "54.1.0", "manifest_path": str(engine / "core/Cargo.toml"), "source": "git+https://example.org/engine?rev=abc123#abc123"},
                {"name": "object_store", "version": "0.13.2", "manifest_path": str(store / "Cargo.toml"), "source": "git+https://example.org/store?rev=def456#def456"},
                *[{"name": name, "version": version, "manifest_path": str(registry / f"{name}-{version}/Cargo.toml"), "source": "registry+https://example.org/index"} for name, version in registry_versions],
            ]}))
            before = fingerprints(native), fingerprints(base / "cache")
            output = base / "staged"
            subprocess.run([sys.executable, "-B", str(SCRIPT), "--metadata", str(metadata), "--output", str(output)], check=True, capture_output=True, text=True)
            staged_engine = output / "git/engine-123/abc123"
            staged_store = output / "git/store-456/def456"
            root = tomllib.loads((output / "Cargo.toml").read_text())
            dep = root["workspace"]["dependencies"]["engine_alias"]
            self.assertEqual(dep["path"], str(staged_engine / "core"))
            self.assertNotIn("git", dep)
            self.assertEqual(dep["features"], ["query"])
            self.assertEqual(root["workspace"]["package"]["rust-version"], "1.98.1")
            self.assertEqual(root["workspace"]["exclude"], ["benches", "git", "registry"])
            self.assertEqual(root["workspace"]["dependencies"]["tokio"]["version"], "1")
            self.assertEqual(root["workspace"]["dependencies"]["reqwest"], {"version": "0.12.22", "features": ["blocking", "rustls-tls"]})
            self.assertEqual(root["patch"]["crates-io"]["object_store"]["path"], str(staged_store))
            staged_store_014 = output / "registry/object_store-0.14.2"
            self.assertEqual(root["patch"]["crates-io"]["wasix-object_store-0-14-2"], {"package": "object_store", "version": "=0.14.2", "path": str(staged_store_014)})
            store_014 = tomllib.loads((staged_store_014 / "Cargo.toml").read_text())
            self.assertEqual(store_014["package"]["version"], "0.14.2")
            self.assertEqual(store_014["dependencies"]["reqwest"], {"version": "0.12.22", "features": ["rustls-tls"]})
            self.assertEqual(root["patch"]["crates-io"]["unused"], {"git": "https://example.org/unused", "rev": "unknown"})
            staged_otlp = output / "registry/opentelemetry-otlp-0.33.0"
            self.assertEqual(root["patch"]["crates-io"]["wasix-opentelemetry-otlp-0-33-0"], {"package": "opentelemetry-otlp", "version": "=0.33.0", "path": str(staged_otlp)})
            otlp = tomllib.loads((staged_otlp / "Cargo.toml").read_text())
            self.assertEqual(otlp["package"]["version"], "0.33.0")
            self.assertIn("workspace", otlp)
            self.assertNotIn("workspace", otlp["package"])
            self.assertEqual(otlp["dependencies"]["web-client"]["version"], "0.12.22")
            self.assertEqual(otlp["dependencies"]["web-client"]["features"], ["json", "rustls-tls"])
            self.assertEqual(otlp["features"], {"tls": ["web-client?/rustls-tls", "old-client/rustls-tls"], "form": ["dep:web-client"], "query": []})
            self.assertEqual((staged_otlp / "src/lib.rs").read_bytes(), (registry / "opentelemetry-otlp-0.33.0/src/lib.rs").read_bytes())
            referencing = tomllib.loads((output / "registry/referencing-0.58.4/Cargo.toml").read_text())
            self.assertEqual(referencing["target"]['cfg(target_arch = "wasm32")']["dependencies"]["getrandom"], {"version": "=0.3.3+wasix.1", "features": ["wasm_js"]})
            platform = tomllib.loads((output / "registry/compat-platform-1.0.0/Cargo.toml").read_text())["dependencies"]
            self.assertEqual(platform["tokio"]["version"], "=1.47.0+wasix.1")
            self.assertEqual(platform["hyper"], {"version": "1.8.0", "features": ["http2"]})
            self.assertEqual(platform["mio"], {"version": "=1.0.3+wasix.1", "features": ["os-poll"]})
            self.assertEqual(platform["reqwest"], {"version": "0.12.22", "features": ["rustls-tls"]})
            self.assertEqual(platform["random-old"], {"package": "getrandom", "version": "0.2.15"})
            self.assertEqual(platform["random-new"], {"package": "getrandom", "version": "0.4.3"})
            for version in ("1.0.0", "2.0.0"):
                clock = output / f"registry/compat-clock-{version}"
                self.assertEqual(root["patch"]["crates-io"][f"wasix-compat-clock-{version.replace('.', '-')}"]["path"], str(clock))
                self.assertEqual(tomllib.loads((clock / "Cargo.toml").read_text())["dependencies"]["tokio"], {"version": "=1.47.0+wasix.1", "features": ["time"]})
            http_sources = {f"{name}-{version}" for name, version in helper.HTTP_OVERLAYS.items()}
            self.assertEqual({p.name for p in (output / "registry").iterdir()}, {"opentelemetry-otlp-0.33.0", "object_store-0.14.2", "compat-clock-1.0.0", "compat-clock-2.0.0", "referencing-0.58.4", "compat-platform-1.0.0"} | http_sources)
            for name, version in helper.HTTP_OVERLAYS.items():
                staged = output / f"registry/{name}-{version}"
                alias = f"wasix-{name}-{version.replace('.', '-')}"
                self.assertEqual(root["patch"]["crates-io"][alias], {"package": name, "version": "=" + version, "path": str(staged)})
                self.assertEqual(tomllib.loads((staged / "Cargo.toml").read_text())["dependencies"]["tokio"], "1")
            deps = tomllib.loads((staged_engine / "Cargo.toml").read_text())["workspace"]["dependencies"]
            self.assertEqual(deps["tokio"], {"version": "=1.47.0+wasix.1", "features": ["rt-multi-thread"]})
            self.assertEqual(deps["object_store"], {"version": "0.14", "features": ["aws"], "default-features": False})
            self.assertEqual(deps["random-generation"], {"package": "getrandom", "version": "=0.3.3+wasix.1", "features": ["wasm_js"]})
            core = tomllib.loads((staged_engine / "core/Cargo.toml").read_text())
            self.assertEqual(core["package"]["workspace"], str(staged_engine))
            self.assertTrue(core["package"]["homepage"]["workspace"])
            self.assertEqual(tomllib.loads((staged_engine / "Cargo.toml").read_text())["workspace"]["package"]["homepage"], "https://example.org/engine")
            support = tomllib.loads((staged_engine / "support/Cargo.toml").read_text())
            self.assertEqual(support["package"]["workspace"], str(staged_engine))
            self.assertEqual(support["dependencies"]["reqwest"], {"version": "0.12", "features": ["rustls-tls"]})
            standalone = tomllib.loads((staged_store / "Cargo.toml").read_text())
            self.assertIn("workspace", standalone)
            self.assertNotIn("workspace", standalone["package"])
            nested = tomllib.loads((staged_engine / "isolated/child/Cargo.toml").read_text())
            self.assertEqual(nested["package"]["workspace"], str(staged_engine / "isolated"))
            self.assertEqual(nested["dependencies"]["reqwest"]["features"], ["rustls-tls-no-provider", "json"])
            self.assertNotIn("workspace", tomllib.loads((staged_engine / "isolated/Cargo.toml").read_text())["package"])
            self.assertEqual(core["dependencies"]["reqwest"]["version"], "0.12.22")
            self.assertEqual(core["dependencies"]["object_store"], {"version": "0.14", "features": ["tokio"]})
            self.assertEqual(core["dependencies"]["object_store_013"], {"package": "object_store", "version": "0.13", "optional": True})
            self.assertEqual(core["dependencies"]["support"]["path"], "../support")
            self.assertTrue((staged_engine / "core" / core["dependencies"]["support"]["path"] / "Cargo.toml").exists())
            self.assertEqual(core["target"]['cfg(target_os = "wasi")']["dependencies"]["tokio"]["version"], "=1.47.0+wasix.1")
            self.assertEqual((staged_engine / "core/src/lib.rs").read_bytes(), (engine / "core/src/lib.rs").read_bytes())
            for workspace in (output, staged_engine, staged_store, staged_otlp):
                self.assertFalse((workspace / "Cargo.lock").exists())
                config = tomllib.loads((workspace / ".cargo/config.toml").read_text())
                self.assertEqual(config["source"]["crates-io"]["replace-with"], "wasix")
                self.assertEqual(config["source"]["wasix"]["registry"], helper.REGISTRY)
                self.assertTrue(config["net"]["git-fetch-with-cli"])
            self.assertEqual(tomllib.loads((output / ".cargo/config.toml").read_text())["build"]["rustflags"], ["--cfg", "tokio_unstable"])
            self.assertFalse(any(p.name == "omit" for p in output.rglob("*")))
            write(output / "keep-user-file", "retained\n")
            helper.prepare(metadata, output)
            self.assertEqual((output / "keep-user-file").read_text(), "retained\n")
            with self.assertRaises(ValueError):
                helper.prepare(metadata, native)
            linked = base / "linked"
            linked.mkdir()
            (linked / "Cargo.toml").symlink_to(native / "crates/app/Cargo.toml")
            with self.assertRaises(ValueError):
                helper.prepare(metadata, linked)
            self.assertEqual((fingerprints(native), fingerprints(base / "cache")), before)
            cargo = shutil.which("cargo")
            if cargo:
                result = subprocess.run([cargo, "metadata", "--offline", "--no-deps", "--format-version", "1", "--manifest-path", str(output / "Cargo.toml")], cwd=output, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout)["workspace_root"], str(output))


if __name__ == "__main__":
    unittest.main()
