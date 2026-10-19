#!/usr/bin/env python3
"""Check reuse and invalidation of completed lab builds."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("lab_cache", Path(__file__).with_name("lab-cache.py"))
cache = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cache)


def write(path, content="fixture"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


class LabCacheTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.site = Path(directory.name) / "website"
        self.tools = self.site / ".tools"
        for name in ["lab-cache.py", "build-runtime.sh", "build-lab.sh", "prepare-wasix.py",
                     "port-wasix.py", "prepare-wasix-std.py"]:
            write(self.site / "scripts" / name)
        for name in ["sdk.Cargo.lock", "std.Cargo.lock", "wasmer-network.patch", "wasmer-address.patch",
                     "wasmer-udp-timeout.patch", "wasix-std-network.patch"]:
            write(self.site / "wasi" / name)
        write(self.site.parent / "rust-toolchain.toml")

    def runtime(self):
        sdk = self.tools / "wasmer-sdk"
        for name in ["Cargo.lock", "LICENSE", "js/dist/index.js", "js/dist/worker.js", "js/pkg/wasmer_sdk_js_bg.wasm"]:
            write(sdk / name)
        write(sdk / ".krabka-runtime-build.json", json.dumps({"revision": "pinned", "wasmSha256": "fixture"}))
        cache.seal_runtime(self.site, sdk)
        return sdk

    def test_runtime_reuse_without_source_checkout(self):
        sdk = self.runtime()
        self.assertTrue(cache.runtime_valid(self.site, sdk))
        self.assertFalse((sdk / ".git").exists())

    def test_runtime_script_stages_cached_outputs_without_installing_tools(self):
        for name in ["build-runtime.sh", "lab-cache.py"]:
            shutil.copy2(Path(__file__).with_name(name), self.site / "scripts" / name)
        sdk = self.runtime()
        subprocess.run(["bash", str(self.site / "scripts/build-runtime.sh")], check=True, capture_output=True)
        self.assertEqual((self.site / "static/lab/sdk/dist/worker.js").read_bytes(),
                         (sdk / "js/dist/worker.js").read_bytes())
        self.assertFalse((sdk / ".git").exists())

    def test_runtime_rejects_missing_changed_or_extra_outputs(self):
        for change in ["missing", "changed", "extra"]:
            with self.subTest(change=change):
                sdk = self.runtime()
                worker = sdk / "js/dist/worker.js"
                if change == "missing":
                    worker.unlink()
                elif change == "changed":
                    worker.write_text("corrupt")
                else:
                    write(sdk / "js/dist/extra.js")
                self.assertFalse(cache.runtime_valid(self.site, sdk))
                (sdk / "js/dist/extra.js").unlink(missing_ok=True)

    def test_runtime_input_changes_invalidate_cache(self):
        for name in ["wasi/sdk.Cargo.lock", "wasi/wasmer-network.patch", "scripts/build-runtime.sh"]:
            with self.subTest(name=name):
                sdk = self.runtime()
                write(self.site / name, "new input")
                self.assertFalse(cache.runtime_valid(self.site, sdk))

    def test_service_edits_do_not_invalidate_runtime(self):
        sdk = self.runtime()
        write(self.site.parent / "crates/traces/src/lib.rs", "changed service")
        write(self.site / "wasi/std.Cargo.lock", "changed std")
        self.assertTrue(cache.runtime_valid(self.site, sdk))

    def test_runtime_compiler_flags_invalidate_cache(self):
        sdk = self.runtime()
        with patch.dict(os.environ, {"RUSTFLAGS": "-C opt-level=3"}):
            self.assertFalse(cache.runtime_valid(self.site, sdk))

    def optimized(self):
        target = self.tools / "wasi-target"
        release = target / "wasm32-wasmer-wasi/release"
        for name in ["service", "broker"]:
            write(release / f"{name}.wasi.wasm", f"optimized {name}")
            write(release / f"{name}.rustc.wasm", f"raw {name}")
        cache.seal_optimized(self.site, target)
        return target, release

    def test_completed_optimizations_are_kept(self):
        target, release = self.optimized()
        cache.check_optimized(self.site, target)
        self.assertEqual(sorted(path.name for path in release.glob("*.wasi.wasm")), ["broker.wasi.wasm", "service.wasi.wasm"])

    def test_interrupted_or_unknown_optimization_is_removed(self):
        for name in ["service.wasi.wasm", "service.rustc.wasm", "unknown.wasi.wasm"]:
            with self.subTest(name=name):
                target, release = self.optimized()
                write(release / name, "partial")
                cache.check_optimized(self.site, target)
                self.assertTrue((release / "broker.wasi.wasm").exists())
                removed = "unknown.wasi.wasm" if name.startswith("unknown") else "service.wasi.wasm"
                self.assertFalse((release / removed).exists())

    def test_changed_optimization_profile_invalidates_sidecars(self):
        target, release = self.optimized()
        with patch.dict(os.environ, {"LAB_OPT_LEVEL": "3"}):
            cache.check_optimized(self.site, target)
        self.assertEqual(list(release.glob("*.wasi.wasm")), [])

    def test_missing_completion_record_invalidates_sidecars(self):
        target, release = self.optimized()
        (target / ".lab-optimized.json").unlink()
        cache.check_optimized(self.site, target)
        self.assertEqual(list(release.glob("*.wasi.wasm")), [])

    def test_unchanged_staged_content_keeps_cached_timestamp(self):
        source = write(self.tools / "wasi-source/crates/demo/src/lib.rs", "old")
        cache.source_stamps(self.site)
        timestamp = source.stat().st_mtime_ns
        source.unlink()
        write(source, "old")
        os.utime(source, ns=(timestamp + 1_000_000_000, timestamp + 1_000_000_000))
        cache.source_stamps(self.site)
        self.assertEqual(source.stat().st_mtime_ns, timestamp)
        write(source, "new")
        os.utime(source, ns=(1, 1))
        cache.source_stamps(self.site)
        self.assertGreater(source.stat().st_mtime_ns, timestamp)


if __name__ == "__main__":
    unittest.main()
