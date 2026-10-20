#!/usr/bin/env python3
"""Check conservative selection of the documentation-only PR path."""

from pathlib import Path
import subprocess
import sys
import unittest


class LabCiTest(unittest.TestCase):
    def test_changed_paths(self):
        cases = [
            (["README.md", "docs/getting_started.md", "docs/diagram.svg", "crates/traces/README.md"], "false"),
            (["docs/guide with spaces.md"], "false"),
            (["crates/traces/test_coverage_report.md"], "false"),
            (["crates/traces/src/README.md"], "true"),
            ([], "true"),
            (["docs/guide.md", "crates/traces/src/lib.rs"], "true"),
            (["Cargo.lock"], "true"),
            (["Cargo.toml"], "true"),
            (["website/wasi/wasmer-network.patch"], "true"),
            (["website/scripts/build-lab.sh"], "true"),
            (["website/src/lab/runtime.js"], "true"),
            ([".github/workflows/ci.yml"], "true"),
            (["unknown"], "true"),
            (["docs/../crates/demo/src/lib.rs"], "true"),
            # A rename includes both the old Rust path and the new docs path.
            (["crates/demo/src/lib.rs", "docs/lib.md"], "true"),
        ]
        for paths, expected in cases:
            with self.subTest(paths=paths):
                result = subprocess.run(
                    [sys.executable, "-B", str(Path(__file__).with_name("lab-ci.py"))],
                    input="\0".join(paths).encode(), capture_output=True, check=True,
                )
                self.assertEqual(result.stdout.decode().strip(), expected)


if __name__ == "__main__":
    unittest.main()
