#!/usr/bin/env python3
"""Check compiler actions: python3 tools/check-image-lto.py FRESH_OUTPUT_DIR."""

import json
from pathlib import Path
import subprocess
import sys

if not __debug__:
    raise SystemExit("Run without Python optimization flags.")
if len(sys.argv) != 2:
    raise SystemExit(__doc__)
output = Path(sys.argv[1]).resolve()
output.mkdir(parents=True)
root = Path(__file__).resolve().parent.parent
image = "deps(//bazel/images/krabka:image)"
queries = {
    "release": (image, ["-c", "opt"]),
    "fastbuild": (image, ["-c", "fastbuild"]),
    "tests": ("deps(//crates/promql:promql_test)", ["-c", "opt"]),
    "off": (image, ["-c", "opt", "--@rules_rust//rust/settings:lto=off"]),
}
binaries = {
    "//crates/metrics:krabka-metrics",
    "//crates/metrics-service:krabka-metrics-service",
    "//crates/observability:krabka-observability",
    "//crates/observability:krabka-o11y-bootstrap",
    "//crates/observability:krabka-o11y-recovery",
    "//crates/profiles:krabka-profiles",
    "//crates/traces:krabka-traces",
}
for name, (query, options) in queries.items():
    with (output / f"{name}.json").open("x") as result:
        with (output / f"{name}.log").open("x") as log:
            subprocess.run(
                ["bazel", "aquery", *options, query, "--output=jsonproto"],
                cwd=root, stdout=result, stderr=log, check=True,
            )
    graph = json.loads((output / f"{name}.json").read_text())
    labels = {target["id"]: target["label"] for target in graph["targets"]}
    configurations = {cfg["id"]: cfg for cfg in graph["configuration"]}
    actions = [action for action in graph["actions"] if action["mnemonic"] == "Rustc"]
    assert actions, name
    if name == "release":
        apps = {labels[a["targetId"]]: a for a in actions if labels[a["targetId"]] in binaries}
        assert set(apps) == binaries
        assert all("-Clto=thin" in action["arguments"] for action in apps.values())
        for action in actions:
            args = action["arguments"]
            if configurations[action["configurationId"]].get("isTool"):
                assert "-Clto=thin" not in args and "-Clinker-plugin-lto" not in args
            elif "--crate-type=rlib" in args or (
                "--crate-type" in args and args[args.index("--crate-type") + 1] == "rlib"
            ):
                assert "-Clinker-plugin-lto" in args
    else:
        assert all(
            not {"-Clto=thin", "-Clto=fat", "-Clinker-plugin-lto"}.intersection(action["arguments"])
            for action in actions
        ), name
        if name == "off":
            apps = [a for a in actions if labels[a["targetId"]] in binaries]
            assert len(apps) == len(binaries)
            assert all("-Clto=off" in action["arguments"] for action in apps)
print("All seven release binaries use ThinLTO; fast builds and tests retain their defaults; explicit off works.")
