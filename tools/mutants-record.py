#!/usr/bin/env python3
"""Checks the Milestone 19 ratchet records, and records a manual sweep.

Two JSON files in `qualification/` are the evidence for two checked-in
baselines:

  * `milestone-19-mutation-baselines.json` for `tools/mutants-baseline.txt`
  * `milestone-19-benchmarks.json` for `tools/bench-baseline.txt`

A baseline number with no run behind it is a guess. `--check` makes sure that
each number has a run behind it, and that the run is described well enough to
repeat. It needs no sweep, no benchmark run and no toolchain:

    tools/mutants-record.py --check

It fails when:

  * a record does not name its commit, toolchain, host shape (OS, CPU count,
    memory), command, start time, duration, or artifact checksums
  * a mutation result's counts do not add up
  * a survivor count in `tools/mutants-baseline.txt` differs from its record,
    or a crate is `unseeded` in one file and seeded in the other
  * a recorded shard count differs from the `mutants_shards` in the crate's
    BUILD file
  * a crate with a mutants target has no baseline line
  * `tools/bench-baseline.txt` differs from the file the benchmark record
    checksummed, or the benchmark count differs
  * the `deliberate_survivor` block does not match a new run of
    `tools/mutants-ratchet.py --prove-gate`, or the `deliberate_regression`
    block is missing

A mutation result can hold a `provenance` block. Its fields replace the
top-level `commit`, `run_url`, `runner` and `toolchain` for that result only.

`--capture <crate>` records a sweep that `tools/mutants-sweep.sh` ran by hand
on a dedicated host. It reads the shard logs through the same structural gate
as `tools/mutants-ratchet.py`, and reads the `<crate>.metadata.txt` and
`<crate>.SHA256SUMS` files that the sweep script writes. It also takes the
checksum of the log archive. Then it prints the result entry. With `--write`,
it puts the entry in the record, removes the crate from `unseeded`, and writes
the survivor count into `tools/mutants-baseline.txt`. A sweep that is not
complete is not recorded, and the exit code is 3.

`--self-test` runs these checks against a synthetic repository.

Exit codes: 0 the records are valid, 1 a record is not valid, 2 the command
line is wrong, 3 `--capture` read a sweep that is not complete.
"""

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import re
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
MUTANTS_RECORD = "qualification/milestone-19-mutation-baselines.json"
BENCH_RECORD = "qualification/milestone-19-benchmarks.json"
MUTANTS_BASELINE = "tools/mutants-baseline.txt"
BENCH_BASELINE = "tools/bench-baseline.txt"

COMMIT = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
MUTANTS_OFF = re.compile(r"^\s*mutants\s*=\s*False\s*,", re.MULTILINE)
BAZEL_LABEL = re.compile(r"Build label: ([^;\s]+)")

EXIT_VALID = 0
EXIT_INVALID = 1
EXIT_USAGE = 2
EXIT_INCOMPLETE = 3


def load_tool(name):
    """Imports a sibling tool whose file name has a hyphen in it."""
    path = pathlib.Path(__file__).resolve().parent / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MUTANTS = load_tool("mutants-ratchet")
BENCH = load_tool("bench-ratchet")


class UsageError(Exception):
    """A fault in what the caller named. It exits 2."""


def sha256_file(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def load_json(path):
    try:
        return json.loads(pathlib.Path(path).read_text())
    except (OSError, ValueError) as error:
        raise UsageError(f"{path}: {error}") from None


def is_count(value, minimum=0):
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


# --- provenance --------------------------------------------------------------


def check_host(where, runner):
    """The faults in a host shape: label, OS, CPU count and memory."""
    if not isinstance(runner, dict):
        return [f"{where}: no runner, so the host shape is unknown"]
    faults = []
    for key in ("label", "os"):
        if not isinstance(runner.get(key), str) or not runner[key]:
            faults.append(f"{where}: runner.{key} is missing")
    for key in ("cpu_count", "memory_kib"):
        if not is_count(runner.get(key), 1):
            faults.append(f"{where}: runner.{key} must be a positive integer")
    return faults


def check_toolchain(where, toolchain, needed):
    if not isinstance(toolchain, dict):
        return [f"{where}: no toolchain"]
    return [
        f"{where}: toolchain.{key} is missing"
        for key in needed
        if not isinstance(toolchain.get(key), str) or not toolchain[key]
    ]


def check_run(where, entry, toolchain_keys):
    """The faults in the fields that every recorded run must name."""
    faults = []
    if not COMMIT.match(str(entry.get("commit", ""))):
        faults.append(f"{where}: commit must be a 40-character hex SHA")
    faults += check_host(where, entry.get("runner"))
    faults += check_toolchain(where, entry.get("toolchain"), toolchain_keys)
    if not isinstance(entry.get("command"), str) or not entry["command"].strip():
        faults.append(f"{where}: command is missing")
    if not TIMESTAMP.match(str(entry.get("started_at", ""))):
        faults.append(f"{where}: started_at must be a UTC timestamp")
    if not is_count(entry.get("duration_seconds"), 1):
        faults.append(f"{where}: duration_seconds must be a positive integer")
    return faults


def effective(record, result):
    """A mutation result with the record's run fields filled in beneath it."""
    merged = {key: record.get(key) for key in ("commit", "run_url", "runner", "toolchain")}
    merged.update(result.get("provenance") or {})
    merged.update({key: value for key, value in result.items() if key != "provenance"})
    return merged


# --- the mutation record -----------------------------------------------------


def mutants_crates(root):
    """Every crate whose BUILD file emits a `cargo_mutants_test` target."""
    found = set()
    for build in sorted((pathlib.Path(root) / "crates").glob("*/BUILD.bazel")):
        text = build.read_text()
        if "crate_tests(" in text and not MUTANTS_OFF.search(text):
            found.add(build.parent.name)
    return found


def check_mutants(root):
    root = pathlib.Path(root)
    record_path = root / MUTANTS_RECORD
    baseline_path = root / MUTANTS_BASELINE
    record = load_json(record_path)
    baseline = MUTANTS.read_baseline(baseline_path)
    faults = []

    if record.get("schema_version") != 1:
        faults.append(f"{MUTANTS_RECORD}: schema_version must be 1")
    results = record.get("results")
    if not isinstance(results, dict) or not results:
        return faults + [f"{MUTANTS_RECORD}: no results"]
    unseeded = record.get("unseeded", [])
    wanted_status = "partial" if unseeded else "seeded"
    if record.get("status") != wanted_status:
        faults.append(
            f"{MUTANTS_RECORD}: status is {record.get('status')!r}, and with "
            f"{len(unseeded)} unseeded crates it must be {wanted_status!r}"
        )

    for crate, result in sorted(results.items()):
        where = f"{MUTANTS_RECORD}: {crate}"
        entry = effective(record, result)
        faults += check_run(where, entry, ("rustc", "bazel"))
        counts = [entry.get(key) for key in ("caught", "survived", "unviable", "total")]
        if not all(is_count(value) for value in counts):
            faults.append(f"{where}: total, caught, survived and unviable must be counts")
        elif counts[0] + counts[1] + counts[2] != counts[3]:
            faults.append(
                f"{where}: {counts[0]} caught plus {counts[1]} survived plus "
                f"{counts[2]} unviable is not the total {counts[3]}"
            )
        for key in ("artifact_sha256", "contents_manifest_sha256"):
            if not SHA256.match(str(entry.get(key, ""))):
                faults.append(f"{where}: {key} must be a 64-character hex SHA-256")
        if "artifact_id" not in entry and "artifact_url" not in entry:
            faults.append(f"{where}: no artifact_id or artifact_url, so the logs cannot be found")

        declared = MUTANTS.declared_shards(root / "crates" / crate / "BUILD.bazel")
        if declared is None:
            faults.append(f"{where}: crates/{crate}/BUILD.bazel does not exist")
        elif entry.get("shards") != declared:
            faults.append(
                f"{where}: the run used {entry.get('shards')} shards and "
                f"crates/{crate}/BUILD.bazel declares {declared}. Change both together."
            )

        if crate not in baseline:
            faults.append(f"{where}: {MUTANTS_BASELINE} has no line for {crate}")
        elif baseline[crate] is None:
            faults.append(f"{where}: {MUTANTS_BASELINE} says unseeded, and the record has a result")
        elif baseline[crate] != entry.get("survived"):
            faults.append(
                f"{where}: {MUTANTS_BASELINE} says {baseline[crate]} survivors, "
                f"and the record says {entry.get('survived')}"
            )

    for crate in sorted(unseeded):
        if crate in results:
            faults.append(f"{MUTANTS_RECORD}: {crate} is in unseeded and has a result")
        if baseline.get(crate, 0) is not None:
            faults.append(f"{MUTANTS_RECORD}: {crate} is unseeded, and {MUTANTS_BASELINE} is not")
    for crate, count in sorted(baseline.items()):
        if count is None and crate not in unseeded:
            faults.append(f"{MUTANTS_BASELINE}: {crate} is unseeded, and the record does not list it")
        if count is not None and crate not in results:
            faults.append(f"{MUTANTS_BASELINE}: {crate} {count} has no recorded run behind it")
    for crate in sorted(mutants_crates(root) - set(baseline)):
        faults.append(f"{MUTANTS_BASELINE}: {crate} has a mutants target and no line")

    faults += check_deliberate_survivor(root, record, record_path, baseline_path)
    return faults


def check_deliberate_survivor(root, record, record_path, baseline_path):
    """Runs the proof again and compares it with the block the record keeps."""
    block = record.get("deliberate_survivor")
    if not isinstance(block, dict):
        return [f"{MUTANTS_RECORD}: no deliberate_survivor block"]
    if "synthetic" not in str(block.get("input", "")):
        return [f"{MUTANTS_RECORD}: deliberate_survivor.input must say the logs are synthetic"]
    try:
        # The proof runs the gate three times, and two of those runs fail on
        # purpose. Their annotations are not faults in the record.
        with contextlib.redirect_stdout(io.StringIO()):
            proof = MUTANTS.prove_gate(block.get("crate"), record_path, baseline_path, root)
    except (MUTANTS.UsageError, MUTANTS.ProofFailed, KeyError, TypeError) as error:
        return [f"{MUTANTS_RECORD}: deliberate_survivor does not run again: {error}"]
    kept = {key: value for key, value in block.items() if key != "python"}
    if kept != proof:
        changed = sorted(key for key in set(kept) | set(proof) if kept.get(key) != proof.get(key))
        return [
            f"{MUTANTS_RECORD}: deliberate_survivor differs from a new run of "
            f"`{proof['command']}` in {', '.join(changed)}"
        ]
    return []


# --- the benchmark record ----------------------------------------------------


def check_bench(root):
    root = pathlib.Path(root)
    record = load_json(root / BENCH_RECORD)
    baseline_path = root / BENCH_BASELINE
    baseline = BENCH.read_baseline(baseline_path)
    seeded = [name for name, budget in baseline.items() if budget is not None]
    where = BENCH_RECORD
    faults = []

    if record.get("schema_version") != 1:
        faults.append(f"{where}: schema_version must be 1")
    wanted_status = "seeded" if len(seeded) == len(baseline) else "partial"
    if record.get("status") != wanted_status:
        faults.append(f"{where}: status must be {wanted_status!r}")
    faults += check_run(where, record, ("rustc",))
    artifact = record.get("artifact") or {}
    for key in ("sha256", "baseline_sha256"):
        if not SHA256.match(str(artifact.get(key, ""))):
            faults.append(f"{where}: artifact.{key} must be a 64-character hex SHA-256")
    if SHA256.match(str(artifact.get("baseline_sha256", ""))):
        actual = sha256_file(baseline_path)
        if artifact["baseline_sha256"] != actual:
            faults.append(
                f"{where}: {BENCH_BASELINE} has SHA-256 {actual}, and the record "
                f"checksummed {artifact['baseline_sha256']}. Record the run that "
                f"wrote the new numbers."
            )
    if record.get("benchmark_count") != len(seeded):
        faults.append(
            f"{where}: benchmark_count is {record.get('benchmark_count')}, and "
            f"{BENCH_BASELINE} has {len(seeded)} seeded budgets"
        )
    if record.get("noise_ceiling") != BENCH.DEFAULT_NOISE_CEILING:
        faults.append(
            f"{where}: noise_ceiling is {record.get('noise_ceiling')}, and "
            f"tools/bench-ratchet.py applies {BENCH.DEFAULT_NOISE_CEILING}"
        )
    regression = record.get("deliberate_regression")
    if not isinstance(regression, dict):
        faults.append(f"{where}: no deliberate_regression block")
    else:
        if regression.get("result") != "rejected":
            faults.append(f"{where}: deliberate_regression.result must be 'rejected'")
        if regression.get("benchmark") not in baseline:
            faults.append(f"{where}: deliberate_regression names a benchmark with no budget")
    return faults


def check(root=ROOT):
    """Every fault in both records. Returns the exit code."""
    faults = check_mutants(root) + check_bench(root)
    for fault in faults:
        MUTANTS.annotate("error", fault)
    if not faults:
        print(f"{MUTANTS_RECORD} and {BENCH_RECORD} match their baselines")
    return EXIT_INVALID if faults else EXIT_VALID


# --- capturing a manual sweep ------------------------------------------------


def read_metadata(path):
    """The `key=value` lines that tools/mutants-sweep.sh writes."""
    fields = {}
    for line in pathlib.Path(path).read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep:
            fields[key.strip()] = value.strip()
    missing = sorted(
        {"commit", "started_at", "duration_seconds", "command", "host",
         "cpu_count", "memory_kib", "rustc", "bazel"} - set(fields)
    )
    if missing:
        raise UsageError(f"{path}: no {', '.join(missing)}")
    return fields


def capture(crate, logs_root, log_dir, artifact, artifact_url, runner_label,
            root=ROOT, run_url=None):
    """The result entry for one manual sweep, or None when it is incomplete."""
    root = pathlib.Path(root)
    log_dir = pathlib.Path(log_dir)
    metadata = read_metadata(log_dir / f"{crate}.metadata.txt")
    manifest = log_dir / f"{crate}.SHA256SUMS"
    if not manifest.is_file():
        raise UsageError(f"{manifest} does not exist. tools/mutants-sweep.sh writes it.")
    if not pathlib.Path(artifact).is_file():
        raise UsageError(f"{artifact} does not exist. Archive the logs first.")

    expected = MUTANTS.declared_shards(root / "crates" / crate / "BUILD.bazel")
    totals, faults = MUTANTS.read_sweep(logs_root, crate, expected)
    for fault in faults:
        MUTANTS.annotate("error", fault)
    if faults:
        return None

    bazel = BAZEL_LABEL.search(metadata["bazel"])
    entry = {
        "started_at": metadata["started_at"],
        "command": metadata["command"],
        "duration_seconds": int(metadata["duration_seconds"]),
        "shards": totals["shards"],
        "total": totals["total"],
        "caught": totals["caught"],
        "survived": totals["missed"],
        "unviable": totals["unviable"],
        "artifact_url": artifact_url,
        "artifact_sha256": sha256_file(artifact),
        "contents_manifest_sha256": sha256_file(manifest),
    }
    entry["provenance"] = {
        "commit": metadata["commit"],
        "runner": {
            "label": runner_label,
            "os": metadata["host"],
            "cpu_count": int(metadata["cpu_count"]),
            "memory_kib": int(metadata["memory_kib"]),
        },
        "toolchain": {
            "rustc": metadata["rustc"].split(";")[0].removeprefix("rustc ").strip(),
            "bazel": bazel.group(1) if bazel else metadata["bazel"].split(";")[0],
        },
    }
    if run_url:
        entry["provenance"]["run_url"] = run_url
    return entry


def write_capture(root, crate, entry):
    """Puts the entry in the record and its count in the baseline."""
    root = pathlib.Path(root)
    record_path = root / MUTANTS_RECORD
    record = load_json(record_path)
    record["results"][crate] = entry
    record["results"] = dict(sorted(record["results"].items()))
    record["unseeded"] = [name for name in record.get("unseeded", []) if name != crate]
    record["status"] = "partial" if record["unseeded"] else "seeded"
    record_path.write_text(json.dumps(record, indent=2) + "\n")

    baseline_path = root / MUTANTS_BASELINE
    lines = baseline_path.read_text().splitlines()
    pattern = re.compile(rf"^({re.escape(crate)}\s+)\S+(.*)$")
    replaced = [pattern.sub(rf"\g<1>{entry['survived']}\g<2>", line) for line in lines]
    if replaced == lines and not any(pattern.match(line) for line in lines):
        replaced.append(f"{crate} {entry['survived']}")
    baseline_path.write_text("\n".join(replaced) + "\n")


# --- self-test ---------------------------------------------------------------
#
# The records change rarely and by hand, so each check is proven against a
# synthetic repository that holds one fault at a time.


def good_mutants_record():
    return {
        "schema_version": 1,
        "status": "partial",
        "commit": "a" * 40,
        "runner": {"label": "host", "os": "Linux", "cpu_count": 4, "memory_kib": 1024},
        "toolchain": {"rustc": "1.97.1", "bazel": "9.2.0"},
        "results": {
            "demo": {
                "started_at": "2026-09-20T22:20:38Z",
                "command": "aspect mutants demo 0 1",
                "duration_seconds": 22,
                "shards": 8,
                "total": 23,
                "caught": 21,
                "survived": 2,
                "unviable": 0,
                "artifact_url": "gs://bucket/demo.tar.zst",
                "artifact_sha256": "1" * 64,
                "contents_manifest_sha256": "2" * 64,
                "provenance": {"commit": "b" * 40},
            },
        },
        "unseeded": ["other"],
    }


def good_bench_record(baseline_sha):
    return {
        "schema_version": 1,
        "status": "seeded",
        "commit": "c" * 40,
        "runner": {"label": "remote", "os": "Linux", "cpu_count": 16, "memory_kib": 2048},
        "toolchain": {"rustc": "1.97.1", "llvm": "22.1.6"},
        "command": "tools/bench.sh && tools/bench-ratchet.py --record",
        "started_at": "2026-09-22T05:30:34Z",
        "duration_seconds": 1193,
        "benchmark_count": 2,
        "noise_ceiling": BENCH.DEFAULT_NOISE_CEILING,
        "artifact": {"sha256": "3" * 64, "baseline_sha256": baseline_sha},
        "deliberate_regression": {"benchmark": "group/a", "result": "rejected"},
    }


def make_repo(root, mutate_mutants=None, mutate_bench=None, files=None):
    """Writes a repository that passes, then applies one fault to it."""
    root = pathlib.Path(root)
    contents = {
        "crates/demo/BUILD.bazel": 'crate_tests(lib = "demo")\n',
        "crates/other/BUILD.bazel": 'crate_tests(\n    lib = "other",\n    mutants_shards = 16,\n)\n',
        "crates/integration/BUILD.bazel": 'crate_tests(\n    lib = "integration",\n    mutants = False,\n)\n',
        MUTANTS_BASELINE: "# comment\ndemo   2\nother  unseeded\n",
        BENCH_BASELINE: "# comment\ngroup/a  100\ngroup/b  200\n",
    }
    for name, text in contents.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)

    record = good_mutants_record()
    (root / MUTANTS_RECORD).parent.mkdir(parents=True, exist_ok=True)
    (root / MUTANTS_RECORD).write_text(json.dumps(record))
    with contextlib.redirect_stdout(io.StringIO()):
        block = MUTANTS.prove_gate("demo", root / MUTANTS_RECORD, root / MUTANTS_BASELINE, root)
    record["deliberate_survivor"] = dict(block, python="3")
    if mutate_mutants:
        mutate_mutants(record)
    (root / MUTANTS_RECORD).write_text(json.dumps(record))
    bench = good_bench_record(sha256_file(root / BENCH_BASELINE))

    # The fault goes in after the good records are written, so each case
    # holds one fault and not a record written to agree with it.
    for name, text in (files or {}).items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
    if mutate_bench:
        mutate_bench(bench)
    (root / BENCH_RECORD).write_text(json.dumps(bench))


def demo(record):
    return record["results"]["demo"]


def setter(path, value):
    """A mutation that sets, or with `...` deletes, one nested field."""
    def apply(record):
        target = record
        for key in path[:-1]:
            target = target[key]
        if value is ...:
            del target[path[-1]]
        else:
            target[path[-1]] = value
    return apply


# Each case: name, mutation record fault, benchmark record fault, extra files,
# and a fragment the fault message must hold. None means the repository passes.
CHECK_CASES = [
    ("a valid pair of records passes", None, None, None, None),
    ("a result inherits the top-level runner", None, None, None, None),
    ("the result's own commit is checked", setter(["results", "demo", "provenance", "commit"], "xyz"),
     None, None, "demo: commit must be"),
    ("no commit anywhere", lambda r: (r.pop("commit"), demo(r).pop("provenance")),
     None, None, "demo: commit must be"),
    ("no toolchain", setter(["toolchain", "bazel"], ...), None, None, "toolchain.bazel is missing"),
    ("no host memory", setter(["runner", "memory_kib"], ...), None, None, "runner.memory_kib"),
    ("no command", setter(["results", "demo", "command"], ""), None, None, "command is missing"),
    ("no duration", setter(["results", "demo", "duration_seconds"], ...), None, None, "duration_seconds"),
    ("a bad artifact checksum", setter(["results", "demo", "artifact_sha256"], "abc"),
     None, None, "artifact_sha256 must be"),
    ("no manifest checksum", setter(["results", "demo", "contents_manifest_sha256"], ...),
     None, None, "contents_manifest_sha256 must be"),
    ("no artifact location", setter(["results", "demo", "artifact_url"], ...),
     None, None, "no artifact_id or artifact_url"),
    ("counts that do not add up", setter(["results", "demo", "caught"], 20),
     None, None, "is not the total 23"),
    ("a baseline that differs from the record", None, None,
     {MUTANTS_BASELINE: "demo 1\nother unseeded\n"}, "says 1 survivors, and the record says 2"),
    ("a baseline number with no run", None, None,
     {MUTANTS_BASELINE: "demo 2\nother 5\n"}, "other 5 has no recorded run"),
    ("an unseeded crate the record does not list", setter(["unseeded"], []),
     None, None, "other is unseeded, and the record does not list it"),
    ("a status that disagrees with unseeded", setter(["status"], "seeded"),
     None, None, "it must be 'partial'"),
    ("a shard count that differs from BUILD", None, None,
     {"crates/demo/BUILD.bazel": 'crate_tests(\n    lib = "demo",\n    mutants_shards = 24,\n)\n'},
     "declares 24"),
    ("a mutants target with no baseline line", None, None,
     {"crates/new/BUILD.bazel": 'crate_tests(lib = "new")\n'}, "new has a mutants target and no line"),
    ("no deliberate survivor", setter(["deliberate_survivor"], ...), None, None, "no deliberate_survivor"),
    ("a deliberate survivor that claims a real sweep", setter(["deliberate_survivor", "input"], "a sweep"),
     None, None, "must say the logs are synthetic"),
    ("a deliberate survivor that does not match a new run",
     setter(["deliberate_survivor", "exit_code"], 0), None, None, "differs from a new run"),
    ("an edited benchmark baseline", None, None,
     {BENCH_BASELINE: "# comment\ngroup/a  100\ngroup/b  201\n"}, "Record the run"),
    ("a benchmark count that differs", None, setter(["benchmark_count"], 3), None, "benchmark_count is 3"),
    ("no benchmark host shape", None, setter(["runner", "cpu_count"], 0), None, "runner.cpu_count"),
    ("no benchmark start time", None, setter(["started_at"], ...), None, "started_at"),
    ("no benchmark artifact checksum", None, setter(["artifact", "sha256"], ...), None, "artifact.sha256"),
    ("no deliberate regression", None, setter(["deliberate_regression"], ...), None, "no deliberate_regression"),
]


def self_test_capture():
    """A manual sweep, captured and written, leaves records that pass --check."""
    with tempfile.TemporaryDirectory() as scratch:
        root = pathlib.Path(scratch)
        make_repo(root)
        logs = root / "bazel-testlogs"
        MUTANTS.synthetic_sweep(logs, "other", 16, 100, 7, 3)
        results = root / "sweep-results"
        results.mkdir()
        (results / "other.metadata.txt").write_text(
            f"commit={'d' * 40}\nstarted_at=2026-09-28T01:02:03Z\n"
            "duration_seconds=36000\ncommand=bazel test //crates/other:other_mutants\n"
            "host=Linux sweeper 6.12 x86_64\ncpu_count=32\nmemory_kib=131072000\n"
            "rustc=rustc 1.97.1 (8bab26f4f 2026-07-14);binary: rustc;\n"
            "bazel=Bazelisk version: 1.28.1;Build label: 9.2.0;\n"
        )
        (results / "other.SHA256SUMS").write_text("0" * 64 + "  other.log\n")
        archive = results / "other.tar.zst"
        archive.write_bytes(b"logs")
        entry = capture("other", logs, results, archive, "gs://bucket/other.tar.zst",
                        "sweeper", root)
        expected = {
            "started_at": "2026-09-28T01:02:03Z",
            "command": "bazel test //crates/other:other_mutants",
            "duration_seconds": 36000,
            "shards": 16,
            "total": 100,
            "caught": 90,
            "survived": 7,
            "unviable": 3,
            "artifact_url": "gs://bucket/other.tar.zst",
            "artifact_sha256": hashlib.sha256(b"logs").hexdigest(),
            "contents_manifest_sha256": sha256_file(results / "other.SHA256SUMS"),
            "provenance": {
                "commit": "d" * 40,
                "runner": {
                    "label": "sweeper", "os": "Linux sweeper 6.12 x86_64",
                    "cpu_count": 32, "memory_kib": 131072000,
                },
                "toolchain": {"rustc": "1.97.1 (8bab26f4f 2026-07-14)", "bazel": "9.2.0"},
            },
        }
        if entry != expected:
            return f"capture wrote {json.dumps(entry, indent=2)}"
        write_capture(root, "other", entry)
        faults = check_mutants(root)
        if faults:
            return f"--check after --write: {faults}"
        record = load_json(root / MUTANTS_RECORD)
        if record["unseeded"] != [] or record["status"] != "seeded":
            return "the crate is still listed as unseeded"
        if "other  7" not in (root / MUTANTS_BASELINE).read_text():
            return "the baseline line was not written in place"

        # A killed shard is not recorded.
        (logs / "crates/other/other_mutants/shard_3_of_16/test.log").write_text("Test timed out\n")
        if capture("other", logs, results, archive, "gs://b/o", "sweeper", root) is not None:
            return "a sweep with a killed shard was captured"
    return None


def self_test():
    failures = 0
    for name, mutants_fault, bench_fault, files, fragment in CHECK_CASES:
        with tempfile.TemporaryDirectory() as scratch:
            make_repo(scratch, mutants_fault, bench_fault, files)
            faults = check_mutants(scratch) + check_bench(scratch)
        if fragment is None:
            ok = faults == []
        else:
            ok = any(fragment in fault for fault in faults)
        failures += 0 if ok else 1
        detail = "" if ok else f" -- got {faults}"
        print(f"  {'ok' if ok else 'FAILED':<8} {name}{detail}", flush=True)

    problem = self_test_capture()
    failures += 1 if problem else 0
    detail = f" -- {problem}" if problem else ""
    print(f"  {'FAILED' if problem else 'ok':<8} a captured sweep passes --check{detail}", flush=True)

    print(f"{len(CHECK_CASES) + 1} cases, {failures} failed", flush=True)
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="check both records against their baselines")
    parser.add_argument("--self-test", action="store_true", help="run the checks against a synthetic repository")
    parser.add_argument("--capture", metavar="CRATE", help="record a manual sweep of CRATE")
    parser.add_argument("--logs", default="bazel-testlogs", help="the bazel-testlogs directory of the sweep")
    parser.add_argument(
        "--log-dir", default=str(pathlib.Path.home() / "krabka-work" / "sweep-results"),
        help="where tools/mutants-sweep.sh wrote <crate>.metadata.txt and <crate>.SHA256SUMS",
    )
    parser.add_argument("--artifact", help="the archive of the sweep's logs")
    parser.add_argument("--artifact-url", help="where the archive is stored, such as a gs:// URL")
    parser.add_argument("--runner-label", help="the name of the host that ran the sweep")
    parser.add_argument("--run-url", help="the job that ran the sweep, if one did")
    parser.add_argument("--write", action="store_true", help="write the captured sweep into the record and baseline")
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    try:
        if args.capture:
            if not (args.artifact and args.artifact_url and args.runner_label):
                parser.error("--capture needs --artifact, --artifact-url and --runner-label")
            entry = capture(args.capture, args.logs, args.log_dir, args.artifact,
                            args.artifact_url, args.runner_label, ROOT, args.run_url)
            if entry is None:
                return EXIT_INCOMPLETE
            print(json.dumps({args.capture: entry}, indent=2))
            if args.write:
                write_capture(ROOT, args.capture, entry)
                return check()
            return EXIT_VALID
        if args.check:
            return check()
    except (UsageError, MUTANTS.UsageError, BENCH.UsageError) as error:
        print(f"mutants-record.py: {error}", file=sys.stderr)
        return EXIT_USAGE
    parser.error("one of --check, --capture or --self-test is needed")
    return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
