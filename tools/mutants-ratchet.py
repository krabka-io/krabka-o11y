#!/usr/bin/env python3
"""Reads a crate's mutation sweep and decides whether the sweep may pass.

Run it after `bazel test //crates/<crate>:<crate>_mutants`, from the workspace
root:

    tools/mutants-ratchet.py <crate>

It reads the shard logs the run left in `bazel-testlogs` and applies two
gates.

The first gate is structural, and it is the one that matters most. A shard
that overruns its bound is killed, and a killed shard writes no summary line
rather than failing loudly. The sweep then reports a crate as "0 mutants, 0
missed", which is the same text a crate with nothing left to kill produces.
`crates/*/BUILD.bazel` and `README.md` both say that the totals line
`caught + missed + unviable == total` is the only place that shows, but until
this script existed nothing read it. Every check below turns one silent no-op
into a named failure:

  * every shard the run split the crate into left a log
  * the run split into the shard count the crate's BUILD file declares
  * every shard log holds exactly one totals line
  * every totals line adds up
  * every totals line agrees with the `MISSED` lines under it
  * the crate enumerated at least one mutant

The second gate is the ratchet. `tools/mutants-baseline.txt` holds one
survivor count per crate. A crate whose count is above its baseline fails. A
crate whose count is below its baseline passes and asks for the baseline to be
lowered, because a baseline nobody lowers stops measuring anything.

A crate whose baseline reads `unseeded` skips the second gate only. The
structural gate still applies, so a crate with no number yet is still unable
to report a killed shard as a clean sweep. Use `--record` to print the line to
check in once a full sweep has run.

`--self-test` runs the checks against synthetic shard logs and needs no sweep.

The exit code names the verdict, so a scheduled rerun or a person reading a
failed job can tell a sweep that did not finish from a score that went down:

  * 0: every crate passed
  * 1: a complete, valid sweep has more survivors than its baseline allows,
    or a crate has no baseline line. This is a real regression.
  * 2: the command line is wrong
  * 3: a sweep is incomplete or structurally invalid. A shard was killed, was
    silent, refused, or its numbers disagree. The survivor count is unknown,
    so no ratchet verdict is given. Rerun the missing shards; do not change the
    baseline.

When crates fall into both classes, 3 wins: an unknown count cannot be called
a pass, and a regression in one crate is still named in the verdict.

`--json <path>` writes the same verdict in machine-readable form: `status` is
`pass`, `regression` or `incomplete`, and each crate carries its counts and
the reasons for its class. `-` writes it to standard output.

`--prove-gate <crate>` shows that the gate rejects one new survivor. It
builds a sweep in the shape that `qualification/milestone-19-mutation-baselines.json`
records for the crate, with the recorded shard count and survivor count, adds
one `MISSED` line, and applies the checked-in baseline. It needs no sweep and
it prints the evidence block that the record keeps as `deliberate_survivor`.
The shard logs are synthetic, and the block says so.
"""

import argparse
import json
import os
import pathlib
import re
import sys
import tempfile

# The line //mutants/private/cargo_mutants_runner.rs prints per shard.
# Leading whitespace is tolerated so the same parser reads a console
# transcript, which Bazel indents, as well as the raw log file, which it does
# not.
TOTALS = re.compile(
    r"^[ \t]*(\d+) mutants: (\d+) caught, (\d+) missed, (\d+) unviable[ \t]*$",
    re.MULTILINE,
)
MISSED = re.compile(r"^[ \t]*MISSED ", re.MULTILINE)
# Bazel appends this to the log of a test it killed on its deadline.
TIMED_OUT = "Test timed out"
# The runner refuses to sweep when an unmutated suite fails inside its sandbox.
# It has two spellings, one for the unit tests and one for an integration
# stage. A sweep once read as clean while seventeen of thirty-two shards had
# refused on the spelling nobody was reading for.
REFUSED = re.compile(r"do(?:es)? not pass; fix (?:it|them) first")
SHARD_DIR = re.compile(r"^shard_(\d+)_of_(\d+)$")
# `mutants_shards = <n>` in a crate's `crate_tests` call.
BUILD_SHARDS = re.compile(r"^\s*mutants_shards\s*=\s*(\d+)\s*,", re.MULTILINE)
# The default in //bazel/defs.bzl, for a BUILD file that does not set one.
DEFAULT_SHARDS = 8

UNSEEDED = "unseeded"

ROOT = pathlib.Path(__file__).resolve().parent.parent
RECORD = ROOT / "qualification" / "milestone-19-mutation-baselines.json"

EXIT_PASS = 0
EXIT_REGRESSION = 1
EXIT_USAGE = 2
EXIT_INCOMPLETE = 3

PASS = "pass"
REGRESSION = "regression"
INCOMPLETE = "incomplete"


class UsageError(Exception):
    """A fault in the input the caller named: a baseline, a record, a crate.

    It exits 2, not 1, because 1 means a sweep regressed and none was read.
    """


class ProofFailed(Exception):
    """`--prove-gate` saw the gate pass a sweep it has to reject."""


def annotate(level, message):
    """Prints a message, as a GitHub annotation when the run is on Actions."""
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::{level}::{message}", flush=True)
    else:
        print(f"{level}: {message}", flush=True)


def keep_stdout_for_json(destination):
    """Sends every other line to standard error when the JSON goes to stdout.

    `--json -` is for a pipe into another tool, and the annotations would
    break that JSON.
    """
    if destination == "-":
        sys.stdout = sys.stderr


def shard_logs(logs_root, crate):
    """Shard name to log text, for one crate's mutants target.

    A sharded target writes one `shard_<i>_of_<n>/test.log` per shard, and an
    unsharded one writes a single `test.log` in their place. Every `test.log`
    under the target is read, so neither layout has to be named here, and a
    layout this script does not know still gets the per-shard checks.
    """
    target = pathlib.Path(logs_root) / "crates" / crate / f"{crate}_mutants"
    if not target.is_dir():
        return {}
    found = {}
    for log in sorted(target.rglob("test.log")):
        where = log.parent.relative_to(target)
        found[str(where) if where.parts else "unsharded"] = log.read_text(
            errors="replace"
        )
    return found


def read_shard(name, text):
    """One shard's numbers, or the reason the shard reported none."""
    totals = TOTALS.findall(text)
    if len(totals) != 1:
        if TIMED_OUT in text:
            why = "the shard overran its deadline and Bazel killed it"
        elif REFUSED.search(text):
            why = "the shard refused to sweep: an unmutated suite fails in its sandbox"
        elif not totals:
            why = "the shard wrote no totals line"
        else:
            why = f"the shard wrote {len(totals)} totals lines, and one is expected"
        return None, f"{name}: {why}"
    total, caught, missed, unviable = (int(field) for field in totals[0])
    if caught + missed + unviable != total:
        return None, (
            f"{name}: the totals line does not add up. {caught} caught plus "
            f"{missed} missed plus {unviable} unviable is "
            f"{caught + missed + unviable}, and the line says {total}"
        )
    labelled = len(MISSED.findall(text))
    if labelled != missed:
        return None, (
            f"{name}: the totals line says {missed} missed and the shard lists "
            f"{labelled} MISSED entries"
        )
    return {
        "total": total, "caught": caught, "missed": missed, "unviable": unviable,
    }, None


def read_sweep(logs_root, crate, expected_shards=None):
    """A crate's totals, or every reason the sweep cannot be trusted.

    `expected_shards` is the shard count the crate's BUILD file declares. A
    run that split into a different count is a run of some other
    configuration, and its number is not comparable with the baseline.
    """
    logs = shard_logs(logs_root, crate)
    if not logs:
        return None, [
            f"{crate}: no shard log under {logs_root}/crates/{crate}/"
            f"{crate}_mutants. The sweep did not run, or it failed before any "
            f"shard started."
        ]

    faults = []
    shards = {}
    for name, text in sorted(logs.items()):
        numbers, fault = read_shard(name, text)
        if fault:
            faults.append(fault)
        else:
            shards[name] = numbers

    # A killed shard leaves no directory at all when Bazel never starts it, so
    # counting the logs that exist is not enough. Each directory names the
    # shard count the run used, so the set is checkable against itself.
    widths = {
        int(SHARD_DIR.match(name).group(2))
        for name in logs
        if SHARD_DIR.match(name)
    }
    if len(widths) > 1:
        faults.append(
            f"{crate}: the shard logs disagree on the shard count: "
            f"{sorted(widths)}. Two runs wrote into the same directory."
        )
    elif widths:
        width = widths.pop()
        if len(logs) != width:
            missing = width - len(logs)
            faults.append(
                f"{crate}: the run split into {width} shards and "
                f"{missing} left no log at all"
            )
        if expected_shards is not None and width != expected_shards:
            faults.append(
                f"{crate}: the run split into {width} shards and "
                f"crates/{crate}/BUILD.bazel declares {expected_shards}. The "
                f"logs are from another configuration."
            )

    if faults:
        return None, faults

    totals = {
        key: sum(shard[key] for shard in shards.values())
        for key in ("total", "caught", "missed", "unviable")
    }
    totals["shards"] = len(shards)
    if totals["total"] == 0:
        return None, [
            f"{crate}: the sweep enumerated 0 mutants across {len(shards)} "
            f"shards. That is a broken run, not a clean one."
        ]
    return totals, []


def declared_shards(build_file):
    """The `mutants_shards` a BUILD file declares, or None with no file."""
    path = pathlib.Path(build_file)
    if not path.is_file():
        return None
    found = BUILD_SHARDS.findall(path.read_text())
    return int(found[0]) if len(found) == 1 else DEFAULT_SHARDS


def read_baseline(path):
    """Crate name to survivor count, where `unseeded` reads as None."""
    baseline = {}
    for number, line in enumerate(pathlib.Path(path).read_text().splitlines(), 1):
        stripped = line.split("#", 1)[0].strip()
        if not stripped:
            continue
        fields = stripped.split()
        if len(fields) != 2:
            raise UsageError(f"{path}:{number}: expected `<crate> <count>`: {line}")
        crate, count = fields
        if count == UNSEEDED:
            baseline[crate] = None
        elif count.isdigit():
            baseline[crate] = int(count)
        else:
            raise UsageError(
                f"{path}:{number}: the count must be a number or `{UNSEEDED}`: {line}"
            )
    return baseline


def survivor_count(number):
    """`3 survivors`, and `1 survivor` for the one case that reads wrong."""
    return f"{number} survivor" + ("" if number == 1 else "s")


def gate(crate, survivors, baseline):
    """Applies the ratchet. Returns the reasons the crate fails, or none."""
    if crate not in baseline:
        reason = (
            f"{crate} has no line in the baseline. Add `{crate} {survivors}` to "
            f"it, or `{crate} {UNSEEDED}` to leave the ratchet dormant."
        )
        annotate("error", reason)
        return [reason]
    allowed = baseline[crate]
    if allowed is None:
        annotate(
            "notice",
            f"{crate}: {survivor_count(survivors)}. The baseline reads "
            f"`{UNSEEDED}`, so the ratchet is dormant for this crate. Check in "
            f"`{crate} {survivors}` to arm it.",
        )
        return []
    if survivors > allowed:
        reason = (
            f"{crate}: {survivor_count(survivors)}, and the baseline allows "
            f"{allowed}. Kill {survivor_count(survivors - allowed)}, or raise "
            f"the baseline and say in the commit message why the score went down."
        )
        annotate("error", reason)
        return [reason]
    if survivors < allowed:
        annotate(
            "notice",
            f"{crate}: {survivor_count(survivors)}, and the baseline allows "
            f"{allowed}. Lower the baseline to {survivors} to hold the ground.",
        )
        return []
    print(f"{crate}: {survivor_count(survivors)}, at its baseline.", flush=True)
    return []


def baseline_label(baseline, crate):
    """What the baseline says for a crate, as the JSON verdict reports it."""
    if baseline is None:
        return None
    if crate not in baseline:
        return "missing"
    return UNSEEDED if baseline[crate] is None else baseline[crate]


def judge(crate, logs_root, baseline, expected_shards):
    """One crate's verdict: its class, its counts and the reasons."""
    totals, faults = read_sweep(logs_root, crate, expected_shards)
    for fault in faults:
        annotate("error", fault)
    verdict = {
        "status": INCOMPLETE if faults else PASS,
        "baseline": baseline_label(baseline, crate),
        "expected_shards": expected_shards,
        "counts": totals,
        "reasons": faults,
    }
    if faults or baseline is None:
        return verdict
    reasons = gate(crate, totals["missed"], baseline)
    if reasons:
        verdict["status"] = REGRESSION
        verdict["reasons"] = reasons
    return verdict


def overall(crates):
    """The run's class, from its crates' classes. Incomplete outranks the rest."""
    statuses = {verdict["status"] for verdict in crates.values()}
    for status in (INCOMPLETE, REGRESSION):
        if status in statuses:
            return status
    return PASS


EXIT_CODES = {PASS: EXIT_PASS, REGRESSION: EXIT_REGRESSION, INCOMPLETE: EXIT_INCOMPLETE}


def write_json(verdict, destination):
    """Writes a verdict to a path, or to standard output for `-`."""
    text = json.dumps(verdict, indent=2, sort_keys=True) + "\n"
    if destination == "-":
        sys.__stdout__.write(text)
        sys.__stdout__.flush()
    else:
        pathlib.Path(destination).write_text(text)


def run(crates, logs_root, baseline_path, record, json_out=None, build_root=ROOT):
    """Judges each crate's sweep. Returns the exit code for the whole run."""
    if isinstance(crates, str):
        crates = [crates]
    baseline = None if record else read_baseline(baseline_path)
    verdicts = {}
    for crate in crates:
        expected = declared_shards(pathlib.Path(build_root) / "crates" / crate / "BUILD.bazel")
        verdicts[crate] = judge(crate, logs_root, baseline, expected)
    status = overall(verdicts)
    if record and status == PASS:
        for crate, verdict in verdicts.items():
            print(f"{crate} {verdict['counts']['missed']}")
    if json_out:
        write_json({"schema_version": 1, "status": status, "crates": verdicts}, json_out)
    return EXIT_CODES[status]


# --- proving the gate --------------------------------------------------------
#
# A gate that has never been seen to fail is not known to work. The sweep
# itself cannot be asked for one more survivor, so the proof builds the logs a
# real sweep of the crate would write -- the recorded shard count, the recorded
# totals, the runner's own line format -- adds one survivor, and shows the
# checked-in baseline rejects it.


def synthetic_sweep(root, crate, shards, total, survived, unviable, extra_missed=0):
    """Writes a crate's shard logs in the runner's format, spread over shards.

    The runner deals mutants to shards round-robin, and so does this. Each
    `MISSED` label has the runner's `<file>:<line>:<column>: replace <function>
    with <replacement>` shape, and names a file that does not exist, so no one
    mistakes a synthetic survivor for a real one.
    """
    survived += extra_missed
    caught = total + extra_missed - survived - unviable
    logs = {}
    for index in range(shards):
        def share(count, index=index):
            return count // shards + (1 if index < count % shards else 0)
        missed = share(survived)
        # `missed` is taken out of the survivors dealt to earlier shards, so
        # the labels stay unique across the sweep.
        first = sum(share(survived, earlier) for earlier in range(index))
        labels = "".join(
            f"MISSED crates/{crate}/src/synthetic.rs:{first + n + 1}:5: "
            f"replace synthetic_{first + n} with Default::default()\n"
            for n in range(missed)
        )
        shard_caught, shard_unviable = share(caught), share(unviable)
        shard_total = shard_caught + missed + shard_unviable
        logs[f"shard_{index + 1}_of_{shards}"] = (
            f"{shard_total} mutants: {shard_caught} caught, {missed} missed, "
            f"{shard_unviable} unviable\n{labels}"
        )
    write_shards(root, crate, logs)


def prove_gate(crate, record_path, baseline_path, build_root=ROOT):
    """Shows the baseline rejects one survivor over the recorded sweep.

    Returns the evidence block. Raises UsageError when the crate has no
    record or no seeded baseline, and ProofFailed when the gate did not
    behave.
    """
    record = json.loads(pathlib.Path(record_path).read_text())
    result = record.get("results", {}).get(crate)
    if result is None:
        raise UsageError(f"{crate} has no recorded sweep in {record_path}")
    shards = result.get("shards")
    total, survived, unviable = result["total"], result["survived"], result["unviable"]
    baseline = read_baseline(baseline_path).get(crate)
    if baseline is None:
        raise UsageError(f"{crate} has no seeded baseline in {baseline_path}")

    # Three runs. The recorded sweep against the checked-in baseline passes.
    # One more survivor against the same baseline fails. The recorded sweep
    # against a baseline lowered by one also fails, which is the same gate
    # seen from the other side.
    exits = {}
    with tempfile.TemporaryDirectory() as scratch:
        lowered = pathlib.Path(scratch) / "lowered.txt"
        lowered.write_text(f"{crate} {max(baseline - 1, 0)}\n")
        for label, extra, against in (
            ("recorded", 0, baseline_path),
            ("one_new_survivor", 1, baseline_path),
            ("baseline_lowered_by_one", 0, lowered),
        ):
            logs = pathlib.Path(scratch) / label
            synthetic_sweep(logs, crate, shards, total, survived, unviable, extra)
            exits[label] = run(crate, logs, against, record=False, build_root=build_root)
    wanted = {
        "recorded": EXIT_PASS,
        "one_new_survivor": EXIT_REGRESSION,
        "baseline_lowered_by_one": EXIT_REGRESSION,
    }
    if baseline == 0:
        # A baseline of 0 cannot be lowered, so that run is not a proof.
        wanted["baseline_lowered_by_one"] = EXIT_PASS
    if exits != wanted:
        raise ProofFailed(f"{crate}: the gate did not behave: {exits}")
    return {
        "crate": crate,
        "command": f"tools/mutants-ratchet.py --prove-gate {crate}",
        "input": (
            "synthetic shard logs in the runner's line format, with the recorded "
            "shard count and totals, and one MISSED line added. No sweep ran."
        ),
        "shards": shards,
        "modelled_artifact_sha256": result.get("artifact_sha256"),
        "baseline_survivors": baseline,
        "recorded_survivors": survived,
        "presented_survivors": survived + 1,
        "recorded_exit_code": exits["recorded"],
        "exit_code": exits["one_new_survivor"],
        "lowered_baseline_exit_code": exits["baseline_lowered_by_one"],
        "result": "rejected",
    }


# --- self-test ---------------------------------------------------------------
#
# A sweep is a manual job that takes hours, so the checks above cannot be
# developed against a real run. They are developed against these, which are the
# shard logs a run writes, written by hand.


def totals_line(total, caught, missed, unviable):
    body = f"{total} mutants: {caught} caught, {missed} missed, {unviable} unviable\n"
    entry = "MISSED src/lib.rs:{}: replace f with Default\n"
    return body + "".join(entry.format(i) for i in range(missed))


def write_shards(root, crate, shards):
    target = pathlib.Path(root) / "crates" / crate / f"{crate}_mutants"
    for name, text in shards.items():
        (target / name).mkdir(parents=True)
        (target / name / "test.log").write_text(text)


def self_test():
    clean = {
        "shard_1_of_2": totals_line(10, 8, 1, 1),
        "shard_2_of_2": totals_line(10, 7, 2, 1),
    }
    # Each case is (name, shards, baseline text, BUILD text, expected exit).
    # A BUILD text of None writes no BUILD file, so no shard count is declared.
    cases = [
        ("at the baseline", clean, "demo 3", None, EXIT_PASS),
        ("under the baseline", clean, "demo 5", None, EXIT_PASS),
        ("over the baseline", clean, "demo 2", None, EXIT_REGRESSION),
        ("unseeded baseline", clean, f"demo {UNSEEDED}", None, EXIT_PASS),
        ("no baseline line", clean, "other 3", None, EXIT_REGRESSION),
        # No crate is unsharded today, but the rule holds for one that is.
        ("an unsharded target", {"": totals_line(10, 9, 1, 0)}, "demo 1", None, EXIT_PASS),
        (
            "an indented log, as Bazel prints one to the console",
            {"shard_1_of_1": "  10 mutants: 9 caught, 1 missed, 0 unviable\n"
             "  MISSED src/lib.rs:12: replace triple with 0\n"},
            "demo 1",
            None,
            EXIT_PASS,
        ),
        (
            "a shard Bazel killed reads as a clean crate without this check",
            {
                "shard_1_of_2": totals_line(10, 10, 0, 0),
                "shard_2_of_2": "running mutants\n-- Test timed out at 05:00 --\n",
            },
            "demo 0",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a shard that refused to sweep",
            {
                "shard_1_of_2": totals_line(10, 10, 0, 0),
                "shard_2_of_2": "cargo_mutants: the unmutated tests do not pass; "
                "fix them first\n",
            },
            "demo 0",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a shard that left no log",
            {"shard_1_of_4": totals_line(10, 10, 0, 0)},
            "demo 0",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a totals line that does not add up",
            {"shard_1_of_1": "10 mutants: 4 caught, 1 missed, 1 unviable\nMISSED x\n"},
            "demo 1",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a totals line that disagrees with its MISSED entries",
            {"shard_1_of_1": "10 mutants: 8 caught, 2 missed, 0 unviable\nMISSED x\n"},
            "demo 2",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a shard that reported twice",
            {"shard_1_of_1": totals_line(10, 10, 0, 0) + totals_line(10, 10, 0, 0)},
            "demo 0",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a sweep that enumerated nothing",
            {
                "shard_1_of_2": totals_line(0, 0, 0, 0),
                "shard_2_of_2": totals_line(0, 0, 0, 0),
            },
            "demo 0",
            None,
            EXIT_INCOMPLETE,
        ),
        ("a sweep that left no logs at all", {}, "demo 0", None, EXIT_INCOMPLETE),
        (
            "a complete sweep at the shard count BUILD declares",
            clean,
            "demo 3",
            "crate_tests(\n    mutants_shards = 2,\n)\n",
            EXIT_PASS,
        ),
        (
            "a complete sweep at another shard count than BUILD declares",
            clean,
            "demo 3",
            "crate_tests(\n    mutants_shards = 24,\n)\n",
            EXIT_INCOMPLETE,
        ),
        (
            "a BUILD file with no count gets the defs.bzl default of 8",
            clean,
            "demo 3",
            "crate_tests(lib = \"demo\")\n",
            EXIT_INCOMPLETE,
        ),
        (
            "an incomplete sweep over a regressed baseline is still incomplete",
            {
                "shard_1_of_2": totals_line(10, 5, 5, 0),
                "shard_2_of_2": "-- Test timed out at 05:00 --\n",
            },
            "demo 0",
            None,
            EXIT_INCOMPLETE,
        ),
    ]

    failures = 0
    for name, shards, baseline, build, expected in cases:
        with tempfile.TemporaryDirectory() as scratch:
            logs = pathlib.Path(scratch) / "bazel-testlogs"
            write_shards(logs, "demo", shards)
            baseline_path = pathlib.Path(scratch) / "baseline.txt"
            baseline_path.write_text(f"# a comment\n\n{baseline}\n")
            if build is not None:
                (pathlib.Path(scratch) / "crates" / "demo").mkdir(parents=True, exist_ok=True)
                (pathlib.Path(scratch) / "crates" / "demo" / "BUILD.bazel").write_text(build)
            got = run("demo", logs, baseline_path, record=False, build_root=scratch)
        verdict = "ok" if got == expected else "FAILED"
        if got != expected:
            failures += 1
        print(f"  {verdict:<8} exit {got}, expected {expected}: {name}", flush=True)

    # The JSON verdict is what a scheduled rerun reads, so its classes and
    # counts are checked whole: one crate passes, one regressed, one is
    # missing a shard, and the run as a whole reads as incomplete.
    with tempfile.TemporaryDirectory() as scratch:
        logs = pathlib.Path(scratch) / "bazel-testlogs"
        write_shards(logs, "good", clean)
        write_shards(logs, "worse", clean)
        write_shards(logs, "short", {"shard_1_of_2": totals_line(10, 10, 0, 0)})
        baseline_path = pathlib.Path(scratch) / "baseline.txt"
        baseline_path.write_text("good 3\nworse 2\nshort 0\n")
        out = pathlib.Path(scratch) / "verdict.json"
        got = run(["good", "worse", "short"], logs, baseline_path, record=False,
                  json_out=out, build_root=scratch)
        counts = {"total": 20, "caught": 15, "missed": 3, "unviable": 2, "shards": 2}
        expected_json = {
            "schema_version": 1,
            "status": INCOMPLETE,
            "crates": {
                "good": {
                    "status": PASS, "baseline": 3, "expected_shards": None,
                    "counts": counts, "reasons": [],
                },
                "worse": {
                    "status": REGRESSION, "baseline": 2, "expected_shards": None,
                    "counts": counts,
                    "reasons": [
                        "worse: 3 survivors, and the baseline allows 2. Kill 1 "
                        "survivor, or raise the baseline and say in the commit "
                        "message why the score went down."
                    ],
                },
                "short": {
                    "status": INCOMPLETE, "baseline": 0, "expected_shards": None,
                    "counts": None,
                    "reasons": ["short: the run split into 2 shards and 1 left no log at all"],
                },
            },
        }
        ok = got == EXIT_INCOMPLETE and json.loads(out.read_text()) == expected_json
        failures += 0 if ok else 1
        print(f"  {'ok' if ok else 'FAILED':<8} the JSON verdict names each crate's class", flush=True)

    # The proof of the gate is what the record quotes, so it runs here too,
    # against a record and a BUILD file in the recorded shape.
    with tempfile.TemporaryDirectory() as scratch:
        record_path = pathlib.Path(scratch) / "record.json"
        record_path.write_text(json.dumps({"results": {"demo": {
            "shards": 8, "total": 23, "caught": 21, "survived": 2, "unviable": 0,
        }}}))
        baseline_path = pathlib.Path(scratch) / "baseline.txt"
        baseline_path.write_text("demo 2\n")
        (pathlib.Path(scratch) / "crates" / "demo").mkdir(parents=True)
        (pathlib.Path(scratch) / "crates" / "demo" / "BUILD.bazel").write_text(
            "crate_tests(lib = \"demo\")\n"
        )
        block = prove_gate("demo", record_path, baseline_path, build_root=scratch)
        ok = block["exit_code"] == EXIT_REGRESSION and block["recorded_exit_code"] == EXIT_PASS
        failures += 0 if ok else 1
        print(f"  {'ok' if ok else 'FAILED':<8} one new survivor over the record is rejected", flush=True)

    # The recorded line is what a person checks in, so it is checked too.
    with tempfile.TemporaryDirectory() as scratch:
        logs = pathlib.Path(scratch) / "bazel-testlogs"
        write_shards(logs, "demo", clean)
        run("demo", logs, None, record=True, build_root=scratch)

    count = len(cases) + 2
    print(f"{count} cases, {failures} failed", flush=True)
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    root = pathlib.Path(__file__).resolve().parent
    parser.add_argument("crates", nargs="*", help="the crates whose sweeps to read")
    parser.add_argument(
        "--logs", default="bazel-testlogs", help="the bazel-testlogs directory to read"
    )
    parser.add_argument(
        "--baseline", default=str(root / "mutants-baseline.txt"),
        help="the survivor baseline to gate against",
    )
    parser.add_argument(
        "--record", action="store_true",
        help="print the baseline line this sweep would set, and gate nothing",
    )
    parser.add_argument(
        "--json", metavar="PATH",
        help="write the verdict as JSON to PATH, or to standard output for `-`",
    )
    parser.add_argument(
        "--prove-gate", metavar="CRATE",
        help="show that one new survivor over the recorded sweep fails the gate",
    )
    parser.add_argument(
        "--self-test", action="store_true",
        help="run the checks against synthetic shard logs, and read no sweep",
    )
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    keep_stdout_for_json(args.json)
    try:
        if args.prove_gate:
            block = prove_gate(args.prove_gate, RECORD, args.baseline)
            print(json.dumps(block, indent=2))
            return EXIT_PASS
        if not args.crates:
            parser.error("a crate is needed, or --self-test or --prove-gate")
        return run(args.crates, args.logs, args.baseline, args.record, args.json)
    except UsageError as error:
        print(f"mutants-ratchet.py: {error}", file=sys.stderr)
        return EXIT_USAGE
    except ProofFailed as error:
        annotate("error", str(error))
        return EXIT_REGRESSION


if __name__ == "__main__":
    sys.exit(main())
