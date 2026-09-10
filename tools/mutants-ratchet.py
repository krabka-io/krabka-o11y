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
"""

import argparse
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

UNSEEDED = "unseeded"


def annotate(level, message):
    """Prints a message, as a GitHub annotation when the run is on Actions."""
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::{level}::{message}", flush=True)
    else:
        print(f"{level}: {message}", flush=True)


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
    return {"total": total, "caught": caught, "missed": missed}, None


def read_sweep(logs_root, crate):
    """A crate's survivor count, or every reason the sweep cannot be trusted."""
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

    if faults:
        return None, faults

    total = sum(shard["total"] for shard in shards.values())
    if total == 0:
        return None, [
            f"{crate}: the sweep enumerated 0 mutants across {len(shards)} "
            f"shards. That is a broken run, not a clean one."
        ]
    return sum(shard["missed"] for shard in shards.values()), []


def read_baseline(path):
    """Crate name to survivor count, where `unseeded` reads as None."""
    baseline = {}
    for number, line in enumerate(pathlib.Path(path).read_text().splitlines(), 1):
        stripped = line.split("#", 1)[0].strip()
        if not stripped:
            continue
        fields = stripped.split()
        if len(fields) != 2:
            raise SystemExit(f"{path}:{number}: expected `<crate> <count>`: {line}")
        crate, count = fields
        if count == UNSEEDED:
            baseline[crate] = None
        elif count.isdigit():
            baseline[crate] = int(count)
        else:
            raise SystemExit(
                f"{path}:{number}: the count must be a number or `{UNSEEDED}`: {line}"
            )
    return baseline


def survivor_count(number):
    """`3 survivors`, and `1 survivor` for the one case that reads wrong."""
    return f"{number} survivor" + ("" if number == 1 else "s")


def gate(crate, survivors, baseline):
    """Applies the ratchet. Returns True when the crate may pass."""
    if crate not in baseline:
        annotate(
            "error",
            f"{crate} has no line in the baseline. Add `{crate} {survivors}` to "
            f"it, or `{crate} {UNSEEDED}` to leave the ratchet dormant.",
        )
        return False
    allowed = baseline[crate]
    if allowed is None:
        annotate(
            "notice",
            f"{crate}: {survivor_count(survivors)}. The baseline reads "
            f"`{UNSEEDED}`, so the ratchet is dormant for this crate. Check in "
            f"`{crate} {survivors}` to arm it.",
        )
        return True
    if survivors > allowed:
        annotate(
            "error",
            f"{crate}: {survivor_count(survivors)}, and the baseline allows "
            f"{allowed}. Kill {survivor_count(survivors - allowed)}, or raise "
            f"the baseline and say in the commit message why the score went down.",
        )
        return False
    if survivors < allowed:
        annotate(
            "notice",
            f"{crate}: {survivor_count(survivors)}, and the baseline allows "
            f"{allowed}. Lower the baseline to {survivors} to hold the ground.",
        )
        return True
    print(f"{crate}: {survivor_count(survivors)}, at its baseline.", flush=True)
    return True


def run(crate, logs_root, baseline_path, record):
    survivors, faults = read_sweep(logs_root, crate)
    for fault in faults:
        annotate("error", fault)
    if faults:
        return 1
    if record:
        print(f"{crate} {survivors}")
        return 0
    return 0 if gate(crate, survivors, read_baseline(baseline_path)) else 1


# --- self-test ---------------------------------------------------------------
#
# The sweep is a nightly job that takes hours, so the checks above cannot be
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
    # Each case is (name, shards, baseline text, expected exit code).
    cases = [
        ("at the baseline", clean, "demo 3", 0),
        ("under the baseline", clean, "demo 5", 0),
        ("over the baseline", clean, "demo 2", 1),
        ("unseeded baseline", clean, f"demo {UNSEEDED}", 0),
        ("no baseline line", clean, "other 3", 1),
        # No crate is unsharded today, but the rule holds for one that is.
        ("an unsharded target", {"": totals_line(10, 9, 1, 0)}, "demo 1", 0),
        (
            "an indented log, as Bazel prints one to the console",
            {"shard_1_of_1": "  10 mutants: 9 caught, 1 missed, 0 unviable\n"
             "  MISSED src/lib.rs:12: replace triple with 0\n"},
            "demo 1",
            0,
        ),
        (
            "a shard Bazel killed reads as a clean crate without this check",
            {
                "shard_1_of_2": totals_line(10, 10, 0, 0),
                "shard_2_of_2": "running mutants\n-- Test timed out at 05:00 --\n",
            },
            "demo 0",
            1,
        ),
        (
            "a shard that refused to sweep",
            {
                "shard_1_of_2": totals_line(10, 10, 0, 0),
                "shard_2_of_2": "cargo_mutants: the unmutated tests do not pass; "
                "fix them first\n",
            },
            "demo 0",
            1,
        ),
        (
            "a shard that left no log",
            {"shard_1_of_4": totals_line(10, 10, 0, 0)},
            "demo 0",
            1,
        ),
        (
            "a totals line that does not add up",
            {"shard_1_of_1": "10 mutants: 4 caught, 1 missed, 1 unviable\nMISSED x\n"},
            "demo 1",
            1,
        ),
        (
            "a totals line that disagrees with its MISSED entries",
            {"shard_1_of_1": "10 mutants: 8 caught, 2 missed, 0 unviable\nMISSED x\n"},
            "demo 2",
            1,
        ),
        (
            "a shard that reported twice",
            {"shard_1_of_1": totals_line(10, 10, 0, 0) + totals_line(10, 10, 0, 0)},
            "demo 0",
            1,
        ),
        (
            "a sweep that enumerated nothing",
            {
                "shard_1_of_2": totals_line(0, 0, 0, 0),
                "shard_2_of_2": totals_line(0, 0, 0, 0),
            },
            "demo 0",
            1,
        ),
        ("a sweep that left no logs at all", {}, "demo 0", 1),
    ]

    failures = 0
    for name, shards, baseline, expected in cases:
        with tempfile.TemporaryDirectory() as scratch:
            logs = pathlib.Path(scratch) / "bazel-testlogs"
            write_shards(logs, "demo", shards)
            baseline_path = pathlib.Path(scratch) / "baseline.txt"
            baseline_path.write_text(f"# a comment\n\n{baseline}\n")
            got = run("demo", logs, baseline_path, record=False)
        verdict = "ok" if got == expected else "FAILED"
        if got != expected:
            failures += 1
        print(f"  {verdict:<8} exit {got}, expected {expected}: {name}", flush=True)

    # The recorded line is what a person checks in, so it is checked too.
    with tempfile.TemporaryDirectory() as scratch:
        logs = pathlib.Path(scratch) / "bazel-testlogs"
        write_shards(logs, "demo", clean)
        run("demo", logs, None, record=True)

    print(f"{len(cases)} cases, {failures} failed", flush=True)
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    root = pathlib.Path(__file__).resolve().parent
    parser.add_argument("crate", nargs="?", help="the crate whose sweep to read")
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
        "--self-test", action="store_true",
        help="run the checks against synthetic shard logs, and read no sweep",
    )
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.crate:
        parser.error("a crate is needed, or --self-test")
    return run(args.crate, args.logs, args.baseline, args.record)


if __name__ == "__main__":
    sys.exit(main())
