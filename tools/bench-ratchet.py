#!/usr/bin/env python3
"""Reads a Criterion run and decides whether the benchmarks may pass.

Run it after `tools/bench.sh`, from the workspace root:

    tools/bench-ratchet.py
    tools/bench-ratchet.py --bench blockstore_write

It reads the estimates Criterion left under `benches/target/criterion` and
applies two gates, the same two `tools/mutants-ratchet.py` applies to a
mutation sweep.

The first gate is structural, and it is the one that matters most. A benchmark
harness that stops measuring does not fail: `cargo bench` exits 0 having run
nothing, and a target whose `criterion_group!` no longer names a function is
indistinguishable from one that ran. `tools/bench-baseline.txt` therefore
carries the inventory -- every benchmark id the suite is expected to produce --
and every check below turns one silent no-op into a named failure:

  * every id in the baseline produced an estimates file
  * every estimates file found has a line in the baseline
  * every estimate parses, with a finite, positive mean
  * every estimate carries the confidence interval Criterion writes with it
  * the run produced at least one benchmark

The second gate is the ratchet, and it is deliberately blunt. A benchmark id
whose baseline line carries a number fails when its mean is more than
`--tolerance` times that number. The default is 1.5, which is not a
performance target: it is the smallest ratio that a shared CI runner does not
produce on its own. Criterion's own default noise threshold is 2%, and a 2%
gate on a GitHub-hosted runner fires on scheduling noise several times a week.
A gate that fires without a cause gets turned off, which is worse than no
gate, so this one is set to catch the regression that matters -- an algorithm
that changed order, an index that stopped pruning, a copy that became a clone
per row -- and to stay quiet about everything else.

Noise is also measured rather than assumed. When Criterion's own confidence
interval for a benchmark is wider than `--noise-ceiling` of its mean, the
numeric gate for that benchmark is skipped and the run says so. A measurement
that noisy cannot support a verdict in either direction, and reporting one
would be inventing it.

A benchmark whose baseline reads `unseeded` skips the second gate only. The
structural gate still applies, so a benchmark with no number yet is still
unable to report a target that measured nothing as a clean run.

Numeric values in `tools/bench-baseline.txt` are valid only for the stable
runner recorded beside them. Use `--record` on that runner to refresh a value.

The exit code names the verdict, so a scheduled run can tell runner variance
and a broken run from a real regression:

  * 0: every gated benchmark is within its budget
  * 1: a benchmark with a steady measurement is over its budget
  * 2: the command line is wrong
  * 3: the run is incomplete. An estimate is missing, unreadable, or not
    listed in the baseline, so the run cannot support a verdict.
  * 4: no benchmark regressed, but one or more measurements were too noisy to
    gate on, or (in `--confirm`) a regression did not reproduce. This is
    runner variance, not a finding.

When classes mix, the lower row in this order wins: pass, noisy, regression,
incomplete. `--json <path>` writes the same verdict in machine-readable form,
with a `status` of `pass`, `noisy`, `regression`, `incomplete` or (from
`--confirm`) `variance`, each benchmark's class, and the reasons. `-` writes it
to standard output.

`--confirm <verdict.json>` reruns only the benchmarks that a previous
`--json` verdict called regressed, through `tools/bench.sh` into a separate
Criterion directory, and gates those again. It exits 1 only when a
regression reproduces, and 4 when none does. It reruns at most
`--confirm-limit` benchmarks, the worst first, and it stops the whole rerun
after `--confirm-timeout` seconds, which reads as incomplete. That keeps the
scheduled job bounded even when a slow runner makes every benchmark regress.

`--self-test` runs the checks against synthetic Criterion output and needs no
benchmark run.
"""

import argparse
import contextlib
import json
import math
import os
import pathlib
import re
import signal
import subprocess
import sys
import tempfile
import time

from ratchet_annotations import annotate, keep_stdout_for_json

UNSEEDED = "unseeded"

ROOT = pathlib.Path(__file__).resolve().parent.parent

EXIT_PASS = 0
EXIT_REGRESSION = 1
EXIT_USAGE = 2
EXIT_INCOMPLETE = 3
EXIT_NOISY = 4

PASS = "pass"
NOISY = "noisy"
REGRESSION = "regression"
INCOMPLETE = "incomplete"
# Only `--confirm` gives this class: a regression that did not reproduce.
VARIANCE = "variance"

# Worst first: a run takes the class of its worst benchmark.
RANK = [PASS, NOISY, VARIANCE, REGRESSION, INCOMPLETE]
EXIT_CODES = {
    PASS: EXIT_PASS,
    NOISY: EXIT_NOISY,
    VARIANCE: EXIT_NOISY,
    REGRESSION: EXIT_REGRESSION,
    INCOMPLETE: EXIT_INCOMPLETE,
}

# Where `--confirm` has `tools/bench.sh` write, so the first run's estimates
# and its uploaded artifact stay as they are.
CONFIRM_CRITERION = "benches/target/criterion-confirm"
DEFAULT_CONFIRM_LIMIT = 8
DEFAULT_CONFIRM_TIMEOUT = 900

# A benchmark id, as `tools/bench-baseline.txt` spells one. `--confirm` puts
# ids into a Criterion filter, which is a regular expression, so an id with any
# other character is refused rather than escaped.
BENCHMARK_ID = re.compile(r"^[A-Za-z0-9_./-]+$")
GROUP = re.compile(r"benchmark_group\(\s*\"([^\"]+)\"")

# `cargo bench` writes here. It is `benches/target`, not `//target`, because
# the benchmarks are a workspace of their own -- see //benches/README.md.
CRITERION = "benches/target/criterion"

# Criterion's own report directories sit beside the benchmark directories and
# hold no estimates. `report` is the top-level rendering; a benchmark
# directory holds `new`, `base` and `change` alongside it, and only `new` is
# this run's measurement.
NOT_A_BENCHMARK = {"report"}

# How much slower than its baseline a benchmark may be before the run fails.
DEFAULT_TOLERANCE = 1.5

# How wide Criterion's 95% confidence interval may be, as a fraction of the
# mean, before the measurement is treated as too noisy to gate on.
DEFAULT_NOISE_CEILING = 0.25


def estimate_files(criterion_root):
    """Benchmark id to its `new/estimates.json` path, over a Criterion tree.

    Criterion names a directory per path segment of a benchmark id, so
    `blockstore_write/parquet/10000` is three nested directories with
    `new/estimates.json` at the bottom. The id is that path, which is why it
    is read back off the filesystem rather than out of Criterion's own
    `benchmark.json`: the directory layout is what the tool documents, and the
    JSON beside it is an internal record whose fields have moved between
    releases.
    """
    root = pathlib.Path(criterion_root)
    if not root.is_dir():
        return {}
    found = {}
    for estimates in sorted(root.rglob("new/estimates.json")):
        # `<root>/<id segments...>/new/estimates.json`.
        parts = estimates.relative_to(root).parts[:-2]
        if not parts or parts[0] in NOT_A_BENCHMARK:
            continue
        found["/".join(parts)] = estimates
    return found


def read_estimate(name, path):
    """One benchmark's mean and interval width, or why it has neither.

    Criterion writes nanoseconds. The numbers are returned in the same unit,
    so the baseline file is in nanoseconds too and needs no conversion on
    either side.
    """
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError) as broken:
        return None, f"{name}: its estimates file could not be read: {broken}"

    mean = payload.get("mean")
    if not isinstance(mean, dict):
        return None, f"{name}: its estimates file has no `mean` object"

    point = mean.get("point_estimate")
    if not isinstance(point, (int, float)) or not math.isfinite(point) or point <= 0:
        return None, (
            f"{name}: its mean point estimate is {point!r}, and a positive "
            f"finite number of nanoseconds is expected. A benchmark that "
            f"measured nothing reports this rather than failing."
        )

    interval = mean.get("confidence_interval")
    if not isinstance(interval, dict):
        return None, f"{name}: its mean carries no `confidence_interval`"
    lower = interval.get("lower_bound")
    upper = interval.get("upper_bound")
    for label, bound in (("lower_bound", lower), ("upper_bound", upper)):
        if not isinstance(bound, (int, float)) or not math.isfinite(bound):
            return None, f"{name}: its confidence interval has no {label}"
    if upper < lower:
        return None, (
            f"{name}: its confidence interval runs backwards, "
            f"{lower} to {upper}"
        )

    return {"mean": float(point), "spread": (float(upper) - float(lower)) / point}, None


def read_baseline(path):
    """Benchmark id to allowed nanoseconds, where `unseeded` reads as None."""
    baseline = {}
    for number, line in enumerate(pathlib.Path(path).read_text().splitlines(), 1):
        stripped = line.split("#", 1)[0].strip()
        if not stripped:
            continue
        fields = stripped.split()
        if len(fields) != 2:
            raise UsageError(f"{path}:{number}: expected `<benchmark> <ns>`: {line}")
        name, allowed = fields
        if name in baseline:
            raise UsageError(f"{path}:{number}: {name} is listed twice")
        if allowed == UNSEEDED:
            baseline[name] = None
        else:
            try:
                baseline[name] = float(allowed)
            except ValueError:
                raise UsageError(
                    f"{path}:{number}: the budget must be nanoseconds or "
                    f"`{UNSEEDED}`: {line}"
                ) from None
    return baseline


def under(name, prefix):
    """Whether a benchmark id belongs to the bench target `prefix`.

    Every benchmark here names its Criterion group after the `[[bench]]`
    target it lives in, so the first segment of an id is the target's name and
    `--bench` can narrow both halves of the comparison to one target's ids.
    `--confirm` passes a set of ids instead, and then only those are read.
    """
    if isinstance(prefix, (set, frozenset)):
        return name in prefix
    return prefix is None or name == prefix or name.startswith(prefix + "/")


def duration(nanoseconds):
    """A wall-clock figure a person can read, from a count of nanoseconds."""
    for unit, scale in (("ns", 1.0), ("us", 1e3), ("ms", 1e6), ("s", 1e9)):
        if nanoseconds < scale * 1000:
            return f"{nanoseconds / scale:.3g} {unit}"
    return f"{nanoseconds / 1e9:.3g} s"


def read_run(criterion_root, baseline, only):
    """Every measurement this run produced, or every reason it cannot be read.

    The inventory check is symmetric on purpose. A benchmark in the baseline
    that produced no estimate is a target that stopped measuring, and a
    benchmark that produced an estimate with no baseline line is a target
    nothing has ever decided about. Both are silent today.
    """
    expected = {name for name in baseline if under(name, only)}
    found = {
        name: path
        for name, path in estimate_files(criterion_root).items()
        if under(name, only)
    }

    faults = []
    for name in sorted(expected - found.keys()):
        faults.append(
            f"{name}: the baseline lists it and the run produced no estimate "
            f"under {criterion_root}. The benchmark was removed from its "
            f"`criterion_group!`, or it failed before it measured anything."
        )
    for name in sorted(found.keys() - expected):
        faults.append(
            f"{name}: the run measured it and {ARGS_BASELINE_HINT} lists no "
            f"line for it. Add `{name} {UNSEEDED}` to leave the ratchet "
            f"dormant for it."
        )

    measured = {}
    for name, path in sorted(found.items()):
        numbers, fault = read_estimate(name, path)
        if fault:
            faults.append(fault)
        else:
            measured[name] = numbers

    if not faults and not measured:
        faults.append(
            f"the run produced no benchmark at all under {criterion_root}. "
            f"That is a broken run, not a fast one."
        )
    return measured, faults


# Filled in by `main` so the inventory message can name the file the caller
# passed rather than the default. Kept module-level because `read_run` is also
# the function `self_test` drives, and threading a display string through it
# would be a parameter that exists only for a message.
ARGS_BASELINE_HINT = "tools/bench-baseline.txt"


def gate(measured, baseline, tolerance, noise_ceiling):
    """Applies the ratchet. Returns each benchmark's class and reason.

    A class is `pass`, `noisy` (the interval is too wide to gate on) or
    `regression`. An `unseeded` benchmark reads as `pass`, since nothing gates
    it.
    """
    classes = {}
    for name, numbers in sorted(measured.items()):
        mean = numbers["mean"]
        spread = numbers["spread"]
        allowed = baseline[name]
        entry = {
            "class": PASS,
            "mean_ns": round(mean, 3),
            "spread": round(spread, 6),
            "baseline_ns": allowed,
            "reason": None,
        }
        classes[name] = entry

        if allowed is None:
            annotate(
                "notice",
                f"{name}: {duration(mean)}, +/-{spread * 100:.0f}%. The "
                f"baseline reads `{UNSEEDED}`, so the ratchet is dormant for "
                f"this benchmark. Check in `{name} {mean:.0f}` from a quiet, "
                f"dedicated runner to arm it.",
            )
            continue

        budget = allowed * tolerance
        if spread > noise_ceiling:
            reason = (
                f"{name}: {duration(mean)}, and Criterion's interval spans "
                f"{spread * 100:.0f}% of it, over the {noise_ceiling * 100:.0f}% "
                f"ceiling. The gate is skipped: this measurement cannot "
                f"support a verdict in either direction."
            )
            annotate("warning", reason)
            # A noisy measurement under its budget is still a pass: noise
            # cannot make a run look slower than it was by less than it was.
            if mean > budget:
                entry["class"] = NOISY
                entry["reason"] = reason
            continue

        if mean > budget:
            reason = (
                f"{name}: {duration(mean)}, and the baseline of "
                f"{duration(allowed)} allows {duration(budget)} at "
                f"{tolerance:g}x. That is {mean / allowed:.2f}x the baseline."
            )
            annotate("error", reason)
            entry["class"] = REGRESSION
            entry["reason"] = reason
        elif mean < allowed:
            annotate(
                "notice",
                f"{name}: {duration(mean)}, under its baseline of "
                f"{duration(allowed)}. Lower the baseline to {mean:.0f} to "
                f"hold the ground.",
            )
        else:
            print(
                f"{name}: {duration(mean)}, within {tolerance:g}x of its "
                f"baseline of {duration(allowed)}.",
                flush=True,
            )
    return classes


def worst(statuses):
    """The worst class in a collection, by RANK. An empty one is a pass."""
    return max(statuses, key=RANK.index, default=PASS)


def judge(criterion_root, baseline, only, tolerance, noise_ceiling):
    """A run's verdict: its class, each benchmark's class, and the reasons."""
    measured, faults = read_run(criterion_root, baseline, only)
    for fault in faults:
        annotate("error", fault)
    if faults:
        return {"status": INCOMPLETE, "benchmarks": {}, "reasons": faults}
    classes = gate(measured, baseline, tolerance, noise_ceiling)
    return {
        "status": worst(entry["class"] for entry in classes.values()),
        "benchmarks": classes,
        "reasons": [entry["reason"] for entry in classes.values() if entry["reason"]],
    }


def write_json(verdict, destination):
    """Writes a verdict to a path, or to standard output for `-`."""
    text = json.dumps(verdict, indent=2, sort_keys=True) + "\n"
    if destination == "-":
        sys.__stdout__.write(text)
        sys.__stdout__.flush()
    else:
        pathlib.Path(destination).write_text(text)


def run(criterion_root, baseline_path, only, record, tolerance, noise_ceiling,
        json_out=None):
    baseline = read_baseline(baseline_path)
    if record:
        measured, faults = read_run(criterion_root, baseline, only)
        for fault in faults:
            annotate("error", fault)
        if faults:
            return EXIT_INCOMPLETE
        for name, numbers in sorted(measured.items()):
            print(f"{name} {numbers['mean']:.0f}")
        return EXIT_PASS
    verdict = judge(criterion_root, baseline, only, tolerance, noise_ceiling)
    if json_out:
        write_json(dict(verdict, schema_version=1, tolerance=tolerance,
                        noise_ceiling=noise_ceiling), json_out)
    return EXIT_CODES[verdict["status"]]


# --- confirming a regression -------------------------------------------------
#
# One run on a shared remote runner can land on a slow host. A second run of
# only the regressed benchmarks tells that apart from a real regression: a
# real one reproduces, and a slow host rarely lands twice.


def bench_targets(benches_dir):
    """Criterion group name to the `[[bench]]` target that defines it.

    A group mostly shares its target's name, but not always: `index_load` is
    a group in the `index_prune` target. The rerun names targets, so the map is
    read out of the benchmark sources rather than guessed from the id.
    """
    targets = {}
    for source in sorted(pathlib.Path(benches_dir).glob("*.rs")):
        for group in GROUP.findall(source.read_text()):
            targets[group] = source.stem
    return targets


def confirm_plan(previous, targets, limit):
    """The benchmarks to rerun, as target to ids, and the ids left out.

    The worst ratio first, so a limit keeps the benchmarks most likely to be
    real. An id whose group no target defines, or whose name a regular
    expression would misread, cannot be rerun and is reported as such.
    """
    regressed = [
        (name, entry) for name, entry in previous.get("benchmarks", {}).items()
        if entry.get("class") == REGRESSION
    ]
    regressed.sort(key=lambda item: -(item[1]["mean_ns"] / item[1]["baseline_ns"]))
    plan, skipped, planned = {}, [], 0
    for name, _ in regressed:
        target = targets.get(name.split("/", 1)[0])
        if target is None or not BENCHMARK_ID.match(name) or planned >= limit:
            skipped.append(name)
            continue
        plan.setdefault(target, []).append(name)
        planned += 1
    return plan, skipped


def rerun(plan, criterion_root, timeout, command=None):
    """Reruns the planned ids through tools/bench.sh. Returns a fault or None.

    One `tools/bench.sh` call per target, each with an anchored, exact filter
    over that target's ids, and all into `criterion_root`. The first call
    clears that directory and the later ones keep it. `timeout` bounds the
    whole rerun, not each call.

    Each call starts in its own process group. On the bound, the whole group
    is stopped, so `cargo bench` and the benchmark binaries under
    `tools/bench.sh` stop with it and write nothing more into `criterion_root`.
    `command` replaces `tools/bench.sh <target>` in the self-test.
    """
    environment = dict(os.environ, CRITERION_HOME=str(pathlib.Path(criterion_root).resolve()))
    deadline = time.monotonic() + timeout
    first = True
    for target, ids in sorted(plan.items()):
        pattern = "^(" + "|".join(re.escape(name) for name in ids) + ")$"
        environment["KRABKA_BENCH_FILTER"] = pattern
        environment["KRABKA_BENCH_KEEP"] = "0" if first else "1"
        first = False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return f"the rerun passed its {timeout}s bound at {target}"
        argv = command(target) if command else [str(ROOT / "tools" / "bench.sh"), target]
        process = subprocess.Popen(argv, env=environment, cwd=ROOT, start_new_session=True)
        try:
            returncode = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            stop_group(process)
            return f"the rerun passed its {timeout}s bound at {target}"
        finally:
            if process.poll() is None:
                stop_group(process)
        if returncode != 0:
            return f"the rerun of {target} exited {returncode}"
    return None


def stop_group(process, grace=10):
    """Stops every process in the group that `process` leads, then reaps it."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, sig)
        try:
            process.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            continue


def confirm(previous_path, criterion_root, baseline_path, tolerance,
            noise_ceiling, limit, timeout, json_out=None, runner=rerun,
            benches_dir=ROOT / "benches" / "benches"):
    """Reruns the regressed benchmarks and gates only those again.

    A regression that reproduces is `regression`, and one that does not is
    `variance`. A first verdict that was not a regression passes through as
    it was: there is nothing to confirm in it.

    The rerun is gated with the `tolerance` and `noise_ceiling` the first
    verdict records. `None` means "use the recorded value". A value that
    differs from the recorded one is refused, because a rerun gated more
    loosely than the first run calls a real regression variance.
    """
    previous = json.loads(pathlib.Path(previous_path).read_text())
    status = previous.get("status")
    if status not in RANK:
        raise UsageError(f"{previous_path} has no verdict status this script wrote")
    tolerance = recorded_threshold(previous_path, previous, "tolerance", tolerance,
                                   DEFAULT_TOLERANCE)
    noise_ceiling = recorded_threshold(previous_path, previous, "noise_ceiling",
                                       noise_ceiling, DEFAULT_NOISE_CEILING)
    if status != REGRESSION:
        print(f"the first run reads `{status}`, so there is nothing to confirm", flush=True)
        if json_out:
            write_json(dict(previous, confirmed=False), json_out)
        return EXIT_CODES[status]

    plan, skipped = confirm_plan(previous, bench_targets(benches_dir), limit)
    ids = {name for names in plan.values() for name in names}
    for name in skipped:
        annotate("warning", f"{name}: regressed on the first run and is not rerun")
    verdict = {"status": INCOMPLETE, "benchmarks": {}, "reasons": []}
    fault = runner(plan, criterion_root, timeout) if ids else "no regressed benchmark can be rerun"
    if fault:
        annotate("error", fault)
        verdict["reasons"] = [fault]
    else:
        verdict = judge(criterion_root, read_baseline(baseline_path), frozenset(ids),
                        tolerance, noise_ceiling)
        if verdict["status"] in (PASS, NOISY):
            verdict["status"] = VARIANCE
            annotate(
                "warning",
                f"none of the {len(ids)} regressed benchmarks regressed again. "
                f"The first run read runner variance, not a regression.",
            )
    # A skipped id stays unconfirmed. A reproduced regression is a finding
    # whatever else was skipped, so only a run that would otherwise pass is
    # held back by one.
    if skipped and verdict["status"] == VARIANCE:
        verdict["status"] = INCOMPLETE
        verdict["reasons"].append(
            f"{len(skipped)} regressed benchmarks were not rerun: {', '.join(skipped)}"
        )
    if json_out:
        write_json(dict(verdict, schema_version=1, confirmed=True,
                        rerun=sorted(ids), not_rerun=skipped), json_out)
    return EXIT_CODES[verdict["status"]]


def recorded_threshold(previous_path, previous, key, given, default):
    """The threshold the first verdict used. Refuses a different given one."""
    recorded = previous.get(key)
    if recorded is None:
        return default if given is None else given
    if given is not None and given != recorded:
        flag = "--" + key.replace("_", "-")
        raise UsageError(
            f"{previous_path} was gated with {key} {recorded}, and {flag} is {given}. "
            f"Leave {flag} out to confirm with the recorded value."
        )
    return recorded


class UsageError(Exception):
    """A fault in what the caller named: a baseline, a verdict, a flag."""


# --- self-test ---------------------------------------------------------------
#
# A benchmark run takes minutes and a nightly one takes longer, so the checks
# above cannot be developed against a real one. They are developed against
# these, which are the files Criterion writes, written by hand.


def estimates(mean, spread=0.02):
    """The `estimates.json` Criterion writes for one benchmark."""
    return json.dumps(
        {
            "mean": {
                "confidence_interval": {
                    "confidence_level": 0.95,
                    "lower_bound": mean * (1 - spread / 2),
                    "upper_bound": mean * (1 + spread / 2),
                },
                "point_estimate": mean,
                "standard_error": mean * spread / 4,
            },
            "median": {"point_estimate": mean},
        }
    )


def write_run(root, benchmarks):
    """A Criterion output tree holding one `new/estimates.json` per entry."""
    for name, text in benchmarks.items():
        where = pathlib.Path(root).joinpath(*name.split("/")) / "new"
        where.mkdir(parents=True)
        (where / "estimates.json").write_text(text)


def self_test():
    clean = {
        "blockstore_write/parquet/1000": estimates(2_000_000.0),
        "blockstore_write/parquet/10000": estimates(20_000_000.0),
    }
    inventory = "blockstore_write/parquet/1000 {}\nblockstore_write/parquet/10000 {}"
    both = inventory.format(UNSEEDED, UNSEEDED)

    # Each case is (name, benchmarks, baseline text, only, expected exit code).
    cases = [
        ("unseeded baseline", clean, both, None, 0),
        (
            "at the baseline",
            clean,
            inventory.format("2000000", "20000000"),
            None,
            0,
        ),
        (
            "within tolerance",
            clean,
            inventory.format("1600000", "16000000"),
            None,
            0,
        ),
        (
            "under the baseline",
            clean,
            inventory.format("4000000", "40000000"),
            None,
            0,
        ),
        (
            "over the tolerance",
            clean,
            inventory.format("1000000", "20000000"),
            None,
            EXIT_REGRESSION,
        ),
        (
            "a benchmark that stopped being registered",
            {"blockstore_write/parquet/1000": estimates(2_000_000.0)},
            both,
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a benchmark with no baseline line",
            clean,
            f"blockstore_write/parquet/1000 {UNSEEDED}",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "--bench narrows both halves to one target",
            dict(clean, **{"pprof_merge/two/100": estimates(500.0)}),
            both + f"\npprof_merge/two/100 {UNSEEDED}",
            "blockstore_write",
            0,
        ),
        (
            "a run that measured nothing",
            {},
            f"blockstore_write/parquet/1000 {UNSEEDED}",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a run with no benchmark and no inventory",
            {},
            "# nothing\n",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "an estimate of zero, which is what a benchmark that ran no "
            "iterations reports",
            {"solo/case": estimates(0.0)},
            f"solo/case {UNSEEDED}",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "an estimates file with no mean",
            {"solo/case": json.dumps({"median": {"point_estimate": 1.0}})},
            f"solo/case {UNSEEDED}",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "an estimates file that is not JSON",
            {"solo/case": "<html>a proxy error</html>"},
            f"solo/case {UNSEEDED}",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "an estimate with no confidence interval",
            {"solo/case": json.dumps({"mean": {"point_estimate": 10.0}})},
            f"solo/case {UNSEEDED}",
            None,
            EXIT_INCOMPLETE,
        ),
        (
            "a noisy measurement over its budget is noisy, not a regression",
            {"solo/case": estimates(1_000.0, spread=0.9)},
            "solo/case 100",
            None,
            EXIT_NOISY,
        ),
        (
            "a noisy measurement under its baseline still passes",
            {"solo/case": estimates(1_000.0, spread=0.9)},
            "solo/case 100000",
            None,
            0,
        ),
        (
            "Criterion's own report directory is not a benchmark",
            dict(clean, **{"report/index": estimates(1.0)}),
            both,
            None,
            0,
        ),
        (
            "a regression beside a noisy one is a regression",
            {
                "solo/fast": estimates(1_000.0),
                "solo/noisy": estimates(1_000.0, spread=0.9),
            },
            "solo/fast 100\nsolo/noisy 100",
            None,
            EXIT_REGRESSION,
        ),
        (
            "a missing estimate beside a regression is incomplete",
            {"solo/fast": estimates(1_000.0)},
            "solo/fast 100\nsolo/gone 100",
            None,
            EXIT_INCOMPLETE,
        ),
    ]

    failures = 0
    for name, benchmarks, baseline, only, expected in cases:
        with tempfile.TemporaryDirectory() as scratch:
            criterion = pathlib.Path(scratch) / "criterion"
            criterion.mkdir()
            write_run(criterion, benchmarks)
            baseline_path = pathlib.Path(scratch) / "baseline.txt"
            baseline_path.write_text(f"# a comment\n\n{baseline}\n")
            got = run(
                criterion,
                baseline_path,
                only,
                record=False,
                tolerance=DEFAULT_TOLERANCE,
                noise_ceiling=DEFAULT_NOISE_CEILING,
            )
        verdict = "ok" if got == expected else "FAILED"
        if got != expected:
            failures += 1
        print(f"  {verdict:<8} exit {got}, expected {expected}: {name}", flush=True)

    # A duplicated line is a baseline nobody can reason about, and it is the
    # one input that raises rather than returning an exit code.
    with tempfile.TemporaryDirectory() as scratch:
        baseline_path = pathlib.Path(scratch) / "baseline.txt"
        baseline_path.write_text(f"solo/case 1\nsolo/case {UNSEEDED}\n")
        try:
            read_baseline(baseline_path)
        except UsageError:
            print("  ok       a benchmark listed twice is refused", flush=True)
        else:
            failures += 1
            print("  FAILED   a benchmark listed twice is refused", flush=True)

    # The recorded lines are what a person checks in, so they are checked too.
    with tempfile.TemporaryDirectory() as scratch:
        criterion = pathlib.Path(scratch) / "criterion"
        criterion.mkdir()
        write_run(criterion, clean)
        baseline_path = pathlib.Path(scratch) / "baseline.txt"
        baseline_path.write_text(both + "\n")
        run(
            criterion,
            baseline_path,
            None,
            record=True,
            tolerance=DEFAULT_TOLERANCE,
            noise_ceiling=DEFAULT_NOISE_CEILING,
        )

    failures += self_test_json()
    failures += self_test_confirm()
    failures += self_test_confirm_thresholds()
    failures += self_test_rerun_bound()
    print(f"{len(cases) + 1 + 1 + len(CONFIRM_CASES) + 1} cases, {failures} failed", flush=True)
    return 1 if failures else 0


def self_test_json():
    """The JSON verdict is what a scheduled confirm step reads, so it is
    checked whole: one benchmark passes, one is noisy, one regressed."""
    with tempfile.TemporaryDirectory() as scratch:
        criterion = pathlib.Path(scratch) / "criterion"
        criterion.mkdir()
        write_run(criterion, {
            "solo/fast": estimates(100.0),
            "solo/noisy": estimates(1_000.0, spread=0.5),
            "solo/slow": estimates(1_000.0),
        })
        baseline_path = pathlib.Path(scratch) / "baseline.txt"
        baseline_path.write_text("solo/fast 100\nsolo/noisy 100\nsolo/slow 100\n")
        out = pathlib.Path(scratch) / "verdict.json"
        got = run(criterion, baseline_path, None, record=False,
                  tolerance=DEFAULT_TOLERANCE, noise_ceiling=DEFAULT_NOISE_CEILING,
                  json_out=out)
        slow = (
            "solo/slow: 1 us, and the baseline of 100 ns allows 150 ns at "
            "1.5x. That is 10.00x the baseline."
        )
        noisy = (
            "solo/noisy: 1 us, and Criterion's interval spans 50% of it, over "
            "the 25% ceiling. The gate is skipped: this measurement cannot "
            "support a verdict in either direction."
        )
        expected = {
            "schema_version": 1,
            "status": REGRESSION,
            "tolerance": DEFAULT_TOLERANCE,
            "noise_ceiling": DEFAULT_NOISE_CEILING,
            "reasons": [noisy, slow],
            "benchmarks": {
                "solo/fast": {"class": PASS, "mean_ns": 100.0, "spread": 0.02,
                              "baseline_ns": 100.0, "reason": None},
                "solo/noisy": {"class": NOISY, "mean_ns": 1000.0, "spread": 0.5,
                               "baseline_ns": 100.0, "reason": noisy},
                "solo/slow": {"class": REGRESSION, "mean_ns": 1000.0, "spread": 0.02,
                              "baseline_ns": 100.0, "reason": slow},
            },
        }
        ok = got == EXIT_REGRESSION and json.loads(out.read_text()) == expected
    print(f"  {'ok' if ok else 'FAILED':<8} the JSON verdict names each benchmark's class",
          flush=True)
    return 0 if ok else 1


# Each case is (name, first verdict's status, the ids the rerun measures and
# their means, the rerun's fault, the limit, expected exit, expected rerun).
# The baseline is 100 ns for every id, so a mean of 1000 regresses.
CONFIRM_CASES = [
    ("a regression that reproduces fails", REGRESSION,
     {"alpha/a": 1_000.0, "beta/b": 1_000.0}, None, 8, EXIT_REGRESSION,
     {"alpha": ["alpha/a"], "beta_target": ["beta/b"]}),
    ("a regression that does not reproduce is variance", REGRESSION,
     {"alpha/a": 100.0, "beta/b": 100.0}, None, 8, EXIT_NOISY,
     {"alpha": ["alpha/a"], "beta_target": ["beta/b"]}),
    ("one of two reproducing still fails", REGRESSION,
     {"alpha/a": 100.0, "beta/b": 1_000.0}, None, 8, EXIT_REGRESSION,
     {"alpha": ["alpha/a"], "beta_target": ["beta/b"]}),
    ("a rerun that times out is incomplete", REGRESSION,
     {}, "the rerun passed its 900s bound at alpha", 8, EXIT_INCOMPLETE,
     {"alpha": ["alpha/a"], "beta_target": ["beta/b"]}),
    ("a rerun that measured nothing is incomplete", REGRESSION,
     {}, None, 8, EXIT_INCOMPLETE,
     {"alpha": ["alpha/a"], "beta_target": ["beta/b"]}),
    ("the limit reruns the worst first, and the rest keep a pass incomplete",
     REGRESSION, {"beta/b": 100.0}, None, 1, EXIT_INCOMPLETE,
     {"beta_target": ["beta/b"]}),
    ("the limit does not hide a regression that reproduces",
     REGRESSION, {"beta/b": 1_000.0}, None, 1, EXIT_REGRESSION,
     {"beta_target": ["beta/b"]}),
    ("a first run that only read noisy has nothing to confirm", NOISY,
     {}, None, 8, EXIT_NOISY, None),
    ("a first run that was incomplete stays incomplete", INCOMPLETE,
     {}, None, 8, EXIT_INCOMPLETE, None),
]


def self_test_confirm():
    """`--confirm` against a fake rerun that writes the estimates it is told to."""
    failures = 0
    for name, status, means, fault, limit, expected, expected_plan in CONFIRM_CASES:
        with tempfile.TemporaryDirectory() as scratch:
            scratch = pathlib.Path(scratch)
            # `beta` is a group in a target of another name, as `index_load`
            # is a group in `index_prune`.
            benches = scratch / "benches"
            benches.mkdir()
            (benches / "alpha.rs").write_text('c.benchmark_group("alpha");\n')
            (benches / "beta_target.rs").write_text('c.benchmark_group(\n    "beta",\n);\n')
            baseline_path = scratch / "baseline.txt"
            baseline_path.write_text("alpha/a 100\nbeta/b 100\ngamma/c 100\n")
            previous = scratch / "first.json"
            previous.write_text(json.dumps({"status": status, "benchmarks": {
                "alpha/a": {"class": REGRESSION, "mean_ns": 200.0, "baseline_ns": 100.0},
                "beta/b": {"class": REGRESSION, "mean_ns": 900.0, "baseline_ns": 100.0},
                "gamma/c": {"class": PASS, "mean_ns": 100.0, "baseline_ns": 100.0},
            }}))
            seen = []

            def fake(plan, criterion_root, timeout, means=means, fault=fault, seen=seen):
                seen.append({target: sorted(ids) for target, ids in plan.items()})
                if fault:
                    return fault
                write_run(criterion_root, {i: estimates(m) for i, m in means.items()})
                return None

            criterion = scratch / "criterion-confirm"
            criterion.mkdir()
            got = confirm(previous, criterion, baseline_path, DEFAULT_TOLERANCE,
                          DEFAULT_NOISE_CEILING, limit, DEFAULT_CONFIRM_TIMEOUT,
                          runner=fake, benches_dir=benches)
        plan = seen[0] if seen else None
        ok = got == expected and plan == expected_plan
        failures += 0 if ok else 1
        print(f"  {'ok' if ok else 'FAILED':<8} exit {got}, expected {expected}, "
              f"rerun {plan}: {name}", flush=True)

    # A rerun filter is a regular expression, so every id in it is escaped
    # and anchored, and a character outside the baseline's spelling is refused.
    plan, skipped = confirm_plan(
        {"benchmarks": {
            "alpha/a.b": {"class": REGRESSION, "mean_ns": 2.0, "baseline_ns": 1.0},
            "alpha/(x)": {"class": REGRESSION, "mean_ns": 3.0, "baseline_ns": 1.0},
            "nowhere/a": {"class": REGRESSION, "mean_ns": 4.0, "baseline_ns": 1.0},
        }},
        {"alpha": "alpha"},
        8,
    )
    ok = plan == {"alpha": ["alpha/a.b"]} and skipped == ["nowhere/a", "alpha/(x)"]
    failures += 0 if ok else 1
    print(f"  {'ok' if ok else 'FAILED':<8} an id no target defines, or a filter "
          f"would misread, is not rerun", flush=True)
    return failures


def self_test_confirm_thresholds():
    """`--confirm` gates the rerun with the thresholds the first verdict used."""
    failures = 0
    # The first run used --tolerance 1.2, so 130 against 100 regressed. The
    # rerun measures 130 again, which the default 1.5 would pass.
    cases = [
        ("the recorded tolerance gates the rerun", None, EXIT_REGRESSION),
        ("the same tolerance, given again, is accepted", 1.2, EXIT_REGRESSION),
        ("a different tolerance is refused", 1.5, EXIT_USAGE),
    ]
    for name, given, expected in cases:
        with tempfile.TemporaryDirectory() as scratch:
            scratch = pathlib.Path(scratch)
            benches = scratch / "benches"
            benches.mkdir()
            (benches / "alpha.rs").write_text('c.benchmark_group("alpha");\n')
            baseline_path = scratch / "baseline.txt"
            baseline_path.write_text("alpha/a 100\n")
            previous = scratch / "first.json"
            previous.write_text(json.dumps({
                "status": REGRESSION, "tolerance": 1.2,
                "noise_ceiling": DEFAULT_NOISE_CEILING,
                "benchmarks": {"alpha/a": {"class": REGRESSION, "mean_ns": 130.0,
                                           "baseline_ns": 100.0}},
            }))

            def fake(plan, criterion_root, timeout):
                write_run(criterion_root, {"alpha/a": estimates(130.0)})
                return None

            criterion = scratch / "criterion-confirm"
            criterion.mkdir()
            try:
                got = confirm(previous, criterion, baseline_path, given, None, 8,
                              DEFAULT_CONFIRM_TIMEOUT, runner=fake, benches_dir=benches)
            except UsageError:
                got = EXIT_USAGE
        ok = got == expected
        failures += 0 if ok else 1
        print(f"  {'ok' if ok else 'FAILED':<8} exit {got}, expected {expected}: {name}",
              flush=True)
    return failures


def process_alive(pid):
    """Whether `pid` runs. A zombie waiting for its reaper has stopped."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    stat = pathlib.Path(f"/proc/{pid}/stat")
    with contextlib.suppress(OSError):
        return stat.read_text().rsplit(")", 1)[1].split()[0] != "Z"
    return True


def self_test_rerun_bound():
    """On the bound, the rerun stops the benchmark under the script, not only the script."""
    with tempfile.TemporaryDirectory() as scratch:
        pid_file = pathlib.Path(scratch) / "child.pid"
        # The shell stands in for tools/bench.sh, and the `sleep` it starts
        # stands in for `cargo bench`.
        script = f'sleep 300 & echo $! > "{pid_file}"; wait'
        fault = rerun({"alpha": ["alpha/a"]}, scratch, 1,
                      command=lambda target: ["sh", "-c", script])
        child = int(pid_file.read_text()) if pid_file.exists() else None
        deadline = time.monotonic() + 10
        while child is not None and process_alive(child) and time.monotonic() < deadline:
            time.sleep(0.1)
        survived = child is not None and process_alive(child)
        if survived:
            with contextlib.suppress(ProcessLookupError):
                os.kill(child, signal.SIGKILL)
    ok = fault == "the rerun passed its 1s bound at alpha" and child is not None and not survived
    print(f"  {'ok' if ok else 'FAILED':<8} a rerun past its bound stops the whole "
          f"process group (fault {fault!r}, child {child}, survived {survived})", flush=True)
    return 0 if ok else 1


def main():
    global ARGS_BASELINE_HINT
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    root = pathlib.Path(__file__).resolve().parent
    parser.add_argument(
        "--criterion", default=CRITERION,
        help="the Criterion output directory to read",
    )
    parser.add_argument(
        "--baseline", default=str(root / "bench-baseline.txt"),
        help="the wall-clock baseline to gate against",
    )
    parser.add_argument(
        "--bench",
        help="narrow to one `[[bench]]` target's ids, for a partial run",
    )
    parser.add_argument(
        "--tolerance", type=float,
        help="how many times its baseline a benchmark may take (default 1.5; "
        "--confirm uses the verdict's)",
    )
    parser.add_argument(
        "--noise-ceiling", type=float,
        help="interval width, over the mean, past which the gate is skipped "
        "(--confirm uses the verdict's)",
    )
    parser.add_argument(
        "--record", action="store_true",
        help="print the baseline lines this run would set, and gate nothing",
    )
    parser.add_argument(
        "--json", metavar="PATH",
        help="write the verdict as JSON to PATH, or to standard output for `-`",
    )
    parser.add_argument(
        "--confirm", metavar="VERDICT",
        help="rerun the benchmarks a --json verdict called regressed, and gate those",
    )
    parser.add_argument(
        "--confirm-criterion", default=CONFIRM_CRITERION,
        help="the Criterion output directory the confirming rerun writes",
    )
    parser.add_argument(
        "--confirm-limit", type=int, default=DEFAULT_CONFIRM_LIMIT,
        help="the most benchmarks a confirming rerun measures (default 8)",
    )
    parser.add_argument(
        "--confirm-timeout", type=int, default=DEFAULT_CONFIRM_TIMEOUT,
        help="seconds the whole rerun may take (default 900)",
    )
    parser.add_argument(
        "--self-test", action="store_true",
        help="run the checks against synthetic Criterion output",
    )
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    keep_stdout_for_json(args.json)
    if args.tolerance is not None and args.tolerance < 1.0:
        parser.error("--tolerance below 1.0 fails a benchmark that got faster")
    if args.confirm and args.record:
        parser.error("--confirm gates a rerun, and --record gates nothing")
    if args.confirm_limit < 1 or args.confirm_timeout < 1:
        parser.error("--confirm-limit and --confirm-timeout must be positive")
    ARGS_BASELINE_HINT = args.baseline
    try:
        if args.confirm:
            return confirm(
                args.confirm,
                args.confirm_criterion,
                args.baseline,
                args.tolerance,
                args.noise_ceiling,
                args.confirm_limit,
                args.confirm_timeout,
                json_out=args.json,
            )
        return run(
            args.criterion,
            args.baseline,
            args.bench,
            args.record,
            DEFAULT_TOLERANCE if args.tolerance is None else args.tolerance,
            DEFAULT_NOISE_CEILING if args.noise_ceiling is None else args.noise_ceiling,
            json_out=args.json,
        )
    except UsageError as error:
        print(f"bench-ratchet.py: {error}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
