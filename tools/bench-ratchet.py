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

**Every id in `tools/bench-baseline.txt` reads `unseeded` today, and the
numbers to replace them with cannot be measured on a shared machine.** A
baseline is a wall-clock number, so it is a statement about one machine under
one load. Seeding it needs a runner that is quiet, dedicated, and the same one
every night; until CI has that, the honest baseline is no baseline. Use
`--record` on such a runner to print the lines to check in.

`--self-test` runs the checks against synthetic Criterion output and needs no
benchmark run.
"""

import argparse
import json
import math
import os
import pathlib
import sys
import tempfile

UNSEEDED = "unseeded"

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


def annotate(level, message):
    """Prints a message, as a GitHub annotation when the run is on Actions."""
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::{level}::{message}", flush=True)
    else:
        print(f"{level}: {message}", flush=True)


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
            raise SystemExit(f"{path}:{number}: expected `<benchmark> <ns>`: {line}")
        name, allowed = fields
        if name in baseline:
            raise SystemExit(f"{path}:{number}: {name} is listed twice")
        if allowed == UNSEEDED:
            baseline[name] = None
        else:
            try:
                baseline[name] = float(allowed)
            except ValueError:
                raise SystemExit(
                    f"{path}:{number}: the budget must be nanoseconds or "
                    f"`{UNSEEDED}`: {line}"
                ) from None
    return baseline


def under(name, prefix):
    """Whether a benchmark id belongs to the bench target `prefix`.

    Every benchmark here names its Criterion group after the `[[bench]]`
    target it lives in, so the first segment of an id is the target's name and
    `--bench` can narrow both halves of the comparison to one target's ids.
    """
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
    """Applies the ratchet. Returns True when the run may pass."""
    passed = True
    for name, numbers in sorted(measured.items()):
        mean = numbers["mean"]
        spread = numbers["spread"]
        allowed = baseline[name]

        if allowed is None:
            annotate(
                "notice",
                f"{name}: {duration(mean)}, +/-{spread * 100:.0f}%. The "
                f"baseline reads `{UNSEEDED}`, so the ratchet is dormant for "
                f"this benchmark. Check in `{name} {mean:.0f}` from a quiet, "
                f"dedicated runner to arm it.",
            )
            continue

        if spread > noise_ceiling:
            annotate(
                "warning",
                f"{name}: {duration(mean)}, and Criterion's interval spans "
                f"{spread * 100:.0f}% of it, over the {noise_ceiling * 100:.0f}% "
                f"ceiling. The gate is skipped: this measurement cannot "
                f"support a verdict in either direction.",
            )
            continue

        budget = allowed * tolerance
        if mean > budget:
            annotate(
                "error",
                f"{name}: {duration(mean)}, and the baseline of "
                f"{duration(allowed)} allows {duration(budget)} at "
                f"{tolerance:g}x. That is {mean / allowed:.2f}x the baseline.",
            )
            passed = False
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
    return passed


def run(criterion_root, baseline_path, only, record, tolerance, noise_ceiling):
    baseline = read_baseline(baseline_path)
    measured, faults = read_run(criterion_root, baseline, only)
    for fault in faults:
        annotate("error", fault)
    if faults:
        return 1
    if record:
        for name, numbers in sorted(measured.items()):
            print(f"{name} {numbers['mean']:.0f}")
        return 0
    return 0 if gate(measured, baseline, tolerance, noise_ceiling) else 1


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
            1,
        ),
        (
            "a benchmark that stopped being registered",
            {"blockstore_write/parquet/1000": estimates(2_000_000.0)},
            both,
            None,
            1,
        ),
        (
            "a benchmark with no baseline line",
            clean,
            f"blockstore_write/parquet/1000 {UNSEEDED}",
            None,
            1,
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
            1,
        ),
        (
            "a run with no benchmark and no inventory",
            {},
            "# nothing\n",
            None,
            1,
        ),
        (
            "an estimate of zero, which is what a benchmark that ran no "
            "iterations reports",
            {"solo/case": estimates(0.0)},
            f"solo/case {UNSEEDED}",
            None,
            1,
        ),
        (
            "an estimates file with no mean",
            {"solo/case": json.dumps({"median": {"point_estimate": 1.0}})},
            f"solo/case {UNSEEDED}",
            None,
            1,
        ),
        (
            "an estimates file that is not JSON",
            {"solo/case": "<html>a proxy error</html>"},
            f"solo/case {UNSEEDED}",
            None,
            1,
        ),
        (
            "an estimate with no confidence interval",
            {"solo/case": json.dumps({"mean": {"point_estimate": 10.0}})},
            f"solo/case {UNSEEDED}",
            None,
            1,
        ),
        (
            "a measurement too noisy to gate on passes rather than failing",
            {"solo/case": estimates(1_000.0, spread=0.9)},
            "solo/case 100",
            None,
            0,
        ),
        (
            "a noisy measurement is still gated when it is under its baseline",
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
        except SystemExit:
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

    print(f"{len(cases) + 1} cases, {failures} failed", flush=True)
    return 1 if failures else 0


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
        "--tolerance", type=float, default=DEFAULT_TOLERANCE,
        help="how many times its baseline a benchmark may take (default 1.5)",
    )
    parser.add_argument(
        "--noise-ceiling", type=float, default=DEFAULT_NOISE_CEILING,
        help="interval width, over the mean, past which the gate is skipped",
    )
    parser.add_argument(
        "--record", action="store_true",
        help="print the baseline lines this run would set, and gate nothing",
    )
    parser.add_argument(
        "--self-test", action="store_true",
        help="run the checks against synthetic Criterion output",
    )
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.tolerance < 1.0:
        parser.error("--tolerance below 1.0 fails a benchmark that got faster")
    ARGS_BASELINE_HINT = args.baseline
    return run(
        args.criterion,
        args.baseline,
        args.bench,
        args.record,
        args.tolerance,
        args.noise_ceiling,
    )


if __name__ == "__main__":
    sys.exit(main())
