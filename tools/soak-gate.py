#!/usr/bin/env python3
"""Reads an operating-envelope soak report and decides whether it may pass.

The soak is `//crates/integration:soak_envelope_docker_test`. It writes
`soak-report.json`, schema version 2, with one entry per signal and phase.
Run this tool from the workspace root in one of four modes:

    tools/soak-gate.py --structural soak-report.json
    tools/soak-gate.py soak-report.json
    tools/soak-gate.py --record soak-report.json
    tools/soak-gate.py --envelope run-1.json run-2.json run-3.json

The first gate is structural, and every mode applies it. A soak that stops
measuring does not fail. A phase that a refactor drops, or a signal whose
writer silently accepts nothing, still produces a report and exits 0. Each
check below turns one such silent no-op into a named failure:

  * the report is schema version 2
  * the report names the commit, the host, the dataset and the shape it ran
  * its phase list is the eight phases, in run order
  * every signal has exactly one entry for every phase, and nothing else
  * every entry carries warm-up, duration, error rate, resident memory,
    write-ahead-log lag and object-store counters
  * every load phase carries write and query quantiles and ingest rates
  * the stepped phases carry their saturation search
  * the restart phase carries its recovery result
  * every signal's steady phase accepted at least one batch

The scheduled `scale` job on a shared runner applies this gate only. Its
numbers vary too much between hosts to support a verdict.

The second gate is the ratchet. It runs when a report is passed without
`--structural`, and it is for the stable runner only. `tools/soak-baseline.txt`
holds one line per `<signal>/<phase>/<metric>`. A metric whose line carries a
number fails when it moves against that number by more than `--tolerance`:

  * `write_p99_us`, `query_p99_us`: fail above baseline x tolerance
  * `accepted_rows_per_sec`: fails below baseline / tolerance
  * `peak_rss_kib`: fails above baseline x tolerance
  * `write_requests_per_op`, `read_requests_per_op`: fail above
    baseline x tolerance. In the `restart` phase, the read ratio is the
    object-store reads for each recovery attempt
  * `recovery_seconds`: fails above baseline x tolerance

The default tolerance is 1.5, for the same reason `tools/bench-ratchet.py`
uses 1.5. It catches a regression that changes order, not scheduling noise.

Noise is measured, not assumed. The soak reports the coefficient of variation
(CV) of the mean latency across 5-second windows. When that CV exceeds
`--noise-ceiling`, the ratchet skips the latency and throughput metrics for
that entry and says so. When the phase was too short to have two windows, the
CV is null and the same skip applies. Resident memory and requests per
operation do not depend on scheduling, so the ratchet never skips them.

The baseline is also the inventory. A baseline line that the report has no
value for fails, and a metric that the report has a value for but the baseline
does not list fails. A line that reads `unseeded` skips the ratchet only.

`--record` prints the baseline lines that one report would set.

`--envelope` reads three or more reports and prints, per signal, the highest
burst level and the highest cardinality step where every run met every
objective, with the accepted rate across the runs. That is the measured
saturation point. When no run reached a failing level, the result is a lower
bound, not a limit, and the output says so. The reports must come from
different runs, so each must have its own `run_id`. They must also name the
same dataset, shape and objectives, and the same build and platform: commit,
image digest, MinIO image, `rustc`, CPU count, memory, OS, architecture and
runner.

`--self-test` runs every check against synthetic reports and needs no soak.
"""

import argparse
import contextlib
import io
import json
import math
import os
import pathlib
import statistics
import sys
import tempfile

SCHEMA_VERSION = 2
UNSEEDED = "unseeded"

SIGNALS = ("metrics", "logs", "traces", "profiles")

# The run order in `crates/integration/tests/support/phases.rs`.
PHASES = (
    "steady",
    "burst",
    "high_cardinality",
    "cold_blocks",
    "compaction",
    "deletion",
    "noisy_tenant",
    "restart",
)

# Phases with writers. `cold_blocks` reads only, and `restart` only opens and
# queries.
WRITE_PHASES = {
    "steady",
    "burst",
    "high_cardinality",
    "compaction",
    "deletion",
    "noisy_tenant",
}
# Phases with a steady reader loop. `restart` reports recovery instead.
QUERY_PHASES = set(PHASES) - {"restart"}
# Phases whose object-store reads are gated. `restart` has no query-latency
# summary, but each recovery attempt reads the store, so its reads for each
# attempt are gated too.
READ_PHASES = QUERY_PHASES | {"restart"}
STEPPED_PHASES = {"burst": "writers", "high_cardinality": "series_per_batch"}

# How far a metric may move against its baseline before the run fails.
DEFAULT_TOLERANCE = 1.5

# The window CV of mean latency past which a latency or throughput verdict is
# skipped.
DEFAULT_NOISE_CEILING = 0.25

# The fewest runs that `--envelope` reads. One run cannot show variance, and
# two cannot tell which of them is the outlier.
MIN_ENVELOPE_RUNS = 3

# Metric name to (direction, noise source). `higher` fails when the value is
# above baseline x tolerance, `lower` when it is below baseline / tolerance.
# The noise source is the latency summary whose CV decides the skip, or None
# for a metric that scheduling does not move.
METRICS = {
    "write_p99_us": ("higher", "write"),
    "query_p99_us": ("higher", "query"),
    "accepted_rows_per_sec": ("lower", "write"),
    "peak_rss_kib": ("higher", None),
    "write_requests_per_op": ("higher", None),
    "read_requests_per_op": ("higher", None),
    "recovery_seconds": ("higher", None),
}


def annotate(level, message):
    """Prints a message, as a GitHub annotation when the run is on Actions."""
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::{level}::{message}", flush=True)
    else:
        print(f"{level}: {message}", flush=True)


def dig(value, *path):
    """`value[path[0]][path[1]]...`, or None where any step is missing."""
    for key in path:
        if isinstance(value, dict):
            value = value.get(key)
        elif isinstance(value, list) and isinstance(key, int) and key < len(value):
            value = value[key]
        else:
            return None
    return value


def number(value):
    """Whether `value` is a finite JSON number, and not a boolean."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def read_report(path):
    """The parsed report, or a fault that says why it cannot be read."""
    try:
        return json.loads(pathlib.Path(path).read_text()), None
    except (OSError, ValueError) as broken:
        return None, f"{path}: the report cannot be read: {broken}"


# --- structural gate ---------------------------------------------------------


def entry_faults(entry):
    """What one entry is missing. The entry's signal and phase are known."""
    phase = entry["phase"]
    faults = []

    def need_number(*path):
        if not number(dig(entry, *path)):
            faults.append(f"`{'.'.join(map(str, path))}` is not a number")

    need_number("warmup_seconds")
    need_number("duration_seconds")
    need_number("error_rate")
    for field in ("start", "peak", "end"):
        need_number("rss_kib", field)
    if not isinstance(dig(entry, "wal_lag", "status"), str):
        faults.append("`wal_lag.status` is missing")
    for side in ("write", "read", "maintenance"):
        for field in ("ops", "total_requests", "total_bytes", "requests_per_op"):
            need_number("object_store", side, field)

    duration = dig(entry, "duration_seconds")
    if number(duration) and duration <= 0:
        faults.append("`duration_seconds` is not positive")

    if phase in WRITE_PHASES:
        for field in ("attempted_batches", "accepted_batches", "accepted_rows_per_sec"):
            need_number("ingest", field)
        for field in ("count", "p50", "p95", "p99"):
            if field not in (dig(entry, "ingest", "latency_us") or {}):
                faults.append(f"`ingest.latency_us.{field}` is missing")
    if phase in QUERY_PHASES:
        need_number("query", "count")
        for field in ("count", "p50", "p95", "p99"):
            if field not in (dig(entry, "query", "latency_us") or {}):
                faults.append(f"`query.latency_us.{field}` is missing")
    if phase in STEPPED_PHASES:
        unit = dig(entry, "saturation", "unit")
        if unit != STEPPED_PHASES[phase]:
            faults.append(
                f"`saturation.unit` is {unit!r}, and "
                f"{STEPPED_PHASES[phase]!r} is expected"
            )
        if not isinstance(dig(entry, "saturation", "status"), str):
            faults.append("`saturation.status` is missing")
        levels = dig(entry, "levels")
        if not isinstance(levels, list) or not levels:
            faults.append("`levels` is empty, so the saturation search ran nothing")
    if phase == "restart":
        if dig(entry, "recovery", "status") not in ("recovered", "timed_out"):
            faults.append("`recovery.status` is missing")
        if "recovery_seconds" not in entry:
            faults.append("`recovery_seconds` is missing")
    if phase == "steady":
        accepted = dig(entry, "ingest", "accepted_batches")
        if number(accepted) and accepted <= 0:
            faults.append(
                "the steady phase accepted no batch. A writer that stopped "
                "writing reports this rather than failing."
            )
    return faults


def structural_faults(report):
    """Every reason the report is not a complete schema-2 soak report."""
    if not isinstance(report, dict):
        return ["the report is not a JSON object"]
    faults = []
    version = report.get("schema_version")
    if version != SCHEMA_VERSION:
        return [
            f"the report is schema version {version!r}, and this gate reads "
            f"version {SCHEMA_VERSION}"
        ]
    for field in ("run_id", "commit", "host", "dataset", "shape", "objectives"):
        if report.get(field) in (None, "", {}):
            faults.append(f"the report has no `{field}`")
    for field in ("warmup_seconds", "phase_seconds"):
        if not number(dig(report, "shape", field)):
            faults.append(f"the report has no `shape.{field}`")
    if not number(dig(report, "dataset", "seed")):
        faults.append("the report has no `dataset.seed`")
    if list(report.get("phases") or []) != list(PHASES):
        faults.append(
            f"the report's phases are {report.get('phases')!r}, and "
            f"{list(PHASES)!r} is expected"
        )

    entries = report.get("entries")
    if not isinstance(entries, list) or not entries:
        faults.append("the report has no entries. That is a broken run, not a clean one.")
        return faults
    seen = {}
    for index, entry in enumerate(entries):
        key = (dig(entry, "signal"), dig(entry, "phase"))
        if key[0] not in SIGNALS or key[1] not in PHASES:
            faults.append(f"entry {index} is {key[0]!r} {key[1]!r}, which no signal and phase name")
            continue
        if key in seen:
            faults.append(f"{key[0]} {key[1]}: the report has two entries")
            continue
        seen[key] = entry
    for signal in SIGNALS:
        for phase in PHASES:
            entry = seen.get((signal, phase))
            if entry is None:
                faults.append(
                    f"{signal} {phase}: the report has no entry. The phase "
                    f"was dropped, or the run failed before it."
                )
                continue
            faults.extend(f"{signal} {phase}: {fault}" for fault in entry_faults(entry))
    return faults


# --- ratchet -----------------------------------------------------------------


def metrics_of(entry):
    """Metric name to value for one entry. Only metrics with a value appear."""
    phase = entry["phase"]
    values = {"peak_rss_kib": dig(entry, "rss_kib", "peak")}
    if phase in WRITE_PHASES:
        values["write_p99_us"] = dig(entry, "ingest", "latency_us", "p99")
        values["accepted_rows_per_sec"] = dig(entry, "ingest", "accepted_rows_per_sec")
        values["write_requests_per_op"] = dig(entry, "object_store", "write", "requests_per_op")
    if phase in QUERY_PHASES:
        values["query_p99_us"] = dig(entry, "query", "latency_us", "p99")
    if phase in READ_PHASES:
        values["read_requests_per_op"] = dig(entry, "object_store", "read", "requests_per_op")
    if phase == "restart":
        values["recovery_seconds"] = dig(entry, "recovery_seconds")
    return {name: value for name, value in values.items() if number(value)}


def measured(report):
    """`<signal>/<phase>/<metric>` to (value, entry), over a whole report."""
    found = {}
    for entry in report["entries"]:
        for name, value in metrics_of(entry).items():
            found[f"{entry['signal']}/{entry['phase']}/{name}"] = (value, entry)
    return found


def read_baseline(path):
    """Metric id to its baseline number, where `unseeded` reads as None."""
    baseline = {}
    for line_number, line in enumerate(pathlib.Path(path).read_text().splitlines(), 1):
        stripped = line.split("#", 1)[0].strip()
        if not stripped:
            continue
        fields = stripped.split()
        if len(fields) != 2:
            raise SystemExit(f"{path}:{line_number}: expected `<signal>/<phase>/<metric> <value>`: {line}")
        name, allowed = fields
        if name in baseline:
            raise SystemExit(f"{path}:{line_number}: {name} is listed twice")
        if name.rsplit("/", 1)[-1] not in METRICS:
            raise SystemExit(f"{path}:{line_number}: {name} names no metric this gate reads")
        if allowed == UNSEEDED:
            baseline[name] = None
            continue
        try:
            baseline[name] = float(allowed)
        except ValueError:
            raise SystemExit(
                f"{path}:{line_number}: the value must be a number or `{UNSEEDED}`: {line}"
            ) from None
    return baseline


def noise(entry, source):
    """The window CV of the named latency summary, or None when unmeasured."""
    side = "ingest" if source == "write" else "query"
    cv = dig(entry, side, "latency_us", "cv")
    return cv if number(cv) else None


def ratchet(report, baseline, tolerance, noise_ceiling, baseline_name):
    """Applies the inventory and the ratchet. Returns True when the run may pass."""
    found = measured(report)
    passed = True
    for name in sorted(baseline.keys() - found.keys()):
        annotate(
            "error",
            f"{name}: {baseline_name} lists it and the report has no value "
            f"for it. The phase measured nothing, or the metric was dropped.",
        )
        passed = False
    for name in sorted(found.keys() - baseline.keys()):
        annotate(
            "error",
            f"{name}: the report measured it and {baseline_name} lists no "
            f"line for it. Add `{name} {UNSEEDED}` to leave the ratchet "
            f"dormant for it.",
        )
        passed = False

    for name in sorted(found.keys() & baseline.keys()):
        value, entry = found[name]
        allowed = baseline[name]
        metric = name.rsplit("/", 1)[-1]
        direction, source = METRICS[metric]
        if allowed is None:
            annotate(
                "notice",
                f"{name}: {value:g}. The baseline reads `{UNSEEDED}`, so the "
                f"ratchet is dormant for it.",
            )
            continue
        if source is not None:
            cv = noise(entry, source)
            if cv is None or cv > noise_ceiling:
                shown = "unmeasured" if cv is None else f"{cv * 100:.0f}%"
                annotate(
                    "warning",
                    f"{name}: {value:g}, and the {source} latency CV is "
                    f"{shown}, over the {noise_ceiling * 100:.0f}% ceiling. The "
                    f"gate is skipped: this measurement cannot support a "
                    f"verdict in either direction.",
                )
                continue
        if direction == "higher":
            budget = allowed * tolerance
            failed = value > budget
            better = value < allowed
        else:
            budget = allowed / tolerance
            failed = value < budget
            better = value > allowed
        if failed:
            annotate(
                "error",
                f"{name}: {value:g}, and the baseline of {allowed:g} allows "
                f"{budget:g} at {tolerance:g}x.",
            )
            passed = False
        elif better:
            annotate(
                "notice",
                f"{name}: {value:g}, better than its baseline of {allowed:g}. "
                f"Record a new baseline on the stable runner to hold the ground.",
            )
        else:
            print(f"{name}: {value:g}, within {tolerance:g}x of {allowed:g}.", flush=True)
    return passed


# --- envelope ----------------------------------------------------------------


def comparable(report):
    """The parts of a report that must match across the runs of one envelope.

    The workload is the dataset, the shape and the objectives. The build is
    the commit, the image digest, the MinIO image and `rustc`. The platform is
    the whole `host` object: CPU count, memory, OS, architecture and runner. A
    run that differs in any of them measures a different envelope.
    """
    return {
        "dataset": report.get("dataset"),
        "shape": report.get("shape"),
        "objectives": report.get("objectives"),
        "commit": report.get("commit"),
        "image_digest": report.get("image_digest"),
        "minio_image": report.get("minio_image"),
        "rustc": report.get("rustc"),
        "host": report.get("host"),
    }


def envelope(reports):
    """The highest level every run met, per signal and stepped phase.

    Returns (result, faults). Each run's saturation search stops at its first
    failing level, so a level that a run did not reach counts as not met.
    """
    faults = []
    if len(reports) < MIN_ENVELOPE_RUNS:
        return None, [
            f"--envelope needs {MIN_ENVELOPE_RUNS} or more runs, and it has "
            f"{len(reports)}"
        ]
    first_seen = {}
    for index, report in enumerate(reports, 1):
        run_id = report.get("run_id")
        if run_id in first_seen:
            faults.append(
                f"run {index} has the run ID {run_id!r} of run "
                f"{first_seen[run_id]}. One run passed twice cannot show "
                f"variance between runs."
            )
        else:
            first_seen[run_id] = index
    reference = comparable(reports[0])
    for index, report in enumerate(reports[1:], 2):
        different = [
            field for field, value in comparable(report).items()
            if value != reference[field]
        ]
        if different:
            faults.append(
                f"run {index} has a different {', '.join(different)} from "
                f"run 1, so the runs do not measure one envelope"
            )
    if faults:
        return None, faults

    result = {
        "runs": len(reports),
        "commit": reports[0].get("commit"),
        "run_ids": [report.get("run_id") for report in reports],
        "signals": {},
    }
    for signal in SIGNALS:
        per_phase = {}
        for phase, unit in STEPPED_PHASES.items():
            runs = []
            for report in reports:
                entry = next(
                    e for e in report["entries"]
                    if e["signal"] == signal and e["phase"] == phase
                )
                runs.append({level["level"]: level for level in entry["levels"]})
            configured = sorted(set().union(*runs))
            supported = None
            failing = None
            for level in configured:
                if all(dig(run.get(level), "objectives_met") is True for run in runs):
                    supported = level
                else:
                    failing = level
                    break
            rates = []
            if supported is not None:
                rates = [run[supported]["accepted_rows_per_sec"] for run in runs]
            mean = statistics.fmean(rates) if rates else None
            per_phase[phase] = {
                "unit": unit,
                "level": supported,
                "first_failing_level": failing,
                "bound": (
                    "none" if supported is None
                    else "saturated" if failing is not None
                    else "lower"
                ),
                "accepted_rows_per_sec_min": min(rates) if rates else None,
                "accepted_rows_per_sec_mean": mean,
                "accepted_rows_per_sec_cv": (
                    statistics.pstdev(rates) / mean if rates and mean else None
                ),
            }
        result["signals"][signal] = per_phase
    return result, []


# --- modes -------------------------------------------------------------------


def run(paths, mode, baseline_path, tolerance, noise_ceiling):
    """Runs one mode over the reports at `paths`. Returns the exit code."""
    reports = []
    for path in paths:
        report, fault = read_report(path)
        if fault:
            annotate("error", fault)
            return 1
        faults = structural_faults(report)
        for fault in faults:
            annotate("error", f"{path}: {fault}")
        if faults:
            return 1
        reports.append(report)

    if mode == "structural":
        for path in paths:
            print(f"{path}: structurally complete, schema version {SCHEMA_VERSION}.", flush=True)
        return 0
    if mode == "envelope":
        result, faults = envelope(reports)
        for fault in faults:
            annotate("error", fault)
        if faults:
            return 1
        for signal, phases in result["signals"].items():
            for phase, found in phases.items():
                if found["bound"] == "lower":
                    annotate(
                        "notice",
                        f"{signal} {phase}: every run met its objectives at "
                        f"the top level, {found['level']}. That is a lower "
                        f"bound, not a limit: add a higher level to find the "
                        f"saturation point.",
                    )
        print(json.dumps(result, indent=2, sort_keys=True), flush=True)
        return 0
    if mode == "record":
        for name, (value, _) in sorted(measured(reports[0]).items()):
            print(f"{name} {value:.6g}", flush=True)
        return 0

    baseline = read_baseline(baseline_path)
    passed = True
    for report in reports:
        passed &= ratchet(report, baseline, tolerance, noise_ceiling, str(baseline_path))
    return 0 if passed else 1


# --- self-test ---------------------------------------------------------------
#
# A soak takes minutes and runs on a schedule, so the checks above cannot be
# developed against a real one. They are developed against these, which are
# the reports the soak writes, built by hand.


def latency(p99, cv=0.05):
    return {"count": 100, "p50": p99 // 2, "p95": p99, "p99": p99, "max": p99,
            "cv": cv, "cv_windows": 3, "cv_window_seconds": 5}


def store(requests_per_op=2.0, ops=10):
    return {"ops": ops, "requests": {}, "bytes": {}, "failures": 0,
            "total_requests": requests_per_op * ops, "total_bytes": 1000,
            "requests_per_op": requests_per_op, "bytes_per_op": 100.0}


def synthetic_entry(signal, phase):
    entry = {
        "signal": signal,
        "phase": phase,
        "tenant": "soak",
        "warmup_seconds": 3.0,
        "duration_seconds": 12.0,
        "error_rate": 0.0,
        "objectives_met": True,
        "rss_kib": {"start": 1000, "peak": 2000, "end": 1500},
        "wal_lag": {"status": "not_measured", "reason": "synthetic"},
        "object_store": {"write": store(), "read": store(3.0), "maintenance": store(0.0, 0)},
        "recovery_seconds": None,
    }
    if phase in WRITE_PHASES:
        entry["ingest"] = {"attempted_batches": 40, "accepted_batches": 40,
                           "accepted_rows_per_sec": 4000.0, "latency_us": latency(10_000)}
    if phase in QUERY_PHASES:
        entry["query"] = {"count": 40, "errors": 0, "latency_us": latency(20_000)}
    if phase in STEPPED_PHASES:
        steps = [1, 2, 4, 8] if phase == "burst" else [100, 1000, 5000]
        entry["levels"] = [{"level": level, "objectives_met": True,
                            "accepted_rows_per_sec": 1000.0 * level} for level in steps]
        entry["saturation"] = {"unit": STEPPED_PHASES[phase], "level": steps[-1],
                               "first_failing_level": None, "status": "not_reached"}
    if phase == "restart":
        entry["recovery"] = {"status": "recovered", "attempts": 1}
        entry["recovery_seconds"] = 0.5
    return entry


def synthetic_report():
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": "synthetic",
        "commit": "0" * 40,
        "image_digest": None,
        "minio_image": {"ref": "minio:synthetic", "id": "sha256:" + "1" * 64},
        "rustc": "rustc 1.97.1",
        "host": {"cpus": 4, "memory_kib": 16_000_000, "os": "linux",
                 "arch": "x86_64", "runner": "synthetic"},
        "dataset": {"seed": 267},
        "shape": {"phase_seconds": 12.0, "warmup_seconds": 3.0},
        "objectives": {"max_error_rate": 0.0},
        "phases": list(PHASES),
        "entries": [synthetic_entry(s, p) for s in SIGNALS for p in PHASES],
    }


def entry_of(report, signal, phase):
    return next(e for e in report["entries"] if e["signal"] == signal and e["phase"] == phase)


def mutated(change):
    report = synthetic_report()
    change(report)
    return report


def runs(*reports):
    """Copies of `reports`, each with its own run ID, as separate runs have."""
    copies = json.loads(json.dumps(reports))
    for index, report in enumerate(copies, 1):
        report["run_id"] = f"synthetic-{index}"
    return copies


def baseline_text(report, overrides=None, drop=(), extra=()):
    """Baseline lines for `report`: every metric at its own value by default."""
    lines = []
    for name, (value, _) in sorted(measured(report).items()):
        if name in drop:
            continue
        lines.append(f"{name} {(overrides or {}).get(name, value)}")
    lines.extend(extra)
    return "\n".join(lines)


def self_test():
    clean = synthetic_report()
    all_unseeded = "\n".join(f"{name} {UNSEEDED}" for name in measured(clean))
    p99 = "metrics/steady/write_p99_us"

    def set_path(signal, phase, path, value):
        def change(report):
            target = entry_of(report, signal, phase)
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
        return change

    # Each case is (name, mode, reports, baseline text, expected exit code).
    cases = [
        ("a complete report is structurally complete", "structural", [clean], "", 0),
        ("another schema version", "structural",
         [mutated(lambda r: r.update(schema_version=1))], "", 1),
        ("a dropped phase", "structural",
         [mutated(lambda r: r["entries"].remove(entry_of(r, "logs", "deletion")))], "", 1),
        ("an entry nothing asked for", "structural",
         [mutated(lambda r: r["entries"].append(synthetic_entry("events", "steady")))], "", 1),
        ("a duplicated entry", "structural",
         [mutated(lambda r: r["entries"].append(synthetic_entry("traces", "burst")))], "", 1),
        ("a phase list out of order", "structural",
         [mutated(lambda r: r.update(phases=list(reversed(PHASES))))], "", 1),
        ("an entry with no resident memory", "structural",
         [mutated(lambda r: entry_of(r, "profiles", "compaction").pop("rss_kib"))], "", 1),
        ("an entry with no write quantiles", "structural",
         [mutated(set_path("metrics", "noisy_tenant", ["ingest", "latency_us"], {}))], "", 1),
        ("a restart with no recovery result", "structural",
         [mutated(lambda r: entry_of(r, "logs", "restart").pop("recovery"))], "", 1),
        ("a saturation search that ran nothing", "structural",
         [mutated(set_path("traces", "burst", ["levels"], []))], "", 1),
        ("a steady phase that accepted nothing", "structural",
         [mutated(set_path("profiles", "steady", ["ingest", "accepted_batches"], 0))], "", 1),
        ("a report with no commit", "structural",
         [mutated(lambda r: r.pop("commit"))], "", 1),
        ("an unseeded baseline", "gate", [clean], all_unseeded, 0),
        ("at the baseline", "gate", [clean], baseline_text(clean), 0),
        ("write p99 over the tolerance", "gate", [clean],
         baseline_text(clean, {p99: 5000}), 1),
        ("write p99 within the tolerance", "gate", [clean],
         baseline_text(clean, {p99: 8000}), 0),
        ("throughput under the tolerance", "gate", [clean],
         baseline_text(clean, {"logs/steady/accepted_rows_per_sec": 8000}), 1),
        ("throughput over the baseline", "gate", [clean],
         baseline_text(clean, {"logs/steady/accepted_rows_per_sec": 1000}), 0),
        ("resident memory over the tolerance", "gate", [clean],
         baseline_text(clean, {"traces/cold_blocks/peak_rss_kib": 1000}), 1),
        ("requests per operation over the tolerance", "gate", [clean],
         baseline_text(clean, {"profiles/compaction/read_requests_per_op": 1.0}), 1),
        ("recovery over the tolerance", "gate", [clean],
         baseline_text(clean, {"metrics/restart/recovery_seconds": 0.1}), 1),
        ("restart reads for each attempt over the tolerance", "gate", [clean],
         baseline_text(clean, {"logs/restart/read_requests_per_op": 1.0}), 1),
        ("a restart read ratio with no baseline line", "gate", [clean],
         baseline_text(clean, drop={"traces/restart/read_requests_per_op"}), 1),
        ("a noisy phase skips its latency verdict", "gate",
         [mutated(set_path("metrics", "steady", ["ingest", "latency_us", "cv"], 0.9))],
         baseline_text(clean, {p99: 5000}), 0),
        ("a phase too short for a CV skips its latency verdict", "gate",
         [mutated(set_path("metrics", "steady", ["ingest", "latency_us", "cv"], None))],
         baseline_text(clean, {p99: 5000}), 0),
        ("noise never skips resident memory", "gate",
         [mutated(set_path("traces", "cold_blocks", ["query", "latency_us", "cv"], 0.9))],
         baseline_text(clean, {"traces/cold_blocks/peak_rss_kib": 1000}), 1),
        ("a baseline line the report has no value for", "gate",
         [mutated(set_path("metrics", "steady", ["ingest", "latency_us", "p99"], None))],
         baseline_text(clean), 1),
        ("a metric with no baseline line", "gate", [clean],
         baseline_text(clean, drop={p99}), 1),
    ]

    failures = 0
    for name, mode, reports, baseline, expected in cases:
        with tempfile.TemporaryDirectory() as scratch:
            paths = []
            for index, report in enumerate(reports):
                path = pathlib.Path(scratch) / f"report-{index}.json"
                path.write_text(json.dumps(report))
                paths.append(path)
            baseline_path = pathlib.Path(scratch) / "baseline.txt"
            baseline_path.write_text(f"# a comment\n\n{baseline}\n")
            # Each case prints one line per metric. The verdict line is what
            # the self-test reports, so the rest goes nowhere.
            with contextlib.redirect_stdout(io.StringIO()):
                got = run(paths, mode, baseline_path, DEFAULT_TOLERANCE, DEFAULT_NOISE_CEILING)
        verdict = "ok" if got == expected else "FAILED"
        failures += got != expected
        print(f"  {verdict:<8} exit {got}, expected {expected}: {name}", flush=True)

    checks = 0

    def check(name, holds):
        nonlocal failures, checks
        checks += 1
        failures += not holds
        print(f"  {'ok' if holds else 'FAILED':<8} {name}", flush=True)

    # The envelope is a computation, so its cases check the value it computes.
    def failing_at(level, signal="metrics"):
        def change(report):
            entry = entry_of(report, signal, "burst")
            for step in entry["levels"]:
                if step["level"] >= level:
                    step["objectives_met"] = False
            entry["levels"] = [s for s in entry["levels"] if s["level"] <= level]
        return mutated(change)

    result, faults = envelope(runs(clean, clean, clean))
    check(
        "three runs that never failed give a lower bound at the top level",
        not faults and result["signals"]["metrics"]["burst"]["level"] == 8
        and result["signals"]["metrics"]["burst"]["bound"] == "lower",
    )
    result, faults = envelope(runs(clean, failing_at(4), clean))
    burst = result["signals"]["metrics"]["burst"] if result else {}
    check(
        "one run failing at 4 writers sets the envelope at 2",
        not faults and burst.get("level") == 2 and burst.get("first_failing_level") == 4
        and burst.get("bound") == "saturated"
        and burst.get("accepted_rows_per_sec_min") == 2000.0,
    )
    result, faults = envelope(runs(clean, failing_at(1, "logs"), clean))
    check(
        "a run failing at the first level gives no envelope",
        not faults and result["signals"]["logs"]["burst"]["level"] is None
        and result["signals"]["logs"]["burst"]["bound"] == "none",
    )
    _, faults = envelope(runs(clean, clean))
    check("two runs are refused", bool(faults))
    _, faults = envelope([clean, clean, clean])
    check("one report passed three times is refused", bool(faults))
    _, faults = envelope([*runs(clean, clean), mutated(lambda r: r.update(run_id="synthetic-1"))])
    check("two reports with one run ID are refused", bool(faults))
    # Each case changes one part of the workload, build or platform in the
    # third run. A run that differs in any of them measures another envelope.
    for name, change in [
        ("dataset", lambda r: r["dataset"].update(seed=1)),
        ("commit", lambda r: r.update(commit="f" * 40)),
        ("image digest", lambda r: r.update(image_digest="sha256:" + "2" * 64)),
        ("MinIO image", lambda r: r["minio_image"].update(id="sha256:" + "3" * 64)),
        ("rustc", lambda r: r.update(rustc="rustc 1.98.0")),
        ("runner", lambda r: r["host"].update(runner="another")),
        ("OS", lambda r: r["host"].update(os="macos")),
        ("architecture", lambda r: r["host"].update(arch="aarch64")),
        ("CPU count", lambda r: r["host"].update(cpus=8)),
    ]:
        _, faults = envelope(runs(clean, clean, mutated(change)))
        check(f"runs with a different {name} are refused", bool(faults))

    # A duplicated line is a baseline nobody can reason about, and it is the
    # one input that raises rather than returning an exit code.
    with tempfile.TemporaryDirectory() as scratch:
        path = pathlib.Path(scratch) / "baseline.txt"
        path.write_text(f"{p99} 1\n{p99} {UNSEEDED}\n")
        try:
            read_baseline(path)
            refused = False
        except SystemExit:
            refused = True
        check("a metric listed twice is refused", refused)

    # The recorded lines are what a person checks in, so they must read back
    # as a baseline the same report passes.
    with tempfile.TemporaryDirectory() as scratch:
        report_path = pathlib.Path(scratch) / "report.json"
        report_path.write_text(json.dumps(clean))
        recorded = pathlib.Path(scratch) / "recorded.txt"
        with recorded.open("w") as sink, contextlib.redirect_stdout(sink):
            run([report_path], "record", None, DEFAULT_TOLERANCE, DEFAULT_NOISE_CEILING)
        with contextlib.redirect_stdout(io.StringIO()):
            got = run([report_path], "gate", recorded, DEFAULT_TOLERANCE, DEFAULT_NOISE_CEILING)
        check("a recorded baseline reads back and passes its own report", got == 0)

    # The checked-in baseline is the inventory a real report is held to. The
    # synthetic report has every entry the soak writes, so the two must list
    # the same metrics.
    checked_in = read_baseline(pathlib.Path(__file__).resolve().parent / "soak-baseline.txt")
    check(
        "tools/soak-baseline.txt lists exactly the metrics a complete report measures",
        checked_in.keys() == measured(clean).keys(),
    )

    total = len(cases) + checks
    print(f"{total} cases, {failures} failed", flush=True)
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    root = pathlib.Path(__file__).resolve().parent
    parser.add_argument("reports", nargs="*", help="soak-report.json files to read")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--structural", action="store_true",
        help="check the report's shape only, for a shared runner",
    )
    mode.add_argument(
        "--record", action="store_true",
        help="print the baseline lines this report would set, and gate nothing",
    )
    mode.add_argument(
        "--envelope", action="store_true",
        help=f"compute the measured envelope over {MIN_ENVELOPE_RUNS} or more reports",
    )
    mode.add_argument(
        "--self-test", action="store_true",
        help="run the checks against synthetic reports",
    )
    parser.add_argument(
        "--baseline", default=str(root / "soak-baseline.txt"),
        help="the baseline to gate against",
    )
    parser.add_argument(
        "--tolerance", type=float, default=DEFAULT_TOLERANCE,
        help="how far a metric may move against its baseline (default 1.5)",
    )
    parser.add_argument(
        "--noise-ceiling", type=float, default=DEFAULT_NOISE_CEILING,
        help="latency window CV past which a latency or throughput verdict is skipped",
    )
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.tolerance < 1.0:
        parser.error("--tolerance below 1.0 fails a run that did not regress")
    if not args.reports:
        parser.error("name at least one soak report")
    if args.envelope and len(args.reports) < MIN_ENVELOPE_RUNS:
        parser.error(f"--envelope needs {MIN_ENVELOPE_RUNS} or more reports")
    if not (args.structural or args.envelope) and len(args.reports) != 1:
        parser.error("the ratchet and --record read one report")
    selected = (
        "structural" if args.structural
        else "record" if args.record
        else "envelope" if args.envelope
        else "gate"
    )
    return run(args.reports, selected, args.baseline, args.tolerance, args.noise_ceiling)


if __name__ == "__main__":
    sys.exit(main())
