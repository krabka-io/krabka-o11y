#!/usr/bin/env python3
"""Sorts a crate's mutation survivors into what is still worth reading.

A survivor list is a snapshot of the source as it was when the sweep ran.
Working it directly re-derives tests that already exist: three times in one
session I started writing a test for a function my own earlier commit had
already covered. This splits the list three ways so only the last group is
read:

  gone      -- the function no longer exists; the entry cannot be actioned
  untested  -- no test in the crate calls it; a genuine candidate
  exercised -- the tests already call it, so the entry is probably stale.
               "Probably" is the point: a called function can still have an
               uncovered arm, so this is a priority order, not a verdict.
               Confirm with tools/verify-mutants.py before believing it.

Usage: tools/survivor-triage.py <crate>
"""

import pathlib
import re
import subprocess
import sys


def survivors(crate):
    """Function name -> number of survivor entries naming it."""
    logs = subprocess.run(
        ["find", f"bazel-testlogs/crates/{crate}", "-name", "test.log"],
        capture_output=True, text=True, check=False,
    ).stdout.split()
    seen = set()
    for log in logs:
        seen.update(
            line for line in pathlib.Path(log).read_text(errors="replace").splitlines()
            if "MISSED" in line
        )
    counts = {}
    for line in seen:
        named = re.search(r"replace (?:<[^>]*>::)?([\w:]+)", line)
        if named:
            counts[named.group(1).split("::")[-1]] = (
                counts.get(named.group(1).split("::")[-1], 0) + 1
            )
    return counts


def main():
    crate = sys.argv[1]
    src = {p: p.read_text() for p in pathlib.Path(f"crates/{crate}/src").rglob("*.rs")}
    groups = {"gone": [], "untested": [], "exercised": []}
    for name, count in sorted(survivors(crate).items(), key=lambda kv: -kv[1]):
        defined_in = [p for p, text in src.items() if re.search(rf"fn {re.escape(name)}\b", text)]
        if not defined_in:
            groups["gone"].append((name, count, ""))
            continue
        # Count MENTIONS after the file's test module begins, not calls. The
        # tests routinely bind a function first -- `let f = super::thing;` --
        # and then call it as `f(..)`, so matching `thing(` misses every one
        # of those and reports a covered function as untested.
        refs = sum(
            len(re.findall(rf"\b{re.escape(name)}\b", src[p][src[p].index("mod tests {"):]))
            for p in defined_in
            if "mod tests {" in src[p]
        )
        where = str(defined_in[0]).replace(f"crates/{crate}/src/", "")
        groups["untested" if refs == 0 else "exercised"].append((name, count, where))

    for label, rows in groups.items():
        total = sum(count for _, count, _ in rows)
        print(f"{label}: {total} entries across {len(rows)} functions")
        if label == "untested":
            for name, count, where in rows:
                print(f"    {count:>3}  {name}  ({where})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
