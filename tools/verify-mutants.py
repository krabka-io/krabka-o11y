#!/usr/bin/env python3
"""Hand-applies mutants to a function and reports whether a test kills each one.

The mutation sweep says a mutant survived; this says whether a specific test
would have caught it. Nothing else establishes that a test written in response
to a survivor actually kills it -- a test can pass both before and after a
mutation and prove nothing.

Usage:
    tools/verify-mutants.py cases.json

where cases.json is:
    {"file": "crates/x/src/y.rs",
     "function": "fn parse_thing(",
     "package": "crabka-x",
     "cases": [{"old": "...", "new": "...", "test": "...", "label": "..."}]}

`old` is matched inside `function`'s body only, and must appear exactly once
there; a patch that silently fails to apply produces a green run that proves
nothing, so an ambiguous match is reported rather than guessed at.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

# A mutant can make the code loop forever -- negating a scan loop's advance
# condition is the usual way. That is a kill, since the test never passes, but
# it has to be collected as one rather than waited on: cargo dies on timeout
# while the test binary it spawned keeps spinning and holding the output pipe,
# so a piped read blocks for as long as the process lives. Output goes to a
# file and the whole process group is killed.
TIMEOUT_SECONDS = 600
KILL_GRACE = 10


def run_case(path, original, start, end, case, package):
    body = original[start:end]
    hits = body.count(case["old"])
    if hits != 1:
        return f"SKIPPED ({hits} matches in function)"

    patched = original[:start] + body.replace(case["old"], case["new"]) + original[end:]
    with open(path, "w") as handle:
        handle.write(patched)

    with tempfile.NamedTemporaryFile(mode="r", suffix=".log") as log:
        result = subprocess.run(
            ["timeout", "-k", str(KILL_GRACE), str(TIMEOUT_SECONDS),
             "cargo", "test", "-p", package, "--lib", case["test"]],
            stdout=log.file, stderr=subprocess.STDOUT,
            start_new_session=True, check=False,
        )
        log.file.flush()
        output = open(log.name).read()

    if result.returncode == 124:
        return "killed (hung)"
    if "test result:" not in output:
        return "NO-COMPILE"
    return "SURVIVED" if "test result: ok. 1 passed" in output else "killed"


def main():
    spec = json.load(open(sys.argv[1]))
    path = spec["file"]
    original = open(path).read()
    start = original.index(spec["function"])
    end = original.index("\n}\n", start) + 3
    backup = path + ".verify-backup"
    shutil.copy(path, backup)

    try:
        for case in spec["cases"]:
            verdict = run_case(path, original, start, end, case, spec["package"])
            marker = "<-" if verdict == "SURVIVED" else "  "
            print(f"  {verdict:<16} {marker} {case['label']}", flush=True)
    finally:
        # Restore even on interrupt: a mutated file left behind is worse than
        # no result, because it looks like ordinary uncommitted work.
        shutil.copy(backup, path)
        os.remove(backup)
        print("  --- source restored", flush=True)


if __name__ == "__main__":
    main()
