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
# A mutation can make a loop non-terminating -- dropping the digit cap in a
# decimal formatter is enough -- so a case that hangs costs this in full.
# Override it for a spec whose tests are fast: VERIFY_MUTANTS_TIMEOUT=60.
TIMEOUT_SECONDS = int(os.environ.get("VERIFY_MUTANTS_TIMEOUT", "600"))
KILL_GRACE = 10


def run_case(path, original, start, end, case, package, target, extra_args):
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
             "cargo", "test", "-p", package, *target, *extra_args, case["test"]],
            stdout=log.file, stderr=subprocess.STDOUT,
            start_new_session=True, check=False,
        )
        log.file.flush()
        output = open(log.name).read()

    if result.returncode == 124:
        return "killed (hung)"
    if "test result:" not in output:
        return "NO-COMPILE"
    # An empty test filter runs the whole suite, which is how to ask "does
    # anything at all catch this?" of a function with no test of its own. The
    # verdict is then whether every test still passed, not whether one did.
    if not case["test"]:
        return "SURVIVED" if " 0 failed;" in output and "FAILED" not in output else "killed"
    return "SURVIVED" if "test result: ok. 1 passed" in output else "killed"


def main():
    spec = json.load(open(sys.argv[1]))
    path = spec["file"]
    # A run killed outright -- Ctrl-C is handled by the `finally` at the end,
    # SIGKILL is not -- leaves the file mutated and its backup behind. Restore
    # before reading anything: otherwise that mutated source becomes the
    # baseline, and every verdict printed afterwards is measured against code
    # nobody wrote.
    backup = path + ".verify-backup"
    if os.path.exists(backup):
        print(f"stale backup found: restoring {path} from a killed run", flush=True)
        shutil.copy(backup, path)
        os.remove(backup)
    original = open(path).read()
    # Most crates keep their logic in the library, but a service's CLI value
    # parsers and config wiring live in main.rs, where `--lib` cannot see them
    # and every case silently reports NO-COMPILE. `"target"` selects the cargo
    # target set: "--bins" for those, "--lib" (the default) otherwise. A test
    # that only exists in an integration suite needs two arguments, so a list
    # is accepted too -- ["--test", "http"].
    target = spec.get("target", "--lib")
    if isinstance(target, str):
        target = [target]
    # A crate whose tests sit behind a non-default feature compiles them out of
    # a plain `cargo test`, so every mutant in the code they cover reports as a
    # survivor. `"features"` passes them through -- e.g. ["--all-features"].
    extra_args = spec.get("features", [])
    # A name can appear in more than one impl -- two types with their own
    # `from_f64` is the usual case -- and patching the first one found would
    # verify a mutant against a function the test never calls. `occurrence`
    # selects which -- **0-indexed**, so the second match is 1, not 2 -- and a
    # marker matching more than once without it is an error rather than a guess.
    marker = spec["function"]
    occurrences = [
        i for i in range(len(original)) if original.startswith(marker, i)
    ]
    if not occurrences:
        raise SystemExit(f"marker not found: {marker}")
    wanted = spec.get("occurrence")
    if wanted is None:
        if len(occurrences) > 1:
            raise SystemExit(
                f"marker matches {len(occurrences)} times, so it is ambiguous; "
                f"set \"occurrence\" to one of 0..{len(occurrences) - 1} "
                f"(0-indexed) to pick one: {marker}"
            )
        wanted = 0
    if not 0 <= wanted < len(occurrences):
        raise SystemExit(
            f"\"occurrence\": {wanted} is out of range; the marker matches "
            f"{len(occurrences)} times, so it must be 0..{len(occurrences) - 1} "
            f"(0-indexed): {marker}"
        )
    start = occurrences[wanted]
    end = original.index("\n    }\n", start) + 7 if marker.startswith("    ") \
        else original.index("\n}\n", start) + 3
    shutil.copy(path, backup)

    try:
        for case in spec["cases"]:
            verdict = run_case(
                path, original, start, end, case, spec["package"], target, extra_args
            )
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
