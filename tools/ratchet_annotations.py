"""Output helpers that the ratchet tools share.

`tools/bench-ratchet.py` and `tools/mutants-ratchet.py` report the same way:
GitHub annotations on Actions, plain lines elsewhere, and nothing but JSON on
standard output when `--json -` asks for it.
"""

import os
import sys


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
