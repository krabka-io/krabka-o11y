"""Capture the first complete benchmark case in base/fix/fix/base order."""
import pathlib
import subprocess
import sys

output = pathlib.Path(__file__).parent
base, after = sys.argv[1:]
for label, binary in [
    ("base-repeat-1", base),
    ("after-repeat-1", after),
    ("after-repeat-2", after),
    ("base-repeat-2", base),
]:
    process = subprocess.Popen([binary, "3"], stdout=subprocess.PIPE, text=True)
    try:
        header = process.stdout.readline()
        row = process.stdout.readline()
        if not row.startswith("30,10,32,"):
            raise RuntimeError(f"incomplete first case: {row!r}")
        (output / f"{label}.csv").write_text(header + row)
        print(label, row.strip(), flush=True)
    finally:
        # All five queries of the first case finished before its CSV row.
        # Stop the remaining cases so this repeat uses the original binaries.
        process.terminate()
        process.wait()
