"""The baseline-file reader that the ratchet tools share.

`tools/bench-ratchet.py` and `tools/mutants-ratchet.py` each keep a baseline
of two-field lines, `<name> <budget>`, with `#` comments and blank lines
allowed. This reads that shape; each tool parses its own budget.
"""

import pathlib


def baseline_fields(path, shape, usage_error):
    """Yields `(line number, line, name, budget)` for each entry in `path`.

    `shape` names the two fields in the error for a malformed line, as in
    `<crate> <count>`, and `usage_error` is the caller's exception for one.
    """
    for number, line in enumerate(pathlib.Path(path).read_text().splitlines(), 1):
        stripped = line.split("#", 1)[0].strip()
        if not stripped:
            continue
        fields = stripped.split()
        if len(fields) != 2:
            raise usage_error(f"{path}:{number}: expected `{shape}`: {line}")
        yield number, line, fields[0], fields[1]
