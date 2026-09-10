#!/usr/bin/env python3
"""Build the checked-in seed corpora under //fuzz/corpus.

A libFuzzer run that starts from nothing spends its first minutes rediscovering
the framing of whatever it is decoding: that a Thrift field header is a nibble
pair, that a remote-write body is snappy over protobuf, that a PromQL query has
balanced braces. A seed corpus hands it those shapes on the first execution, so
the budget goes on the decode instead.

Every seed here comes from something the repository already holds. The query
corpora are mined from the vendored conformance corpora and from the query
strings the crates' own tests drive their parsers with. The binary corpora are
re-encodings of the fixtures those tests build: the Jaeger sample batch that
//crates/traces/src/wire/jaeger builds in `test_support`, the `WriteRequest`
that //crates/metrics/src/wire/v1 builds, and so on. The encoders below are
small enough to read in one sitting, and `--self-test` checks them against
values whose encodings are fixed by the formats rather than by this file.

Usage:

    tools/fuzz-corpus.py             # rewrite //fuzz/corpus
    tools/fuzz-corpus.py --check     # fail if //fuzz/corpus is out of date
    tools/fuzz-corpus.py --self-test # check the encoders, no repository needed
"""

from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import struct
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CORPUS = REPO / "fuzz" / "corpus"

# A corpus wants breadth, not volume: past a few dozen seeds the fuzzer's own
# coverage feedback picks better inputs than a mined list does, and every seed
# is a file a reviewer has to scroll past. Where a source yields more than this,
# an even stride across the sorted set is taken so the pick stays deterministic
# and spreads over the source files rather than clustering in the first one.
MAX_SEEDS = 64


# ---------------------------------------------------------------- encodings


def varint(value: int) -> bytes:
    """Base-128 varint, low group first, continuation bit on all but the last."""
    if value < 0:
        raise ValueError("varint is unsigned")
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def zigzag(value: int, bits: int) -> int:
    """Map a signed value onto an unsigned one that keeps small magnitudes small."""
    return ((value << 1) ^ (value >> (bits - 1))) & ((1 << bits) - 1)


def snappy_literal_block(payload: bytes) -> bytes:
    """A snappy raw block that copies nothing.

    The raw format is an uncompressed-length varint followed by tags. A literal
    tag has the two low bits clear; lengths up to 60 sit in the high six bits,
    and longer ones put `59 + n` there and follow with n little-endian length
    bytes. Emitting one literal covering the whole payload is a valid block that
    any decoder accepts, which is all a seed needs.
    """
    out = bytearray(varint(len(payload)))
    length = len(payload)
    if length == 0:
        return bytes(out)
    if length <= 60:
        out.append((length - 1) << 2)
    else:
        extra = (length - 1).to_bytes(4, "little").rstrip(b"\x00") or b"\x00"
        out.append(((59 + len(extra)) << 2))
        out.extend(extra)
    out.extend(payload)
    return bytes(out)


def pb_varint_field(number: int, value: int) -> bytes:
    return varint(number << 3) + varint(value)


def pb_sint32_field(number: int, value: int) -> bytes:
    return varint(number << 3) + varint(zigzag(value, 32))


def pb_sint64_field(number: int, value: int) -> bytes:
    return varint(number << 3) + varint(zigzag(value, 64))


def pb_int64_field(number: int, value: int) -> bytes:
    """A proto3 `int64`, which is a plain varint over the two's-complement bits."""
    return varint(number << 3) + varint(value & ((1 << 64) - 1))


def pb_double_field(number: int, value: float) -> bytes:
    return varint((number << 3) | 1) + struct.pack("<d", value)


def pb_bytes_field(number: int, value: bytes) -> bytes:
    return varint((number << 3) | 2) + varint(len(value)) + value


def pb_string_field(number: int, value: str) -> bytes:
    return pb_bytes_field(number, value.encode())


# ------------------------------------------------------- thrift, compact


class Compact:
    """The Thrift compact protocol, as //crates/traces/src/wire/jaeger reads it."""

    STOP = 0
    BOOL_TRUE = 1
    I32 = 5
    I64 = 6
    BINARY = 8
    LIST = 9
    MAP = 11
    STRUCT = 12

    def __init__(self) -> None:
        self.out = bytearray()

    def field(self, type_id: int, field_id: int, last: list[int]) -> None:
        delta = field_id - last[0]
        if 1 <= delta <= 15:
            self.out.append((delta << 4) | type_id)
        else:
            self.out.append(type_id)
            self.out.extend(varint(zigzag(field_id, 32)))
        last[0] = field_id

    def list_header(self, element_type: int, size: int) -> None:
        if size < 15:
            self.out.append((size << 4) | element_type)
        else:
            self.out.append(0xF0 | element_type)
            self.out.extend(varint(size))

    def map_header(self, key_type: int, value_type: int, size: int) -> None:
        self.out.extend(varint(size))
        if size:
            self.out.append((key_type << 4) | value_type)

    def binary(self, payload: bytes) -> None:
        self.out.extend(varint(len(payload)))
        self.out.extend(payload)

    def i64(self, value: int) -> None:
        self.out.extend(varint(zigzag(value, 64)))

    def i32(self, value: int) -> None:
        self.out.extend(varint(zigzag(value, 32)))

    def stop(self) -> None:
        self.out.append(self.STOP)


def compact_tag(writer: Compact, key: str, value: str) -> None:
    last = [0]
    writer.field(Compact.BINARY, 1, last)
    writer.binary(key.encode())
    writer.field(Compact.I32, 2, last)
    writer.i32(0)
    writer.field(Compact.BINARY, 3, last)
    writer.binary(value.encode())
    writer.stop()


def compact_bool_tag(writer: Compact, key: str) -> None:
    last = [0]
    writer.field(Compact.BINARY, 1, last)
    writer.binary(key.encode())
    writer.field(Compact.I32, 2, last)
    writer.i32(3)
    writer.field(Compact.BOOL_TRUE, 5, last)
    writer.stop()


def compact_span_ref(writer: Compact, ref_type: int, low: int, high: int, span: int) -> None:
    last = [0]
    writer.field(Compact.I32, 1, last)
    writer.i32(ref_type)
    writer.field(Compact.I64, 2, last)
    writer.i64(low)
    writer.field(Compact.I64, 3, last)
    writer.i64(high)
    writer.field(Compact.I64, 4, last)
    writer.i64(span)
    writer.stop()


def compact_log(writer: Compact) -> None:
    last = [0]
    writer.field(Compact.I64, 1, last)
    writer.i64(1_005)
    writer.field(Compact.LIST, 2, last)
    writer.list_header(Compact.STRUCT, 2)
    compact_tag(writer, "event", "cache.miss")
    compact_tag(writer, "cache.key", "users")
    writer.stop()


def jaeger_compact_batch() -> bytes:
    """The sample batch //crates/traces/src/wire/jaeger builds in `test_support`.

    One process with a tag, one span carrying two references, three tags and a
    log -- so a decode of it visits every read the module has.
    """
    writer = Compact()
    last = [0]

    writer.field(Compact.STRUCT, 1, last)
    process = [0]
    writer.field(Compact.BINARY, 1, process)
    writer.binary(b"checkout")
    writer.field(Compact.LIST, 2, process)
    writer.list_header(Compact.STRUCT, 1)
    compact_tag(writer, "process.tag", "present")
    writer.stop()

    writer.field(Compact.LIST, 2, last)
    writer.list_header(Compact.STRUCT, 1)
    span = [0]
    writer.field(Compact.I64, 1, span)
    writer.i64(2)
    writer.field(Compact.I64, 2, span)
    writer.i64(1)
    writer.field(Compact.I64, 3, span)
    writer.i64(3)
    writer.field(Compact.I64, 4, span)
    writer.i64(0)
    writer.field(Compact.BINARY, 5, span)
    writer.binary(b"GET /")
    writer.field(Compact.LIST, 6, span)
    writer.list_header(Compact.STRUCT, 2)
    compact_span_ref(writer, 0, 2, 1, 4)
    compact_span_ref(writer, 1, 5, 6, 7)
    writer.field(Compact.I32, 7, span)
    writer.i32(0)
    writer.field(Compact.I64, 8, span)
    writer.i64(1_000)
    writer.field(Compact.I64, 9, span)
    writer.i64(25)
    writer.field(Compact.LIST, 10, span)
    writer.list_header(Compact.STRUCT, 3)
    compact_tag(writer, "span.kind", "server")
    compact_tag(writer, "http.method", "GET")
    compact_bool_tag(writer, "error")
    writer.field(Compact.LIST, 11, span)
    writer.list_header(Compact.STRUCT, 1)
    compact_log(writer)
    writer.stop()

    writer.stop()
    return bytes(writer.out)


def jaeger_compact_empty_batch() -> bytes:
    """A batch with neither field set: one stop byte."""
    writer = Compact()
    writer.stop()
    return bytes(writer.out)


def jaeger_compact_skipped_fields() -> bytes:
    """A batch whose only fields are ones the decoder has to skip.

    A map and a long-form field id are the two skip shapes a real Jaeger client
    never emits, so nothing else in the corpus reaches them.
    """
    writer = Compact()
    last = [0]
    writer.field(Compact.MAP, 7, last)
    writer.map_header(Compact.I64, Compact.BINARY, 2)
    writer.i64(1)
    writer.binary(b"a")
    writer.i64(-2)
    writer.binary(b"")
    writer.field(Compact.LIST, 900, last)
    writer.list_header(Compact.I64, 17)
    for value in range(17):
        writer.i64(value - 8)
    writer.stop()
    return bytes(writer.out)


# -------------------------------------------------------- thrift, binary


class Binary:
    """The Thrift binary protocol, as `decode_jaeger_binary_thrift` reads it."""

    STOP = 0
    I32 = 8
    I64 = 10
    BINARY = 11
    LIST = 15
    STRUCT = 12

    def __init__(self) -> None:
        self.out = bytearray()

    def field(self, type_id: int, field_id: int) -> None:
        self.out.append(type_id)
        self.out.extend(struct.pack(">h", field_id))

    def list_header(self, element_type: int, size: int) -> None:
        self.out.append(element_type)
        self.out.extend(struct.pack(">i", size))

    def binary(self, payload: bytes) -> None:
        self.out.extend(struct.pack(">i", len(payload)))
        self.out.extend(payload)

    def i32(self, value: int) -> None:
        self.out.extend(struct.pack(">i", value))

    def i64(self, value: int) -> None:
        self.out.extend(struct.pack(">q", value))

    def stop(self) -> None:
        self.out.append(self.STOP)


def binary_tag(writer: Binary, key: str, value: str) -> None:
    writer.field(Binary.BINARY, 1)
    writer.binary(key.encode())
    writer.field(Binary.I32, 2)
    writer.i32(0)
    writer.field(Binary.BINARY, 3)
    writer.binary(value.encode())
    writer.stop()


def jaeger_binary_batch() -> bytes:
    """The same batch as `jaeger_compact_batch`, in the binary framing."""
    writer = Binary()

    writer.field(Binary.STRUCT, 1)
    writer.field(Binary.BINARY, 1)
    writer.binary(b"checkout")
    writer.field(Binary.LIST, 2)
    writer.list_header(Binary.STRUCT, 1)
    binary_tag(writer, "process.tag", "present")
    writer.stop()

    writer.field(Binary.LIST, 2)
    writer.list_header(Binary.STRUCT, 1)
    writer.field(Binary.I64, 1)
    writer.i64(2)
    writer.field(Binary.I64, 2)
    writer.i64(1)
    writer.field(Binary.I64, 3)
    writer.i64(3)
    writer.field(Binary.BINARY, 5)
    writer.binary(b"GET /")
    writer.field(Binary.LIST, 6)
    writer.list_header(Binary.STRUCT, 1)
    writer.field(Binary.I32, 1)
    writer.i32(0)
    writer.field(Binary.I64, 2)
    writer.i64(2)
    writer.field(Binary.I64, 3)
    writer.i64(1)
    writer.field(Binary.I64, 4)
    writer.i64(4)
    writer.stop()
    writer.field(Binary.I64, 8)
    writer.i64(1_000)
    writer.field(Binary.I64, 9)
    writer.i64(25)
    writer.field(Binary.LIST, 10)
    writer.list_header(Binary.STRUCT, 1)
    binary_tag(writer, "span.kind", "server")
    writer.stop()

    writer.stop()
    return bytes(writer.out)


# ------------------------------------------------------ prometheus bodies


def pb_label(name: str, value: str) -> bytes:
    return pb_string_field(1, name) + pb_string_field(2, value)


def remote_write_v1_simple() -> bytes:
    """The `WriteRequest` //crates/metrics/src/wire/v1 decodes in its tests."""
    sample = pb_double_field(1, 1.0) + pb_int64_field(2, 1000)
    exemplar = (
        pb_bytes_field(1, pb_label("trace_id", "abc"))
        + pb_double_field(2, 2.0)
        + pb_int64_field(3, 1100)
    )
    series = (
        pb_bytes_field(1, pb_label("__name__", "up"))
        + pb_bytes_field(2, sample)
        + pb_bytes_field(3, exemplar)
    )
    return snappy_literal_block(pb_bytes_field(1, series))


def remote_write_v1_histogram() -> bytes:
    """A native histogram: two spans, three deltas, a schema and a reset hint.

    The span-length and delta-run agreement is what `validate_spans_and_counts`
    checks, and no other seed carries a shape that reaches it.
    """
    span = pb_sint32_field(1, 0) + pb_varint_field(2, 2)
    second_span = pb_sint32_field(1, 3) + pb_varint_field(2, 1)
    histogram = (
        pb_varint_field(1, 12)
        + pb_double_field(3, 34.5)
        + pb_sint32_field(4, 3)
        + pb_double_field(5, 0.001)
        + pb_varint_field(6, 2)
        + pb_bytes_field(11, span)
        + pb_bytes_field(11, second_span)
        + pb_sint64_field(12, 4)
        + pb_sint64_field(12, -2)
        + pb_sint64_field(12, 1)
        + pb_varint_field(14, 2)
        + pb_int64_field(15, 1200)
    )
    series = pb_bytes_field(1, pb_label("__name__", "latency")) + pb_bytes_field(4, histogram)
    return snappy_literal_block(pb_bytes_field(1, series))


def remote_write_v1_metadata() -> bytes:
    """A metadata-only body, which takes the second loop in `decode_v1`."""
    metadata = (
        pb_varint_field(1, 1)
        + pb_string_field(2, "http_requests_total")
        + pb_string_field(4, "total requests")
        + pb_string_field(5, "requests")
    )
    return snappy_literal_block(pb_bytes_field(3, metadata))


def remote_write_v2_simple() -> bytes:
    """A v2 request: a symbol table, and label references into it."""
    symbols = b"".join(
        pb_string_field(4, symbol)
        for symbol in ["", "__name__", "up", "job", "api", "requests", "seconds"]
    )
    sample = pb_double_field(1, 3.0) + pb_int64_field(2, 2000)
    metadata = pb_varint_field(1, 1) + pb_varint_field(3, 5) + pb_varint_field(4, 6)
    series = (
        pb_varint_field(1, 1)
        + pb_varint_field(1, 2)
        + pb_varint_field(1, 3)
        + pb_varint_field(1, 4)
        + pb_bytes_field(2, sample)
        + pb_bytes_field(5, metadata)
    )
    return snappy_literal_block(symbols + pb_bytes_field(5, series))


def remote_read_request() -> bytes:
    """A read request with one query, one matcher and a hint block."""
    matcher = pb_varint_field(1, 0) + pb_string_field(2, "__name__") + pb_string_field(3, "up")
    hints = pb_int64_field(1, 30_000) + pb_string_field(2, "rate") + pb_int64_field(7, 300_000)
    query = (
        pb_int64_field(1, 1_000)
        + pb_int64_field(2, 2_000)
        + pb_bytes_field(3, matcher)
        + pb_bytes_field(4, hints)
    )
    return snappy_literal_block(pb_bytes_field(1, query) + pb_varint_field(2, 1))


# ------------------------------------------------------------------ pprof


def pprof_profile() -> bytes:
    """A minimal perftools.profiles profile: one sample type, one sample.

    Field numbers come from the vendored `profile.proto` under
    //crates/pprof/proto.
    """
    value_type = pb_int64_field(1, 1) + pb_int64_field(2, 2)
    sample = pb_varint_field(1, 1) + pb_int64_field(2, 7)
    location = pb_varint_field(1, 1) + pb_varint_field(3, 0x1000)
    strings = b"".join(pb_string_field(6, name) for name in ["", "samples", "count"])
    return (
        pb_bytes_field(1, value_type)
        + pb_bytes_field(2, sample)
        + pb_bytes_field(4, location)
        + strings
        + pb_int64_field(9, 0)
        + pb_int64_field(10, 1_000_000_000)
    )


# ------------------------------------------------------------ text mining

EVAL = re.compile(
    r"^\s*eval(?:_(?:fail|warn|info|ordered))?"
    r"(?:\s+(?:instant|range))?"
    r"(?:\s+from\s+\S+\s+to\s+\S+\s+step\s+\S+)?"
    r"(?:\s+at\s+\S+)?\s+(?P<query>\S.*?)\s*$"
)
CASE_QUERY = re.compile(r"^query:\s*(?P<query>\S.*?)\s*$")
JSON_ARRAY_LITERAL = re.compile(r'r#"(\s*\[.*?\])"#', re.DOTALL)

# Rust string literals, raw and ordinary. Both forms are scanned, because a
# LogQL query with a quoted label value is written either way depending on how
# much escaping the author wanted.
RUST_RAW_LITERAL = re.compile(r'r(#*)"(?P<body>.*?)"\1', re.DOTALL)
RUST_LITERAL = re.compile(r'"(?P<body>(?:[^"\\\n]|\\.)*)"')

# A LogQL stream selector: a label name, a matcher operator, and a quoted
# value, all inside one pair of braces. Requiring the quoted value is what
# separates a selector from a Rust format string such as `{op:?}` or `{}`.
LOGQL_SELECTOR = re.compile(r'\{[^{}]*[A-Za-z_][A-Za-z0-9_]*\s*(?:=~|!~|!=|=)\s*"')

RUST_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "0": "\0", '"': '"', "\\": "\\", "'": "'"}


def unescape_rust(body: str) -> str:
    """Undo the escapes an ordinary Rust string literal carries.

    An escape this table does not name is left as it was written. Nothing here
    depends on reading every escape correctly: an entry that comes out wrong is
    one seed that says something slightly different from its source.
    """
    out = []
    index = 0
    while index < len(body):
        char = body[index]
        if char == "\\" and index + 1 < len(body):
            out.append(RUST_ESCAPES.get(body[index + 1], body[index + 1]))
            index += 2
        else:
            out.append(char)
            index += 1
    return "".join(out)


def promql_queries() -> list[str]:
    out = []
    for path in sorted((REPO / "crates" / "promql" / "tests" / "testdata").glob("*.test")):
        for line in path.read_text().splitlines():
            match = EVAL.match(line)
            if match:
                out.append(match.group("query"))
    return out


def traceql_queries() -> list[str]:
    root = REPO / "crates" / "traceql" / "tests" / "testdata" / "traceql"
    out = []
    for path in sorted(root.glob("*.case")):
        for line in path.read_text().splitlines():
            match = CASE_QUERY.match(line)
            if match:
                out.append(match.group("query"))
    return out


def logql_queries() -> list[str]:
    """LogQL strings the crates already drive their parsers with.

    LogQL has no vendored conformance corpus the way PromQL and TraceQL do, so
    the source is the query strings in the LogQL and observability sources. A
    stream selector is the one shape a LogQL query cannot be without, and that
    is the mining rule. It over-collects a little, because a PromQL selector
    reads the same, and an over-collected seed costs one file.
    """
    out = []
    roots = [REPO / "crates" / "logql" / "src", REPO / "crates" / "observability" / "src"]
    for root in roots:
        for path in sorted(root.rglob("*.rs")):
            source = path.read_text()
            candidates = [match.group("body") for match in RUST_RAW_LITERAL.finditer(source)]
            candidates += [
                unescape_rust(match.group("body")) for match in RUST_LITERAL.finditer(source)
            ]
            for candidate in candidates:
                if len(candidate) <= 200 and LOGQL_SELECTOR.search(candidate):
                    out.append(candidate)
    return out


def zipkin_bodies() -> list[str]:
    """The Zipkin JSON bodies //crates/traces/src/wire/zipkin decodes in tests."""
    source = (REPO / "crates" / "traces" / "src" / "wire" / "zipkin.rs").read_text()
    return [match.group(1) for match in JSON_ARRAY_LITERAL.finditer(source)]


# ------------------------------------------------------------------ output


def thin(items: list[str]) -> list[str]:
    """Deduplicate, then take an even stride so the pick spreads over the source."""
    unique = sorted(set(items))
    if len(unique) <= MAX_SEEDS:
        return unique
    stride = len(unique) / MAX_SEEDS
    return [unique[int(index * stride)] for index in range(MAX_SEEDS)]


def corpora() -> dict[str, list[bytes]]:
    """Every target's seeds. A target absent here starts from an empty corpus."""
    return {
        "jaeger_compact_thrift": [
            jaeger_compact_batch(),
            jaeger_compact_empty_batch(),
            jaeger_compact_skipped_fields(),
        ],
        "jaeger_binary_thrift": [jaeger_binary_batch()],
        "zipkin_json": [body.encode() for body in zipkin_bodies()],
        "pprof_profile": [pprof_profile()],
        "remote_write_v1": [
            remote_write_v1_simple(),
            remote_write_v1_histogram(),
            remote_write_v1_metadata(),
        ],
        "remote_write_v2": [remote_write_v2_simple()],
        "remote_read": [remote_read_request()],
        "promql_parse": [query.encode() for query in thin(promql_queries())],
        "logql_parse": [query.encode() for query in thin(logql_queries())],
        "traceql_parse": [query.encode() for query in thin(traceql_queries())],
    }


def write(root: Path) -> None:
    """Write every corpus under `root`, one file per seed named by its digest.

    libFuzzer names a corpus file after the SHA-1 of its contents, so using the
    same rule keeps a regenerated corpus byte-identical to the checked-in one
    whenever the seeds themselves have not moved, and keeps a seed the fuzzer
    adds indistinguishable from one this script wrote.
    """
    for target, seeds in corpora().items():
        directory = root / target
        directory.mkdir(parents=True, exist_ok=True)
        if not seeds:
            raise SystemExit(f"{target}: no seeds; a corpus that mines nothing is a bug")
        for seed in seeds:
            (directory / hashlib.sha1(seed).hexdigest()).write_bytes(seed)


TARGET_BIN = re.compile(r'^path = "fuzz_targets/(?P<name>[A-Za-z0-9_]+)\.rs"$', re.MULTILINE)


def declared_targets() -> set[str]:
    """The fuzz targets //fuzz/Cargo.toml declares, by `[[bin]]` path."""
    manifest = (REPO / "fuzz" / "Cargo.toml").read_text()
    return {match.group("name") for match in TARGET_BIN.finditer(manifest)}


def check_targets() -> list[str]:
    """Check that the target files, the manifest and the corpus agree.

    cargo-fuzz builds what the manifest declares, so a target file with no
    `[[bin]]` entry is a file nothing compiles and nothing runs. It reads as
    covered surface and is not, which is the failure this whole directory
    exists to stop happening elsewhere.
    """
    problems = []
    declared = declared_targets()
    on_disk = {path.stem for path in (REPO / "fuzz" / "fuzz_targets").glob("*.rs")}

    for name in sorted(on_disk - declared):
        problems.append(f"fuzz/fuzz_targets/{name}.rs has no [[bin]] entry in fuzz/Cargo.toml")
    for name in sorted(declared - on_disk):
        problems.append(f"fuzz/Cargo.toml declares {name}, but fuzz_targets/{name}.rs is missing")
    for name in sorted(set(corpora()) - declared):
        problems.append(f"tools/fuzz-corpus.py seeds {name}, which is not a fuzz target")
    if CORPUS.exists():
        for directory in sorted(CORPUS.iterdir()):
            if directory.is_dir() and directory.name not in declared:
                problems.append(f"fuzz/corpus/{directory.name} is not a fuzz target")
    return problems


def check() -> int:
    problems = check_targets()
    with tempfile.TemporaryDirectory() as scratch:
        fresh = Path(scratch) / "corpus"
        write(fresh)
        want = {
            path.relative_to(fresh): path.read_bytes() for path in sorted(fresh.rglob("*")) if path.is_file()
        }
    have = {
        path.relative_to(CORPUS): path.read_bytes()
        for path in sorted(CORPUS.rglob("*"))
        if path.is_file()
    }
    # This script owns every file under fuzz/corpus, so the comparison is exact
    # in both directions. A seed a run found and a maintainer wants to keep is
    # added to this script rather than dropped into the directory, which is
    # what gets it a name, a reason, and a review.
    problems += [
        f"missing seed: fuzz/corpus/{name}" for name in sorted(str(n) for n in want.keys() - have.keys())
    ]
    problems += [
        f"unknown seed: fuzz/corpus/{name}" for name in sorted(str(n) for n in have.keys() - want.keys())
    ]
    if not problems:
        print(f"fuzz corpus up to date: {len(want)} generated seeds, {len(declared_targets())} targets")
        return 0
    for problem in problems:
        print(problem)
    print("run tools/fuzz-corpus.py to rebuild the corpus")
    return 1


def self_test() -> int:
    """Check the encoders against encodings the formats fix, not this file.

    Nothing here reads the repository, so it runs on a bare checkout in seconds.
    """
    failures = []

    def expect(name: str, got: object, want: object) -> None:
        if got != want:
            failures.append(f"{name}: got {got!r}, want {want!r}")

    expect("varint(0)", varint(0), b"\x00")
    expect("varint(300)", varint(300), b"\xac\x02")
    expect("varint(127)", varint(127), b"\x7f")
    expect("varint(128)", varint(128), b"\x80\x01")

    expect("zigzag(0)", zigzag(0, 32), 0)
    expect("zigzag(-1)", zigzag(-1, 32), 1)
    expect("zigzag(1)", zigzag(1, 32), 2)
    expect("zigzag(-2)", zigzag(-2, 64), 3)
    expect("zigzag(i64::MIN)", zigzag(-(1 << 63), 64), (1 << 64) - 1)

    # A snappy block starts with the uncompressed length and then a literal tag
    # whose high six bits hold `len - 1`.
    expect("snappy(b'')", snappy_literal_block(b""), b"\x00")
    expect("snappy(b'ab')", snappy_literal_block(b"ab"), b"\x02\x04ab")
    long_payload = b"x" * 200
    block = snappy_literal_block(long_payload)
    expect("snappy long length prefix", block[:2], varint(200))
    expect("snappy long tag", block[2], (59 + 1) << 2)
    expect("snappy long extra length byte", block[3], 199)
    expect("snappy long payload", block[4:], long_payload)

    # proto3 field tags are `(number << 3) | wire_type`.
    expect("pb varint field", pb_varint_field(1, 1), b"\x08\x01")
    expect("pb string field", pb_string_field(2, "up"), b"\x12\x02up")
    expect("pb double field", pb_double_field(1, 1.0), b"\x09" + struct.pack("<d", 1.0))
    expect("pb negative int64", pb_int64_field(2, -1), b"\x10" + b"\xff" * 9 + b"\x01")

    # A compact field header holds the delta in the high nibble when it fits.
    writer = Compact()
    last = [0]
    writer.field(Compact.I64, 1, last)
    writer.field(Compact.I64, 3, last)
    writer.field(Compact.I64, 1, last)
    expect("compact short then long form", bytes(writer.out), b"\x16\x26\x06\x02")

    # Every generated binary seed has to be non-empty and stable.
    for name, builder in [
        ("jaeger_compact_batch", jaeger_compact_batch),
        ("jaeger_compact_skipped_fields", jaeger_compact_skipped_fields),
        ("jaeger_binary_batch", jaeger_binary_batch),
        ("remote_write_v1_simple", remote_write_v1_simple),
        ("remote_write_v1_histogram", remote_write_v1_histogram),
        ("remote_write_v1_metadata", remote_write_v1_metadata),
        ("remote_write_v2_simple", remote_write_v2_simple),
        ("remote_read_request", remote_read_request),
        ("pprof_profile", pprof_profile),
    ]:
        first, second = builder(), builder()
        if not first:
            failures.append(f"{name}: encoded to nothing")
        if first != second:
            failures.append(f"{name}: not deterministic")

    for failure in failures:
        print(f"self-test failure: {failure}")
    if failures:
        return 1
    print("fuzz-corpus self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the corpus is out of date")
    parser.add_argument("--self-test", action="store_true", help="check the encoders and exit")
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    if args.check:
        return check()

    if CORPUS.exists():
        for target in corpora():
            shutil.rmtree(CORPUS / target, ignore_errors=True)
    write(CORPUS)
    total = sum(len(seeds) for seeds in corpora().values())
    print(f"wrote {total} seeds under {CORPUS.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
