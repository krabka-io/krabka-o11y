use std::collections::BTreeMap;

use assert2::{assert, check};
use krabka_blockstore::Labels;
use krabka_metrics::{
    DecodedTsdbBlock, NativeHistogram, TsdbBlockFiles, TsdbBlockMeta, TsdbImportError,
    TsdbImportLimits, TsdbImportStats, decode_tsdb_block,
};
use serde_json::{Value, json};
use tsdb_fixture::{
    FIXTURE_MAX_TIME, FIXTURE_MIN_TIME, FixtureFiles, expected_samples, fixture_files,
};
use tsdb_writer::{SyntheticChunk, SyntheticSeries, write_block, write_tombstones};

#[path = "support/tsdb_fixture.rs"]
mod tsdb_fixture;
#[path = "support/tsdb_writer.rs"]
mod tsdb_writer;

const TENANT: &str = "tenant-a";
const STALE_NAN: u64 = 0x7ff0_0000_0000_0002;

fn fixture_meta() -> TsdbBlockMeta {
    let meta: Value = serde_json::from_slice(&fixture_files().meta).expect("meta.json is JSON");
    TsdbBlockMeta {
        min_time: meta["minTime"].as_i64().expect("minTime is an integer"),
        max_time: meta["maxTime"].as_i64().expect("maxTime is an integer"),
        external_labels: BTreeMap::new(),
    }
}

#[test]
fn fixture_meta_names_the_fixture_range() {
    let meta = fixture_meta();

    check!((meta.min_time, meta.max_time) == (FIXTURE_MIN_TIME, FIXTURE_MAX_TIME));
}

fn decode_fixture(
    files: &FixtureFiles,
    meta: &TsdbBlockMeta,
    limits: &TsdbImportLimits,
) -> Result<DecodedTsdbBlock, TsdbImportError> {
    let segments = [files.chunks.as_slice()];
    decode_tsdb_block(
        TENANT,
        meta,
        TsdbBlockFiles {
            index: &files.index,
            chunk_segments: &segments,
            tombstones: Some(&files.tombstones),
        },
        limits,
    )
}

fn bits(value: f64) -> String {
    format!("{:016x}", value.to_bits())
}

fn spans(spans: &[krabka_metrics::BucketSpan]) -> Value {
    spans
        .iter()
        .map(|span| json!({"offset": span.offset, "length": span.length}))
        .collect()
}

fn histogram_json(timestamp: i64, hist: &NativeHistogram) -> Value {
    json!({
        "t": timestamp,
        "is_float": hist.is_float,
        "reset_hint": hist.reset_hint.as_i8(),
        "schema": hist.schema,
        "zero_threshold": bits(hist.zero_threshold),
        "zero_count": bits(hist.zero_count),
        "count": bits(hist.count),
        "sum": bits(hist.sum),
        "positive_spans": spans(&hist.positive_spans),
        "positive_counts": hist.positive_counts.iter().copied().map(bits).collect::<Vec<_>>(),
        "negative_spans": spans(&hist.negative_spans),
        "negative_counts": hist.negative_counts.iter().copied().map(bits).collect::<Vec<_>>(),
        "custom_values": hist.custom_values.iter().flatten().copied().map(bits).collect::<Vec<_>>(),
    })
}

/// The decoded rows in the reference dump's shape: one entry per series in
/// label order, with every `f64` as hex bits.
fn decoded_as_dump(block: &DecodedTsdbBlock) -> Value {
    let rows = &block.rows;
    let mut by_labels = rows
        .series_labels
        .iter()
        .map(|(fingerprint, labels)| {
            let labels = labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>();
            let floats = rows
                .float_rows
                .iter()
                .filter(|row| row.fingerprint == *fingerprint)
                .map(|row| json!({"t": row.timestamp_ms, "v": bits(row.value)}))
                .collect::<Vec<_>>();
            let histograms = rows
                .histogram_rows
                .iter()
                .filter(|row| row.fingerprint == *fingerprint)
                .map(|row| histogram_json(row.timestamp_ms, &row.hist))
                .collect::<Vec<_>>();
            (
                labels.clone(),
                json!({"labels": labels, "floats": floats, "histograms": histograms}),
            )
        })
        .collect::<Vec<_>>();
    by_labels.sort_by(|left, right| left.0.cmp(&right.0));
    by_labels.into_iter().map(|(_, series)| series).collect()
}

/// The reference dump with each histogram stale marker moved to the float
/// samples of its series, which is how the import stores it.
fn expected_as_imported() -> Value {
    let mut dump = expected_samples();
    for series in dump.as_array_mut().expect("the dump is an array") {
        let histograms = series["histograms"]
            .as_array()
            .expect("histograms are an array")
            .clone();
        let (stale, live): (Vec<Value>, Vec<Value>) = histograms
            .into_iter()
            .partition(|hist| hist["sum"] == json!(format!("{STALE_NAN:016x}")));
        let floats = series["floats"]
            .as_array_mut()
            .expect("floats are an array");
        floats.extend(
            stale
                .iter()
                .map(|hist| json!({"t": hist["t"], "v": hist["sum"]})),
        );
        floats.sort_by_key(|sample| sample["t"].as_i64());
        series["histograms"] = Value::Array(live);
    }
    dump
}

#[test]
fn fixture_block_decodes_to_the_samples_prometheus_reads() {
    let files = fixture_files();

    let block = decode_fixture(&files, &fixture_meta(), &TsdbImportLimits::default())
        .expect("the fixture block decodes");

    check!(
        block.stats
            == TsdbImportStats {
                series: 7,
                chunks: 35,
                float_samples: 3 * 480 + 419 + 2,
                histogram_samples: 3 * 480 - 2,
                stale_histogram_markers: 2,
                deleted_samples: 61,
            }
    );
    check!(block.rows.tenant == TENANT);
    check!(block.rows.exemplar_rows.is_empty());
    check!(block.rows.metadata_rows.is_empty());
    check!(block.rows.clock_rows.is_empty());
    assert!(decoded_as_dump(&block) == expected_as_imported());
}

#[test]
fn fixture_series_carry_the_krabka_fingerprint_of_their_labels() {
    let files = fixture_files();

    let block = decode_fixture(&files, &fixture_meta(), &TsdbImportLimits::default())
        .expect("the fixture block decodes");

    let expected = tsdb_fixture::expected_series_labels()
        .into_iter()
        .map(|labels| {
            let labels = labels.into_iter().collect::<Labels>();
            (labels.fingerprint(), labels)
        })
        .collect::<BTreeMap<_, _>>();
    assert!(block.rows.series_labels == expected);
}

#[test]
fn fixture_block_is_accepted_under_a_matching_tenant_label() {
    let files = fixture_files();
    let meta = TsdbBlockMeta {
        external_labels: BTreeMap::from([("__tenant_id__".to_owned(), TENANT.to_owned())]),
        ..fixture_meta()
    };

    let block = decode_fixture(&files, &meta, &TsdbImportLimits::default());

    assert!(block.is_ok());
}

/// Rewrites the chunk at the first reference of the segment with a new
/// encoding byte, and recomputes the chunk CRC so that only the encoding is
/// wrong.
fn with_chunk_encoding(chunks: &[u8], encoding: u8) -> Vec<u8> {
    let mut chunks = chunks.to_vec();
    let mut length = 0_usize;
    let mut position = 8;
    let mut shift = 0;
    loop {
        let byte = chunks[position];
        length |= usize::from(byte & 0x7f) << shift;
        position += 1;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
    }
    chunks[position] = encoding;
    let crc = crc32c::crc32c(&chunks[position..=position + length]);
    chunks[position + length + 1..position + length + 5].copy_from_slice(&crc.to_be_bytes());
    chunks
}

fn flip(mut bytes: Vec<u8>, position: usize) -> Vec<u8> {
    bytes[position] ^= 0x01;
    bytes
}

struct CorruptionCase {
    name: &'static str,
    corrupt: fn(&mut FixtureFiles, &mut TsdbBlockMeta, &mut TsdbImportLimits),
    expected: TsdbImportError,
}

#[test]
fn corrupt_fixture_blocks_fail_with_a_specific_error() {
    let cases = [
        CorruptionCase {
            name: "flipped TOC checksum",
            corrupt: |files, _, _| {
                let last = files.index.len() - 1;
                files.index = flip(std::mem::take(&mut files.index), last);
            },
            expected: TsdbImportError::Checksum {
                section: "index TOC".to_owned(),
            },
        },
        CorruptionCase {
            name: "flipped symbol byte",
            corrupt: |files, _, _| files.index = flip(std::mem::take(&mut files.index), 14),
            expected: TsdbImportError::Checksum {
                section: "index symbols".to_owned(),
            },
        },
        CorruptionCase {
            name: "truncated index",
            corrupt: |files, _, _| files.index.truncate(4),
            expected: TsdbImportError::Truncated {
                section: "index header",
            },
        },
        CorruptionCase {
            name: "index format version 1",
            corrupt: |files, _, _| files.index[4] = 1,
            expected: TsdbImportError::UnsupportedIndexVersion(1),
        },
        CorruptionCase {
            name: "flipped chunk byte",
            corrupt: |files, _, _| files.chunks = flip(std::mem::take(&mut files.chunks), 12),
            expected: TsdbImportError::Checksum {
                section: "chunk at reference 0x8".to_owned(),
            },
        },
        CorruptionCase {
            name: "truncated chunk segment",
            corrupt: |files, _, _| {
                let half = files.chunks.len() / 2;
                files.chunks.truncate(half);
            },
            expected: TsdbImportError::Truncated { section: "chunk" },
        },
        CorruptionCase {
            name: "unknown chunk encoding",
            corrupt: |files, _, _| files.chunks = with_chunk_encoding(&files.chunks, 9),
            expected: TsdbImportError::UnknownChunkEncoding {
                reference: 8,
                encoding: 9,
            },
        },
        CorruptionCase {
            name: "chunk segment format 2",
            corrupt: |files, _, _| files.chunks[4] = 2,
            expected: TsdbImportError::UnsupportedChunkFormat {
                segment: 0,
                version: 2,
            },
        },
        CorruptionCase {
            name: "flipped tombstones checksum",
            corrupt: |files, _, _| {
                let last = files.tombstones.len() - 1;
                files.tombstones = flip(std::mem::take(&mut files.tombstones), last);
            },
            expected: TsdbImportError::Checksum {
                section: "tombstones".to_owned(),
            },
        },
        CorruptionCase {
            name: "samples after the block end",
            corrupt: |_, meta, _| meta.max_time = FIXTURE_MIN_TIME + 3_600_000,
            expected: TsdbImportError::OutOfBounds {
                labels: r#"{__name__="imported_counter_total", instance="i1", job="fixture"}"#
                    .to_owned(),
                timestamp: 1_749_999_600_000,
                min_time: FIXTURE_MIN_TIME,
                max_time: FIXTURE_MIN_TIME + 3_600_000,
            },
        },
        CorruptionCase {
            name: "another tenant's block",
            corrupt: |_, meta, _| {
                meta.external_labels
                    .insert("__tenant_id__".to_owned(), "tenant-b".to_owned());
            },
            expected: TsdbImportError::TenantMismatch {
                label: "__tenant_id__".to_owned(),
                expected: TENANT.to_owned(),
                found: "tenant-b".to_owned(),
            },
        },
        CorruptionCase {
            name: "sample limit",
            corrupt: |_, _, limits| limits.max_samples = 1_000,
            expected: TsdbImportError::LimitExceeded {
                limit: "samples",
                value: 1_029,
                max: 1_000,
            },
        },
        CorruptionCase {
            name: "block size limit",
            corrupt: |_, _, limits| limits.max_block_bytes = 1_000,
            expected: TsdbImportError::LimitExceeded {
                limit: "block bytes",
                value: 1_074 + 23_047 + 22,
                max: 1_000,
            },
        },
        CorruptionCase {
            name: "block-wide chunk limit",
            corrupt: |_, _, limits| limits.max_chunks = 34,
            expected: TsdbImportError::LimitExceeded {
                limit: "chunks",
                value: 35,
                max: 34,
            },
        },
        CorruptionCase {
            name: "series limit",
            corrupt: |_, _, limits| limits.max_series = 6,
            expected: TsdbImportError::LimitExceeded {
                limit: "series",
                value: 7,
                max: 6,
            },
        },
    ];

    for case in cases {
        let mut files = fixture_files();
        let mut meta = fixture_meta();
        let mut limits = TsdbImportLimits::default();
        (case.corrupt)(&mut files, &mut meta, &mut limits);

        let result = decode_fixture(&files, &meta, &limits);

        check!(result.err() == Some(case.expected), "case: {}", case.name);
    }
}

fn series(
    labels: Vec<(&'static str, &'static str)>,
    chunks: Vec<SyntheticChunk>,
) -> SyntheticSeries {
    SyntheticSeries { labels, chunks }
}

fn chunk(segment: usize, samples: &[(i64, f64)]) -> SyntheticChunk {
    SyntheticChunk {
        segment,
        samples: samples.to_vec(),
    }
}

fn decode_synthetic(
    block: &tsdb_writer::SyntheticBlock,
    tombstones: Option<&[u8]>,
) -> Result<DecodedTsdbBlock, TsdbImportError> {
    let segments = block.segments.iter().map(Vec::as_slice).collect::<Vec<_>>();
    decode_tsdb_block(
        TENANT,
        &TsdbBlockMeta {
            min_time: 0,
            max_time: 1_000,
            external_labels: BTreeMap::new(),
        },
        TsdbBlockFiles {
            index: &block.index,
            chunk_segments: &segments,
            tombstones,
        },
        &TsdbImportLimits::default(),
    )
}

fn float_rows(block: &DecodedTsdbBlock) -> Vec<(u64, i64, f64)> {
    block
        .rows
        .float_rows
        .iter()
        .map(|row| (row.fingerprint, row.timestamp_ms, row.value))
        .collect()
}

fn fingerprint(labels: &[(&str, &str)]) -> u64 {
    Labels::from_pairs(labels.iter().copied()).fingerprint()
}

#[test]
fn chunks_in_several_segments_decode_in_reference_order() {
    let block = write_block(
        &[series(
            vec![("__name__", "up")],
            vec![
                chunk(0, &[(10, 1.0), (20, 2.5)]),
                chunk(1, &[(30, -3.0), (45, 4.0)]),
            ],
        )],
        2,
    );

    let decoded = decode_synthetic(&block, None).expect("the block decodes");

    let up = fingerprint(&[("__name__", "up")]);
    assert!(
        float_rows(&decoded) == vec![(up, 10, 1.0), (up, 20, 2.5), (up, 30, -3.0), (up, 45, 4.0)]
    );
}

#[test]
fn tombstones_drop_the_deleted_samples_inclusively() {
    let block = write_block(
        &[
            series(
                vec![("__name__", "a")],
                vec![chunk(0, &[(10, 1.0), (20, 2.0), (30, 3.0), (40, 4.0)])],
            ),
            series(
                vec![("__name__", "b")],
                vec![chunk(0, &[(10, 5.0), (20, 6.0)])],
            ),
        ],
        1,
    );
    // The first series entry sits at the first 16-byte boundary after the
    // symbol table.
    let first = 2;
    let tombstones = write_tombstones(&[(first, 20, 30)]);

    let decoded = decode_synthetic(&block, Some(&tombstones)).expect("the block decodes");

    let a = fingerprint(&[("__name__", "a")]);
    let b = fingerprint(&[("__name__", "b")]);
    let mut expected = vec![(a, 10, 1.0), (a, 40, 4.0), (b, 10, 5.0), (b, 20, 6.0)];
    expected.sort_by_key(|(fingerprint, timestamp, _)| (*fingerprint, *timestamp));
    check!(float_rows(&decoded) == expected);
    check!(decoded.stats.deleted_samples == 2);
}

/// A name, the series to write, an optional tombstones file, and the error.
type SyntheticCase = (
    &'static str,
    Vec<SyntheticSeries>,
    Option<Vec<u8>>,
    TsdbImportError,
);

#[test]
fn malformed_synthetic_blocks_fail_with_a_specific_error() {
    let cases: [SyntheticCase; 6] = [
        (
            "samples out of order within a chunk",
            vec![series(
                vec![("__name__", "a")],
                vec![chunk(0, &[(10, 1.0), (30, 2.0), (20, 3.0)])],
            )],
            None,
            TsdbImportError::SampleOrder {
                labels: r#"{__name__="a"}"#.to_owned(),
                previous: 30,
                timestamp: 20,
            },
        ),
        (
            "duplicate series",
            vec![
                series(vec![("__name__", "a")], vec![chunk(0, &[(10, 1.0)])]),
                series(vec![("__name__", "a")], vec![chunk(0, &[(20, 1.0)])]),
            ],
            None,
            TsdbImportError::SeriesOrder {
                labels: r#"{__name__="a"}"#.to_owned(),
            },
        ),
        (
            "series out of order",
            vec![
                series(vec![("__name__", "b")], vec![chunk(0, &[(10, 1.0)])]),
                series(vec![("__name__", "a")], vec![chunk(0, &[(20, 1.0)])]),
            ],
            None,
            TsdbImportError::SeriesOrder {
                labels: r#"{__name__="a"}"#.to_owned(),
            },
        ),
        (
            "labels out of order",
            vec![series(
                vec![("job", "x"), ("__name__", "a")],
                vec![chunk(0, &[(10, 1.0)])],
            )],
            None,
            TsdbImportError::InvalidLabels(
                r#"series 3 has label "__name__" out of order or repeated"#.to_owned(),
            ),
        ),
        (
            "sample past the block end",
            vec![series(
                vec![("__name__", "a")],
                vec![chunk(0, &[(10, 1.0), (1_000, 2.0)])],
            )],
            None,
            TsdbImportError::OutOfBounds {
                labels: r#"{__name__="a"}"#.to_owned(),
                timestamp: 1_000,
                min_time: 0,
                max_time: 1_000,
            },
        ),
        (
            "tombstone for a series the index lacks",
            vec![series(
                vec![("__name__", "a")],
                vec![chunk(0, &[(10, 1.0)])],
            )],
            Some(write_tombstones(&[(99, 0, 5)])),
            TsdbImportError::InvalidIndex(
                "tombstones name series reference 0x63, which the index does not hold".to_owned(),
            ),
        ),
    ];

    for (name, series, tombstones, expected) in cases {
        let block = write_block(&series, 1);

        let result = decode_synthetic(&block, tombstones.as_deref());

        check!(result.err() == Some(expected), "case: {name}");
    }
}

#[test]
fn a_block_whose_samples_are_all_deleted_is_empty() {
    let block = write_block(
        &[series(
            vec![("__name__", "a")],
            vec![chunk(0, &[(10, 1.0), (20, 2.0)])],
        )],
        1,
    );
    let tombstones = write_tombstones(&[(2, 0, 999)]);

    let result = decode_synthetic(&block, Some(&tombstones));

    assert!(result.err() == Some(TsdbImportError::Empty));
}
