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
use tsdb_writer::{
    LabelBytes, SyntheticChunk, SyntheticSample, SyntheticSeries, TombstoneRecord, write_block,
    write_tombstones,
};

#[path = "support/tsdb_fixture.rs"]
mod tsdb_fixture;
#[path = "support/tsdb_writer.rs"]
mod tsdb_writer;

const TENANT: &str = "tenant-a";
const STALE_NAN: u64 = 0x7ff0_0000_0000_0002;

#[derive(Clone, Copy)]
struct LabelInput {
    name: &'static str,
    value: &'static str,
}

const fn label(name: &'static str, value: &'static str) -> LabelInput {
    LabelInput { name, value }
}

const fn sample(timestamp_ms: i64, value: f64) -> SyntheticSample {
    SyntheticSample::new(timestamp_ms, value)
}

struct PartitionedMarkers {
    stale: Vec<Value>,
    live: Vec<Value>,
}

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
    struct LabelSeriesDump {
        labels: BTreeMap<String, String>,
        series: Value,
    }
    let rows = &block.rows;
    let mut by_labels = rows
        .series_labels
        .iter()
        .map(|series| {
            let labels = series.1.iter();
            let mut label_map = BTreeMap::new();
            for label in labels {
                label_map.insert(label.name.clone(), label.value.as_str().to_owned());
            }
            let floats = rows
                .float_rows
                .iter()
                .filter(|row| row.fingerprint == *series.0)
                .map(|row| json!({"t": row.timestamp_ms, "v": bits(row.value)}))
                .collect::<Vec<_>>();
            let histograms = rows
                .histogram_rows
                .iter()
                .filter(|row| row.fingerprint == *series.0)
                .map(|row| histogram_json(row.timestamp_ms, &row.hist))
                .collect::<Vec<_>>();
            LabelSeriesDump {
                labels: label_map.clone(),
                series: json!({"labels": label_map, "floats": floats, "histograms": histograms}),
            }
        })
        .collect::<Vec<_>>();
    by_labels.sort_by(|left, right| left.labels.cmp(&right.labels));
    by_labels.into_iter().map(|entry| entry.series).collect()
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
        let markers = histograms.into_iter().fold(
            PartitionedMarkers {
                stale: Vec::new(),
                live: Vec::new(),
            },
            |mut markers, histogram| {
                if histogram["sum"] == json!(format!("{STALE_NAN:016x}")) {
                    markers.stale.push(histogram);
                } else {
                    markers.live.push(histogram);
                }
                markers
            },
        );
        let floats = series["floats"]
            .as_array_mut()
            .expect("floats are an array");
        floats.extend(
            markers
                .stale
                .iter()
                .map(|hist| json!({"t": hist["t"], "v": hist["sum"]})),
        );
        floats.sort_by_key(|sample| sample["t"].as_i64());
        series["histograms"] = Value::Array(markers.live);
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

    let mut expected = BTreeMap::new();
    for label_set in tsdb_fixture::expected_series_labels() {
        let labels = krabka_metrics::MetricLabels::from_pairs(
            label_set
                .into_iter()
                .map(|pair| krabka_metrics::MetricLabelPair::new(pair.0, pair.1)),
        );
        expected.insert(labels.fingerprint(), labels);
    }
    assert!(block.rows.series_labels == expected);
}

#[test]
fn fixture_block_is_accepted_under_a_matching_tenant_label() {
    let files = fixture_files();
    let mut external_labels = BTreeMap::new();
    external_labels.insert("__tenant_id__".to_owned(), TENANT.to_owned());
    let meta = TsdbBlockMeta {
        external_labels,
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

fn series(labels: Vec<LabelInput>, chunks: Vec<SyntheticChunk>) -> SyntheticSeries {
    SyntheticSeries {
        labels: labels
            .into_iter()
            .map(|label| LabelBytes {
                name: label.name.as_bytes().to_vec(),
                value: label.value.as_bytes().to_vec(),
            })
            .collect(),
        chunks,
    }
}

// A series named `name` with one sample, 1.0 at `timestamp_ms`, in the first
// segment.
fn one_sample_series(name: &'static str, timestamp_ms: i64) -> SyntheticSeries {
    series(
        vec![label("__name__", name)],
        vec![chunk(0, &[sample(timestamp_ms, 1.0)])],
    )
}

fn series_order_error_for_a() -> TsdbImportError {
    TsdbImportError::SeriesOrder {
        labels: r#"{__name__="a"}"#.to_owned(),
    }
}

fn chunk(segment: usize, samples: &[SyntheticSample]) -> SyntheticChunk {
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

fn float_rows(block: &DecodedTsdbBlock) -> Vec<krabka_metrics::FloatRow> {
    block.rows.float_rows.iter().cloned().collect()
}

fn fingerprint(labels: &[LabelInput]) -> u64 {
    Labels::from_pairs(
        labels
            .iter()
            .map(|label| krabka_blockstore::LabelPair::new(label.name, label.value)),
    )
    .fingerprint()
}

#[test]
fn chunks_in_several_segments_decode_in_reference_order() {
    let block = write_block(
        &[series(
            vec![label("__name__", "up")],
            vec![
                chunk(0, &[sample(10, 1.0), sample(20, 2.5)]),
                chunk(1, &[sample(30, -3.0), sample(45, 4.0)]),
            ],
        )],
        2,
    );

    let decoded = decode_synthetic(&block, None).expect("the block decodes");

    let up = fingerprint(&[label("__name__", "up")]);
    assert!(
        float_rows(&decoded)
            == vec![
                krabka_metrics::FloatRow {
                    fingerprint: up,
                    timestamp_ms: 10,
                    value: 1.0,
                    start_timestamp_ms: None,
                },
                krabka_metrics::FloatRow {
                    fingerprint: up,
                    timestamp_ms: 20,
                    value: 2.5,
                    start_timestamp_ms: None,
                },
                krabka_metrics::FloatRow {
                    fingerprint: up,
                    timestamp_ms: 30,
                    value: -3.0,
                    start_timestamp_ms: None,
                },
                krabka_metrics::FloatRow {
                    fingerprint: up,
                    timestamp_ms: 45,
                    value: 4.0,
                    start_timestamp_ms: None,
                },
            ]
    );
}

#[test]
fn tombstones_drop_the_deleted_samples_inclusively() {
    let block = write_block(
        &[
            series(
                vec![label("__name__", "a")],
                vec![chunk(
                    0,
                    &[
                        sample(10, 1.0),
                        sample(20, 2.0),
                        sample(30, 3.0),
                        sample(40, 4.0),
                    ],
                )],
            ),
            series(
                vec![label("__name__", "b")],
                vec![chunk(0, &[sample(10, 5.0), sample(20, 6.0)])],
            ),
        ],
        1,
    );
    // The first series entry sits at the first 16-byte boundary after the
    // symbol table.
    let first = 2;
    let tombstones = write_tombstones(&[TombstoneRecord {
        series_reference: first,
        min_time: 20,
        max_time: 30,
    }]);

    let decoded = decode_synthetic(&block, Some(&tombstones)).expect("the block decodes");

    let a = fingerprint(&[label("__name__", "a")]);
    let b = fingerprint(&[label("__name__", "b")]);
    let mut expected = vec![
        krabka_metrics::FloatRow {
            fingerprint: a,
            timestamp_ms: 10,
            value: 1.0,
            start_timestamp_ms: None,
        },
        krabka_metrics::FloatRow {
            fingerprint: a,
            timestamp_ms: 40,
            value: 4.0,
            start_timestamp_ms: None,
        },
        krabka_metrics::FloatRow {
            fingerprint: b,
            timestamp_ms: 10,
            value: 5.0,
            start_timestamp_ms: None,
        },
        krabka_metrics::FloatRow {
            fingerprint: b,
            timestamp_ms: 20,
            value: 6.0,
            start_timestamp_ms: None,
        },
    ];
    expected.sort_by(|left, right| {
        left.fingerprint
            .cmp(&right.fingerprint)
            .then_with(|| left.timestamp_ms.cmp(&right.timestamp_ms))
    });
    check!(float_rows(&decoded) == expected);
    check!(decoded.stats.deleted_samples == 2);
}

/// A name, the series to write, an optional tombstones file, and the error.
struct SyntheticCase {
    name: &'static str,
    series: Vec<SyntheticSeries>,
    tombstones: Option<Vec<u8>>,
    expected: TsdbImportError,
}

#[test]
fn malformed_synthetic_blocks_fail_with_a_specific_error() {
    let cases: [SyntheticCase; 6] = [
        SyntheticCase {
            name: "samples out of order within a chunk",
            series: vec![series(
                vec![label("__name__", "a")],
                vec![chunk(
                    0,
                    &[sample(10, 1.0), sample(30, 2.0), sample(20, 3.0)],
                )],
            )],
            tombstones: None,
            expected: TsdbImportError::SampleOrder {
                labels: r#"{__name__="a"}"#.to_owned(),
                previous: 30,
                timestamp: 20,
            },
        },
        SyntheticCase {
            name: "duplicate series",
            series: vec![one_sample_series("a", 10), one_sample_series("a", 20)],
            tombstones: None,
            expected: series_order_error_for_a(),
        },
        SyntheticCase {
            name: "series out of order",
            series: vec![one_sample_series("b", 10), one_sample_series("a", 20)],
            tombstones: None,
            expected: series_order_error_for_a(),
        },
        SyntheticCase {
            name: "labels out of order",
            series: vec![series(
                vec![label("job", "x"), label("__name__", "a")],
                vec![chunk(0, &[sample(10, 1.0)])],
            )],
            tombstones: None,
            expected: TsdbImportError::InvalidLabels(
                r#"series 3 has label "__name__" out of order or repeated"#.to_owned(),
            ),
        },
        SyntheticCase {
            name: "sample past the block end",
            series: vec![series(
                vec![label("__name__", "a")],
                vec![chunk(0, &[sample(10, 1.0), sample(1_000, 2.0)])],
            )],
            tombstones: None,
            expected: TsdbImportError::OutOfBounds {
                labels: r#"{__name__="a"}"#.to_owned(),
                timestamp: 1_000,
                min_time: 0,
                max_time: 1_000,
            },
        },
        SyntheticCase {
            name: "tombstone for a series the index lacks",
            series: vec![series(
                vec![label("__name__", "a")],
                vec![chunk(0, &[sample(10, 1.0)])],
            )],
            tombstones: Some(write_tombstones(&[TombstoneRecord {
                series_reference: 99,
                min_time: 0,
                max_time: 5,
            }])),
            expected: TsdbImportError::InvalidIndex(
                "tombstones name series reference 0x63, which the index does not hold".to_owned(),
            ),
        },
    ];

    for case in cases {
        let block = write_block(&case.series, 1);

        let result = decode_synthetic(&block, case.tombstones.as_deref());

        check!(result.err() == Some(case.expected), "case: {}", case.name);
    }
}

#[test]
fn a_block_whose_samples_are_all_deleted_is_empty() {
    let block = write_block(
        &[series(
            vec![label("__name__", "a")],
            vec![chunk(0, &[sample(10, 1.0), sample(20, 2.0)])],
        )],
        1,
    );
    let tombstones = write_tombstones(&[TombstoneRecord {
        series_reference: 2,
        min_time: 0,
        max_time: 999,
    }]);

    let result = decode_synthetic(&block, Some(&tombstones));

    assert!(result.err() == Some(TsdbImportError::Empty));
}

#[test]
fn native_index_values_preserve_bytes_and_validate_names_independently() {
    let values = [
        b"__krabka_bytes_ff".to_vec(),
        "�".as_bytes().to_vec(),
        vec![0xfe],
        vec![0xff],
    ];
    let series = values
        .iter()
        .enumerate()
        .map(|entry| SyntheticSeries {
            labels: vec![
                LabelBytes {
                    name: b"__name__".to_vec(),
                    value: b"byte_input".to_vec(),
                },
                LabelBytes {
                    name: b"raw".to_vec(),
                    value: entry.1.clone(),
                },
            ],
            chunks: vec![chunk(
                0,
                &[sample(10, f64::from(u32::try_from(entry.0).unwrap()) + 1.0)],
            )],
        })
        .collect::<Vec<_>>();
    let block = write_block(&series, 1);
    let decoded = decode_synthetic(&block, None).unwrap();
    assert!(decoded.rows.series_labels.len() == 4 && decoded.rows.float_rows.len() == 4);
    let mut fingerprints = std::collections::BTreeSet::new();
    for entry in values.iter().enumerate() {
        let mut expected =
            krabka_metrics::MetricLabels::from_pairs([krabka_metrics::MetricLabelPair::new(
                "__name__",
                "byte_input",
            )]);
        expected.insert("raw", krabka_metrics::MetricString::from(entry.1.clone()));
        let fingerprint = expected.fingerprint();
        assert!(fingerprints.insert(fingerprint));
        assert!(decoded.rows.series_labels[&fingerprint] == expected);
        let sample = decoded
            .rows
            .float_rows
            .iter()
            .find(|sample| sample.fingerprint == fingerprint)
            .unwrap();
        assert!(
            sample.timestamp_ms == 10
                && sample.value.to_bits()
                    == (f64::from(u32::try_from(entry.0).unwrap()) + 1.0).to_bits()
        );
    }
    let invalid_name = write_block(
        &[SyntheticSeries {
            labels: vec![
                LabelBytes {
                    name: b"__name__".to_vec(),
                    value: b"byte_input".to_vec(),
                },
                LabelBytes {
                    name: vec![0xff],
                    value: b"valid-value".to_vec(),
                },
            ],
            chunks: vec![chunk(0, &[sample(10, 1.0)])],
        }],
        1,
    );
    assert!(
        matches!(decode_synthetic(&invalid_name, None), Err(TsdbImportError::InvalidLabels(reason)) if reason.contains("label name that is not valid UTF-8"))
    );
    let duplicate = write_block(
        &[SyntheticSeries {
            labels: vec![
                LabelBytes {
                    name: b"raw".to_vec(),
                    value: vec![0xff],
                },
                LabelBytes {
                    name: b"raw".to_vec(),
                    value: vec![0xfe],
                },
            ],
            chunks: vec![chunk(0, &[sample(10, 1.0)])],
        }],
        1,
    );
    assert!(
        matches!(decode_synthetic(&duplicate, None), Err(TsdbImportError::InvalidLabels(reason)) if reason.contains("out of order or repeated"))
    );
    let mut bad_checksum = block;
    bad_checksum.index[10] ^= 1;
    assert!(
        matches!(decode_synthetic(&bad_checksum, None), Err(TsdbImportError::Checksum { section }) if section == "index symbols")
    );
}
