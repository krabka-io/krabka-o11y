//! The checked-in Prometheus TSDB block and the samples that the Prometheus
//! TSDB library reads from it. `tests/testdata/tsdb/ATTRIBUTION.md` records
//! how both were produced.

use std::{collections::BTreeMap, io::Read as _, path::PathBuf};

use flate2::read::GzDecoder;
use serde_json::Value;

/// The ULID of the checked-in block.
pub const FIXTURE_ULID: &str = "01M3MJXM7R4M5X4Q4CKHW5Q8N0";
pub const FIXTURE_MIN_TIME: i64 = 1_749_996_000_000;
pub const FIXTURE_MAX_TIME: i64 = 1_750_003_200_000;

/// The raw files of the checked-in block.
pub struct FixtureFiles {
    pub meta: Vec<u8>,
    pub index: Vec<u8>,
    pub chunks: Vec<u8>,
    pub tombstones: Vec<u8>,
}

/// The fixture directory, found from wherever the suite runs.
///
/// Cargo runs a suite from its crate directory. Bazel runs it from the
/// runfiles root, where a `data` file keeps its workspace path.
#[must_use]
pub fn fixture_dir() -> PathBuf {
    [
        "tests/testdata/tsdb",
        "../metrics/tests/testdata/tsdb",
        "crates/metrics/tests/testdata/tsdb",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|candidate| candidate.join(FIXTURE_ULID).is_dir())
    .expect("the TSDB fixture is on the data path of the test")
}

#[must_use]
pub fn fixture_files() -> FixtureFiles {
    let block = fixture_dir().join(FIXTURE_ULID);
    let read = |name: &str| {
        std::fs::read(block.join(name))
            .unwrap_or_else(|error| panic!("read fixture file {name}: {error}"))
    };
    FixtureFiles {
        meta: read("meta.json"),
        index: read("index"),
        chunks: read("chunks/000001"),
        tombstones: read("tombstones"),
    }
}

/// The samples that the Prometheus TSDB library reads from the block, after
/// tombstones, one entry per series in label order.
///
/// Each float and histogram field is the hex form of its `f64` bits, so the
/// comparison is exact and covers NaN payloads.
#[must_use]
pub fn expected_samples() -> Value {
    let compressed = std::fs::read(fixture_dir().join("expected_samples.json.gz"))
        .expect("read expected_samples.json.gz");
    let mut json = String::new();
    GzDecoder::new(compressed.as_slice())
        .read_to_string(&mut json)
        .expect("expected_samples.json.gz is gzip");
    serde_json::from_str(&json).expect("expected_samples.json.gz holds JSON")
}

/// The labels of each series in [`expected_samples`], in order.
#[must_use]
pub fn expected_series_labels() -> Vec<BTreeMap<String, String>> {
    expected_samples()
        .as_array()
        .expect("the expected samples are an array")
        .iter()
        .map(|series| {
            serde_json::from_value(series["labels"].clone()).expect("labels are a string map")
        })
        .collect()
}
