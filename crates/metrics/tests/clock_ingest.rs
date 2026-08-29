//! Clock confidence ingest, from the `/api/v1/clocks` push to the WAL records
//! and the compacted Arrow block.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use arrow::{
    array::{Array, ArrayAccessor as _, AsArray},
    datatypes::{DataType, Int32Type, Int64Type, UInt32Type},
};
use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::validate_against;
use krabka_metrics::{
    ClockReadingPayload, MetricBlockKind, SamplePayload, WalRecord, clock_reading_decl,
    compact_wal_records, compaction_object_key,
    distributor::{
        CLOCK_READING_METRIC, DistributorState, ProduceError, WalSink, clock_series,
        clock_wal_records, router,
    },
    encode_tenant_batches,
    schema::{
        CCOL_GNSS_FIX, CCOL_INGEST_UNIX_NANOS, CCOL_MEAN_PATH_DELAY_NANOS, CCOL_NODE,
        CCOL_ROOT_DELAY_NANOS, CCOL_SOURCE_KIND, CCOL_STRATUM, CCOL_SYNC_STATE,
        CCOL_UNCERTAINTY_NANOS, COL_TIMESTAMP,
    },
    wire::{
        ClockSourceKind, ClockSyncState, ClockWireError, DecodedClockReading, GnssFix, GnssReading,
        NtpReading, PtpReading, UnixNanos, decode_clock_readings, pb,
    },
};
use krabka_units::prelude::*;
use prost::Message as _;
use tower::ServiceExt as _;

/// A round nanosecond instant, so every expected second value in this suite is
/// exact in binary floating point.
const READING_NANOS: i64 = 1_700_000_000_000_000_000;
/// One quarter of a millisecond after the host read its clock.
const INGEST_NANOS: i64 = READING_NANOS + 250_000;

fn snappy(body: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(body)
        .expect("snappy compress")
}

fn snappy_batch(readings: &[pb::clocks::ClockReading]) -> Vec<u8> {
    snappy(
        &pb::clocks::ClockReadingBatch {
            readings: readings.to_vec(),
        }
        .encode_to_vec(),
    )
}

/// A well-formed NTP reading with every discipline field filled.
fn ntp_wire() -> pb::clocks::ClockReading {
    pb::clocks::ClockReading {
        node: "host-a".into(),
        clock: "CLOCK_REALTIME".into(),
        source_kind: pb::clocks::SourceKind::Ntp.into(),
        reading_unix_nanos: READING_NANOS,
        uncertainty_nanos: 2_000_000,
        offset_nanos: -125_000,
        sync_state: pb::clocks::SyncState::Synchronized.into(),
        reference_id: "10.0.0.1".into(),
        last_sync_unix_nanos: READING_NANOS - 8_000_000_000,
        frequency_ppb: -1_250,
        last_step_nanos: 4_000_000,
        root_delay_nanos: 1_500_000,
        root_dispersion_nanos: 3_000_000,
        stratum: 2,
        ..Default::default()
    }
}

/// A well-formed PTP reading. Its NTP columns stay empty by construction.
fn ptp_wire() -> pb::clocks::ClockReading {
    pb::clocks::ClockReading {
        node: "host-b".into(),
        clock: "/dev/ptp0".into(),
        source_kind: pb::clocks::SourceKind::Ptp.into(),
        reading_unix_nanos: READING_NANOS,
        uncertainty_nanos: 50_000,
        offset_nanos: 12_000,
        sync_state: pb::clocks::SyncState::Holdover.into(),
        mean_path_delay_nanos: 7_500,
        steps_removed: 3,
        gm_clock_class: 6,
        gm_clock_accuracy: 32,
        ..Default::default()
    }
}

fn gnss_wire() -> pb::clocks::ClockReading {
    pb::clocks::ClockReading {
        node: "host-c".into(),
        clock: "gnss0".into(),
        source_kind: pb::clocks::SourceKind::Gnss.into(),
        reading_unix_nanos: READING_NANOS,
        uncertainty_nanos: 100,
        sync_state: pb::clocks::SyncState::Synchronized.into(),
        satellites_used: 11,
        gnss_fix: pb::clocks::GnssFix::ThreeD.into(),
        ..Default::default()
    }
}

fn decode_one(wire: pb::clocks::ClockReading) -> DecodedClockReading {
    let mut readings =
        decode_clock_readings(&snappy_batch(&[wire]), mebibytes(1)).expect("decode succeeds");
    assert!(readings.len() == 1);
    readings.remove(0)
}

/// The projected series a batch publishes, keyed by its sorted label set.
///
/// The clock block's own identity series carries no sample, so it never shows
/// up here.
fn projection(
    readings: &[DecodedClockReading],
    ingest: UnixNanos,
) -> BTreeMap<Vec<(String, String)>, f64> {
    clock_series(readings, ingest)
        .into_iter()
        .filter_map(|series| {
            let sample = series.samples.first().copied()?;
            let labels = series
                .labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            Some((labels, sample.value))
        })
        .collect()
}

/// Builds the label key a projected series carries.
fn key(
    name: &str,
    node: &str,
    clock: &str,
    source: &str,
    extra: &[(&str, &str)],
) -> Vec<(String, String)> {
    let mut labels = vec![
        ("__name__".to_string(), name.to_string()),
        ("clock".to_string(), clock.to_string()),
        ("node".to_string(), node.to_string()),
        ("source".to_string(), source.to_string()),
    ];
    labels.extend(
        extra
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
    );
    labels.sort();
    labels
}

fn ntp_key(name: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
    key(name, "host-a", "CLOCK_REALTIME", "ntp", extra)
}

/// The bit pattern of a projected sample.
///
/// Every value this suite checks is an integer or a round binary fraction, so
/// the comparison is exact by construction. A tolerance would only hide a real
/// regression, and comparing bit patterns says that plainly.
fn exactly(value: f64) -> u64 {
    value.to_bits()
}

/// Whether a state series carries the Prometheus `1` that marks it current.
fn is_set(value: f64) -> bool {
    exactly(value) == exactly(1.0)
}

#[test]
fn ntp_reading_decodes_into_its_domain_shape() {
    let decoded = decode_one(ntp_wire());

    assert!(
        decoded
            == DecodedClockReading {
                node: "host-a".to_string(),
                clock: "CLOCK_REALTIME".to_string(),
                source_kind: ClockSourceKind::Ntp,
                reading_unix_nanos: UnixNanos::new(READING_NANOS),
                uncertainty_nanos: 2_000_000,
                offset_nanos: -125_000,
                sync_state: ClockSyncState::Synchronized,
                reference_id: Some("10.0.0.1".to_string()),
                last_sync_unix_nanos: Some(UnixNanos::new(READING_NANOS - 8_000_000_000)),
                frequency_ppb: Some(-1_250),
                last_step_nanos: Some(4_000_000),
                ntp: Some(NtpReading {
                    root_delay_nanos: 1_500_000,
                    root_dispersion_nanos: 3_000_000,
                    stratum: 2,
                }),
                ptp: None,
                timex: None,
                gnss: None,
            }
    );
}

#[test]
fn ptp_reading_carries_only_its_own_source_group() {
    let decoded = decode_one(ptp_wire());

    check!(
        decoded.ptp
            == Some(PtpReading {
                mean_path_delay_nanos: 7_500,
                steps_removed: 3,
                gm_clock_class: 6,
                gm_clock_accuracy: 32,
            })
    );
    check!(decoded.ntp == None);
    check!(decoded.timex == None);
    check!(decoded.gnss == None);
}

#[test]
fn kernel_timex_reading_keeps_a_false_unsync_bit() {
    let decoded = decode_one(pb::clocks::ClockReading {
        source_kind: pb::clocks::SourceKind::KernelTimex.into(),
        max_error_nanos: 16_000_000,
        est_error_nanos: 250_000,
        unsynchronized: false,
        ..ntp_wire()
    });

    assert!(let Some(_) = decoded.timex);
    let timex = decoded.timex.expect("a kernel timex group");
    check!(timex.max_error_nanos == 16_000_000);
    check!(timex.est_error_nanos == 250_000);
    // `false` is the healthy value of `STA_UNSYNC`, and the source kind is what
    // says the column applies. A zero-means-absent rule would erase it.
    check!(!timex.unsynchronized);
}

#[test]
fn gnss_reading_keeps_its_fix_quality() {
    let decoded = decode_one(gnss_wire());

    check!(
        decoded.gnss
            == Some(GnssReading {
                satellites_used: 11,
                fix: Some(GnssFix::ThreeD),
            })
    );
}

#[test]
fn an_unspecified_gnss_fix_is_absent_rather_than_a_rejection() {
    let decoded = decode_one(pb::clocks::ClockReading {
        gnss_fix: pb::clocks::GnssFix::Unspecified.into(),
        ..gnss_wire()
    });

    check!(
        decoded.gnss
            == Some(GnssReading {
                satellites_used: 11,
                fix: None
            })
    );
}

#[test]
fn every_malformed_reading_is_an_error_and_not_a_panic() {
    type Matches = fn(&ClockWireError) -> bool;

    let cases: [(&str, pb::clocks::ClockReading, Matches); 7] = [
        (
            "empty node",
            pb::clocks::ClockReading {
                node: String::new(),
                ..ntp_wire()
            },
            |error| matches!(error, ClockWireError::EmptyIdentity { field: "node", .. }),
        ),
        (
            "empty clock",
            pb::clocks::ClockReading {
                clock: String::new(),
                ..ntp_wire()
            },
            |error| matches!(error, ClockWireError::EmptyIdentity { field: "clock", .. }),
        ),
        (
            "negative uncertainty",
            pb::clocks::ClockReading {
                uncertainty_nanos: -1,
                ..ntp_wire()
            },
            |error| matches!(error, ClockWireError::NegativeUncertainty { .. }),
        ),
        (
            "far-future reading",
            pb::clocks::ClockReading {
                reading_unix_nanos: i64::MAX,
                ..ntp_wire()
            },
            |error| matches!(error, ClockWireError::ReadingTooFarInFuture { .. }),
        ),
        (
            "unspecified source kind",
            pb::clocks::ClockReading {
                source_kind: pb::clocks::SourceKind::Unspecified.into(),
                ..ntp_wire()
            },
            |error| {
                matches!(
                    error,
                    ClockWireError::UnspecifiedEnum {
                        field: "source_kind",
                        ..
                    }
                )
            },
        ),
        (
            "unspecified sync state",
            pb::clocks::ClockReading {
                sync_state: pb::clocks::SyncState::Unspecified.into(),
                ..ntp_wire()
            },
            |error| {
                matches!(
                    error,
                    ClockWireError::UnspecifiedEnum {
                        field: "sync_state",
                        ..
                    }
                )
            },
        ),
        (
            "unknown source kind discriminant",
            pb::clocks::ClockReading {
                source_kind: 4242,
                ..ntp_wire()
            },
            |error| {
                matches!(
                    error,
                    ClockWireError::UnknownEnum {
                        field: "source_kind",
                        value: 4242,
                        ..
                    }
                )
            },
        ),
    ];

    for (name, wire, matches) in cases {
        let error = decode_clock_readings(&snappy_batch(&[wire]), mebibytes(1))
            .expect_err("malformed reading is rejected");

        check!(matches(&error), "case `{name}` gave {error}");
        check!(error.status_code() == 400, "case `{name}`");
    }
}

#[test]
fn a_body_that_is_not_snappy_is_an_error() {
    let error = decode_clock_readings(b"not snappy at all", mebibytes(1))
        .expect_err("a bare body is rejected");

    check!(error.status_code() == 400);
}

#[test]
fn a_decompression_bomb_is_capped_by_the_shared_ceiling() {
    let body = snappy_batch(&[ntp_wire(), ntp_wire(), ntp_wire()]);

    let error =
        decode_clock_readings(&body, bytes(8)).expect_err("the shared cap rejects the body");

    check!(error.status_code() == 400);
}

#[test]
fn the_ntp_projection_is_exactly_the_expected_series() {
    let readings = vec![decode_one(ntp_wire())];

    let projected = projection(&readings, UnixNanos::new(INGEST_NANOS));

    assert!(
        projected
            == BTreeMap::from([
                (ntp_key("krabka_clock_uncertainty_seconds", &[]), 0.002),
                (ntp_key("krabka_clock_offset_seconds", &[]), -0.000_125),
                (ntp_key("krabka_clock_ingest_skew_seconds", &[]), 0.000_25),
                (
                    ntp_key("krabka_clock_last_sync_seconds", &[]),
                    1_699_999_992.0
                ),
                (ntp_key("krabka_clock_frequency_ppb", &[]), -1_250.0),
                (ntp_key("krabka_clock_step_seconds_total", &[]), 0.004),
                (ntp_key("krabka_clock_root_delay_seconds", &[]), 0.0015),
                (ntp_key("krabka_clock_root_dispersion_seconds", &[]), 0.003),
                (ntp_key("krabka_clock_stratum", &[]), 2.0),
                (
                    ntp_key("krabka_clock_sync_state", &[("state", "synchronized")]),
                    1.0
                ),
                (
                    ntp_key("krabka_clock_sync_state", &[("state", "holdover")]),
                    0.0
                ),
                (
                    ntp_key("krabka_clock_sync_state", &[("state", "free_running")]),
                    0.0
                ),
                (
                    ntp_key("krabka_clock_sync_state", &[("state", "unsynchronized")]),
                    0.0
                ),
                (
                    ntp_key("krabka_clock_sync_state", &[("state", "stepped")]),
                    0.0
                ),
            ])
    );
}

#[test]
fn a_state_transition_moves_the_one_and_zeroes_the_rest() {
    let holdover = vec![decode_one(pb::clocks::ClockReading {
        sync_state: pb::clocks::SyncState::Holdover.into(),
        ..ntp_wire()
    })];

    let projected = projection(&holdover, UnixNanos::new(INGEST_NANOS));
    let states = [
        "synchronized",
        "holdover",
        "free_running",
        "unsynchronized",
        "stepped",
    ]
    .map(|state| projected[&ntp_key("krabka_clock_sync_state", &[("state", state)])])
    .map(is_set);

    assert!(states == [false, true, false, false, false]);
}

#[test]
fn an_ntp_host_publishes_no_ptp_path_delay() {
    let readings = vec![decode_one(ntp_wire())];

    let names = projection(&readings, UnixNanos::new(INGEST_NANOS))
        .into_keys()
        .filter_map(|labels| {
            labels
                .into_iter()
                .find(|(name, _)| name == "__name__")
                .map(|(_, value)| value)
        })
        .collect::<Vec<_>>();

    check!(!names.contains(&"krabka_clock_path_delay_seconds".to_string()));
    check!(!names.contains(&"krabka_clock_steps_removed".to_string()));
    check!(!names.contains(&"krabka_clock_class".to_string()));
    check!(!names.contains(&"krabka_gnss_satellites_used".to_string()));
    check!(!names.contains(&"krabka_gnss_fix".to_string()));
}

#[test]
fn a_gnss_reading_publishes_the_whole_fix_family() {
    let readings = vec![decode_one(gnss_wire())];

    let projected = projection(&readings, UnixNanos::new(INGEST_NANOS));
    let fixes = ["none", "2d", "3d"]
        .map(|fix| {
            projected[&key(
                "krabka_gnss_fix",
                "host-c",
                "gnss0",
                "gnss",
                &[("fix", fix)],
            )]
        })
        .map(is_set);

    assert!(fixes == [false, false, true]);
    check!(
        exactly(
            projected[&key(
                "krabka_gnss_satellites_used",
                "host-c",
                "gnss0",
                "gnss",
                &[]
            )]
        ) == exactly(11.0)
    );
}

#[test]
fn a_reading_with_no_gnss_fix_publishes_no_fix_family() {
    let readings = vec![decode_one(pb::clocks::ClockReading {
        gnss_fix: pb::clocks::GnssFix::Unspecified.into(),
        ..gnss_wire()
    })];

    let projected = projection(&readings, UnixNanos::new(INGEST_NANOS));

    check!(!projected.contains_key(&key(
        "krabka_gnss_fix",
        "host-c",
        "gnss0",
        "gnss",
        &[("fix", "none")]
    )));
}

#[test]
fn the_clock_wal_record_carries_the_identity_and_the_ingest_stamp() {
    let reading = decode_one(ntp_wire());
    let records = clock_wal_records(
        "tenant-a",
        std::slice::from_ref(&reading),
        UnixNanos::new(INGEST_NANOS),
    );

    assert!(
        records
            == vec![WalRecord {
                tenant: "tenant-a".to_string(),
                labels: vec![
                    ("__name__".to_string(), CLOCK_READING_METRIC.to_string()),
                    ("node".to_string(), "host-a".to_string()),
                    ("clock".to_string(), "CLOCK_REALTIME".to_string()),
                    ("source".to_string(), "ntp".to_string()),
                ],
                payload: SamplePayload::ClockReading(Box::new(ClockReadingPayload {
                    reading,
                    ingest_unix_nanos: UnixNanos::new(INGEST_NANOS),
                })),
                exemplars: Vec::new(),
            }]
    );
    check!(records[0].payload.timestamp_ms() == Some(READING_NANOS / 1_000_000));
}

#[test]
fn the_wal_record_round_trips_through_its_codec() {
    let reading = decode_one(gnss_wire());
    let record = clock_wal_records("tenant-a", &[reading], UnixNanos::new(INGEST_NANOS)).remove(0);

    let back = WalRecord::decode(&record.encode().expect("encode")).expect("decode");

    assert!(back == record);
}

// ---------------------------------------------------------------------------
// Compaction.
// ---------------------------------------------------------------------------

fn clock_batch(wire: pb::clocks::ClockReading) -> arrow::record_batch::RecordBatch {
    let reading = decode_one(wire);
    let records = clock_wal_records("tenant-a", &[reading], UnixNanos::new(INGEST_NANOS));
    let rows = compact_wal_records(&records);
    assert!(rows.len() == 1);
    encode_tenant_batches(&rows[0])
        .expect("encode batches")
        .clock_readings
        .expect("a clock block")
}

#[test]
fn the_clock_block_validates_against_its_declaration() {
    let batch = clock_batch(ntp_wire());

    check!(validate_against(&batch.schema(), &clock_reading_decl()).is_ok());
    check!(batch.num_rows() == 1);
}

#[test]
fn the_clock_block_dictionary_encodes_its_identity_columns() {
    let batch = clock_batch(ntp_wire());
    let schema = batch.schema();

    for column in [CCOL_NODE, CCOL_SOURCE_KIND, CCOL_SYNC_STATE] {
        let (_, field) = schema.column_with_name(column).expect("column");
        check!(
            field.data_type()
                == &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            "column {column}",
        );
    }

    let nodes = batch
        .column_by_name(CCOL_NODE)
        .expect("node column")
        .as_dictionary::<Int32Type>()
        .downcast_dict::<arrow::array::StringArray>()
        .expect("utf8 values");
    check!(nodes.value(0) == "host-a");
}

#[test]
fn a_column_the_reading_did_not_supply_is_null() {
    let batch = clock_batch(ntp_wire());

    // The NTP group is filled.
    let root_delay = batch
        .column_by_name(CCOL_ROOT_DELAY_NANOS)
        .expect("root delay column")
        .as_primitive::<Int64Type>();
    check!(root_delay.value(0) == 1_500_000);
    let stratum = batch
        .column_by_name(CCOL_STRATUM)
        .expect("stratum column")
        .as_primitive::<UInt32Type>();
    check!(stratum.value(0) == 2);

    // Every other source group is null, not zero.
    for column in [CCOL_MEAN_PATH_DELAY_NANOS, CCOL_GNSS_FIX] {
        check!(
            batch.column_by_name(column).expect("column").is_null(0),
            "column {column}",
        );
    }
}

#[test]
fn the_clock_block_keeps_the_reading_in_nanoseconds_and_the_stamp_in_milliseconds() {
    let batch = clock_batch(ntp_wire());

    let timestamps = batch
        .column_by_name(COL_TIMESTAMP)
        .expect("timestamp column")
        .as_primitive::<Int64Type>();
    let uncertainty = batch
        .column_by_name(CCOL_UNCERTAINTY_NANOS)
        .expect("uncertainty column")
        .as_primitive::<Int64Type>();
    let ingest = batch
        .column_by_name(CCOL_INGEST_UNIX_NANOS)
        .expect("ingest column")
        .as_primitive::<Int64Type>();

    check!(timestamps.value(0) == READING_NANOS / 1_000_000);
    check!(uncertainty.value(0) == 2_000_000);
    check!(ingest.value(0) == INGEST_NANOS);
}

#[test]
fn the_clock_block_has_its_own_object_path() {
    let key = compaction_object_key("tenant-a", MetricBlockKind::ClockReadings, 42, 99);

    check!(key.contains("clock-readings"), "key was `{key}`");
}

// ---------------------------------------------------------------------------
// The HTTP route.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingSink {
    records: Mutex<Vec<WalRecord>>,
}

#[async_trait::async_trait]
impl WalSink for RecordingSink {
    async fn append(&self, _key: bytes::Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.records
            .lock()
            .expect("recording sink poisoned")
            .push(record);
        Ok(())
    }
}

async fn push_clocks(sink: &Arc<RecordingSink>, body: Vec<u8>, tenant: &str) -> StatusCode {
    let state = Arc::new(DistributorState::new(Arc::clone(sink) as Arc<dyn WalSink>));
    router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clocks")
                .header("Content-Encoding", "snappy")
                .header("X-Scope-OrgID", tenant)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("clocks response")
        .status()
}

fn now_unix_nanos() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_nanos(),
    )
    .expect("before the year 2262")
}

#[tokio::test]
async fn a_pushed_batch_lands_as_a_clock_record_and_its_projection() {
    let sink = Arc::new(RecordingSink::default());
    let before = now_unix_nanos();

    let status = push_clocks(&sink, snappy_batch(&[ptp_wire()]), "tenant-a").await;
    let after = now_unix_nanos();

    assert!(status == StatusCode::NO_CONTENT);
    let records = sink.records.lock().expect("sink").clone();

    let clock_records = records
        .iter()
        .filter_map(|record| match &record.payload {
            SamplePayload::ClockReading(payload) => Some(payload.as_ref().clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(clock_records.len() == 1);
    check!(clock_records[0].reading == decode_one(ptp_wire()));

    // The stamp comes from this process's clock at receive, not from the wire.
    let stamped = clock_records[0].ingest_unix_nanos.as_i64();
    check!(stamped >= before && stamped <= after, "stamp was {stamped}");

    // Every projected series arrives as an ordinary float record.
    let float_names = records
        .iter()
        .filter(|record| matches!(record.payload, SamplePayload::Float { .. }))
        .filter_map(|record| {
            record
                .labels
                .iter()
                .find(|(name, _)| name == "__name__")
                .map(|(_, value)| value.clone())
        })
        .collect::<Vec<_>>();
    check!(float_names.contains(&"krabka_clock_path_delay_seconds".to_string()));
    check!(float_names.contains(&"krabka_clock_uncertainty_seconds".to_string()));
    check!(float_names.contains(&"krabka_clock_sync_state".to_string()));
    check!(!float_names.contains(&CLOCK_READING_METRIC.to_string()));
}

#[tokio::test]
async fn the_ingest_skew_series_reflects_the_stamp() {
    let sink = Arc::new(RecordingSink::default());
    // A reading one whole second in the past gives a skew of at least a second.
    let stale = pb::clocks::ClockReading {
        reading_unix_nanos: now_unix_nanos() - 1_000_000_000,
        ..ptp_wire()
    };

    let status = push_clocks(&sink, snappy_batch(&[stale]), "tenant-a").await;

    assert!(status == StatusCode::NO_CONTENT);
    let records = sink.records.lock().expect("sink").clone();
    let skew = records
        .iter()
        .find_map(|record| {
            let is_skew = record.labels.iter().any(|(name, value)| {
                name == "__name__" && value == "krabka_clock_ingest_skew_seconds"
            });
            match (&record.payload, is_skew) {
                (SamplePayload::Float { value, .. }, true) => Some(*value),
                _ => None,
            }
        })
        .expect("a skew sample");

    check!(skew >= 1.0, "skew was {skew}");
    check!(skew < 60.0, "skew was {skew}");
}

#[tokio::test]
async fn a_malformed_batch_is_rejected_and_writes_nothing() {
    let sink = Arc::new(RecordingSink::default());

    let status = push_clocks(
        &sink,
        snappy_batch(&[pb::clocks::ClockReading {
            node: String::new(),
            ..ntp_wire()
        }]),
        "tenant-a",
    )
    .await;

    check!(status == StatusCode::BAD_REQUEST);
    check!(sink.records.lock().expect("sink").is_empty());
}

#[tokio::test]
async fn a_batch_without_a_tenant_is_rejected() {
    let sink = Arc::new(RecordingSink::default());
    let state = Arc::new(DistributorState::new(Arc::clone(&sink) as Arc<dyn WalSink>));

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clocks")
                .header("Content-Encoding", "snappy")
                .body(Body::from(snappy_batch(&[ntp_wire()])))
                .expect("request"),
        )
        .await
        .expect("clocks response");

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(sink.records.lock().expect("sink").is_empty());
}

#[tokio::test]
async fn a_batch_without_snappy_encoding_is_rejected() {
    let sink = Arc::new(RecordingSink::default());
    let state = Arc::new(DistributorState::new(Arc::clone(&sink) as Arc<dyn WalSink>));

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clocks")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::from(snappy_batch(&[ntp_wire()])))
                .expect("request"),
        )
        .await
        .expect("clocks response");

    check!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
    check!(sink.records.lock().expect("sink").is_empty());
}
