use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::AsArray,
    datatypes::{Float64Type, Int64Type, UInt64Type},
};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use krabka_blockstore::{MatchOp, SeriesFingerprint, TenantId};
use krabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, decode_native_histograms,
    wire::{
        encode_chunked_read_frames, encode_read_response, negotiate_read_response_type,
        remote_read_pb as pb, snappy_block_decode,
    },
};
use num_traits::ToPrimitive;
use prost::Message;

use super::{
    ApiError, Extension, Principal, PrometheusApiState, authorized_tenant_from_headers,
    enforce_query_range_limit, enforce_sample_count, enforce_selected_series_limit,
    validate_timestamp_range,
};
use crate::{
    MetricStore, PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher,
    store::{ExemplarRecord, ScanResult},
};
mod append_remote_read_float_samples;
mod remote_read_fn;
mod remote_read_histogram;
mod remote_read_response;
mod remote_read_sample_sink;
mod require_remote_read_headers;
use append_remote_read_float_samples::append_remote_read_float_samples;
pub(super) use remote_read_fn::remote_read;
use remote_read_histogram::remote_read_histogram;
use remote_read_response::remote_read_response;
use remote_read_sample_sink::{RemoteReadSampleSink, collect_remote_read_batches};
use require_remote_read_headers::require_remote_read_headers;

pub(crate) fn header_list_includes(value: &str, expected: &str) -> bool {
    value
        .split(',')
        .any(|item| item.trim().eq_ignore_ascii_case(expected))
}

pub(crate) fn negotiate_remote_read_response_type(
    request: &pb::v1::ReadRequest,
) -> Result<pb::v1::ResponseType, ApiError> {
    negotiate_read_response_type(&request.accepted_response_types).map_err(|error| ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        error_type: "execution",
        message: error.to_string(),
    })
}

pub(crate) fn remote_read_bucket_spans(spans: &[BucketSpan]) -> Vec<pb::v1::BucketSpan> {
    spans
        .iter()
        .map(|span| pb::v1::BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

pub(crate) fn remote_read_exemplar(exemplar: &ExemplarRecord) -> pb::v1::Exemplar {
    pb::v1::Exemplar {
        labels: remote_read_labels(&exemplar.labels),
        value: exemplar.value,
        timestamp: exemplar.ts_ms,
    }
}

pub(crate) fn remote_read_histogram_count(hist: &NativeHistogram) -> pb::v1::histogram::Count {
    if hist.is_float {
        pb::v1::histogram::Count::CountFloat(hist.count)
    } else {
        pb::v1::histogram::Count::CountInt(hist.count.to_u64().unwrap_or(u64::MAX))
    }
}

pub(crate) fn remote_read_histogram_zero_count(
    hist: &NativeHistogram,
) -> pb::v1::histogram::ZeroCount {
    if hist.is_float {
        pb::v1::histogram::ZeroCount::ZeroCountFloat(hist.zero_count)
    } else {
        pb::v1::histogram::ZeroCount::ZeroCountInt(hist.zero_count.to_u64().unwrap_or(u64::MAX))
    }
}

pub(crate) fn remote_read_labels(labels: &Labels) -> Vec<pb::v1::Label> {
    labels
        .iter()
        .map(|pair| pb::v1::Label {
            name: pair.name.clone(),
            value: pair.value.as_bytes().to_vec(),
        })
        .collect()
}

pub(crate) async fn append_remote_read_exemplars<S: MetricStore>(
    store: &S,
    tenant: &str,
    matchers: &[LabelMatcher],
    start_ms: i64,
    end_ms: i64,
    labels_by_fp: &mut BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
) -> Result<(), ApiError> {
    // Remote read has no warnings field in its protobuf response, so a skipped
    // block stays in the block store's log and reaches no further here.
    let scan = store
        .exemplars(tenant, matchers, start_ms, end_ms)
        .await
        .map_err(ApiError::from)?;
    for exemplar in scan.exemplars {
        let fp = exemplar.series_labels.fingerprint();
        labels_by_fp
            .entry(fp)
            .or_insert_with(|| exemplar.series_labels.clone());
        let series = remote_read_series(by_fp, labels_by_fp, fp)?;
        series.exemplars.push(remote_read_exemplar(&exemplar));
    }
    Ok(())
}

pub(crate) async fn append_remote_read_histogram_samples<S: MetricStore>(
    sink: &mut RemoteReadSampleSink<'_, S>,
    scan: &ScanResult,
    table: &str,
) -> Result<(), ApiError> {
    let batches = collect_remote_read_batches(
        scan,
        &format!("SELECT * FROM {table} ORDER BY series_fingerprint, timestamp"),
    )
    .await?;

    for batch in batches {
        for row in decode_native_histograms(&batch)
            .map_err(|error| ApiError::internal(error.to_string()))?
        {
            let series = sink.next_sample_series(row.fingerprint)?;
            series
                .histograms
                .push(remote_read_histogram(row.timestamp_ms, &row.hist));
        }
    }
    Ok(())
}

pub(crate) fn remote_read_histogram_deltas(is_float: bool, counts: &[f64]) -> Vec<i64> {
    if is_float {
        return Vec::new();
    }
    let mut previous = 0.0;
    counts
        .iter()
        .map(|count| {
            let delta = *count - previous;
            previous = *count;
            delta.to_i64().unwrap_or_else(|| {
                if delta.is_sign_negative() {
                    i64::MIN
                } else {
                    i64::MAX
                }
            })
        })
        .collect()
}

pub(crate) fn remote_read_matchers(
    matchers: &[pb::v1::LabelMatcher],
) -> Result<Vec<LabelMatcher>, ApiError> {
    matchers
        .iter()
        .map(|matcher| {
            let op = MatchOp::try_from(matcher.r#type).map_err(|other| {
                ApiError::bad_data(format!("unknown remote_read matcher type {other}"))
            })?;
            Ok(LabelMatcher::new(
                &matcher.name,
                op,
                crate::PromqlString::from(matcher.value.clone()),
            ))
        })
        .collect()
}

/// The remote-read protobuf `Histogram.reset_hint` value for `reset_hint`, as
/// the `i32` that prost stores for an enum field.
#[must_use]
pub fn remote_read_reset_hint(reset_hint: ResetHint) -> i32 {
    match reset_hint {
        ResetHint::Unknown => pb::v1::histogram::ResetHint::Unknown as i32,
        ResetHint::Yes => pb::v1::histogram::ResetHint::Yes as i32,
        ResetHint::No => pb::v1::histogram::ResetHint::No as i32,
        ResetHint::Gauge => pb::v1::histogram::ResetHint::Gauge as i32,
    }
}

pub(crate) fn remote_read_series<'a>(
    by_fp: &'a mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    fp: SeriesFingerprint,
) -> Result<&'a mut pb::v1::TimeSeries, ApiError> {
    let labels = labels_by_fp
        .get(&fp)
        .ok_or_else(|| ApiError::bad_data("remote_read series labels not found"))?;
    Ok(by_fp.entry(fp).or_insert_with(|| pb::v1::TimeSeries {
        labels: remote_read_labels(labels),
        samples: Vec::new(),
        exemplars: Vec::new(),
        histograms: Vec::new(),
    }))
}
