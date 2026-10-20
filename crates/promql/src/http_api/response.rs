use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use krabka_blockstore::SeriesFingerprint;
use krabka_metrics::{BucketSpan, NativeHistogram};
use serde_json::{Map, Value, json};

use super::apply_limit;
use crate::{
    Annotations, PromqlLabels as Labels, QueryResult, RangeSeries, SampleValue,
    store::ExemplarRecord,
};
mod append_spanned_buckets;
mod append_standard_histogram_buckets;
mod cardinality_label_names_response;
mod cardinality_label_values_response;
mod exemplars_json;
mod labels_json;
mod native_histogram_buckets_json;
mod native_histogram_string;
mod range_matrix_json;
mod result_json;
mod success_response;
mod success_response_with_stats;
use append_spanned_buckets::append_spanned_buckets;
use append_standard_histogram_buckets::append_standard_histogram_buckets;
pub(super) use cardinality_label_names_response::cardinality_label_names_response;
pub(super) use cardinality_label_values_response::cardinality_label_values_response;
pub(super) use exemplars_json::exemplars_json;
pub(super) use labels_json::labels_json;
pub(crate) use native_histogram_buckets_json::native_histogram_buckets;
use native_histogram_buckets_json::native_histogram_buckets_json;
pub(crate) use native_histogram_string::native_histogram_string;
use range_matrix_json::range_matrix_json;
use result_json::result_json;
pub(super) use success_response::{annotated_success_response, success_response};
pub(super) use success_response_with_stats::{
    QueryPhaseDurations, QueryResponseStats, success_response_with_stats,
};

use crate::engine::standard_histogram_bound;

pub(crate) struct HistogramBucketJson {
    pub(crate) boundary_rule: u8,
    pub(crate) lower: f64,
    pub(crate) upper: f64,
    pub(crate) count: f64,
}

pub(crate) const BOUNDARY_CLOSED_BOTH: u8 = 3;

pub(crate) const BOUNDARY_OPEN_LEFT: u8 = 0;

pub(crate) const BOUNDARY_OPEN_RIGHT: u8 = 1;

pub(crate) fn custom_histogram_bound(index: i32, custom_values: &[f64]) -> f64 {
    match index {
        -1 => f64::NEG_INFINITY,
        _ => usize::try_from(index)
            .ok()
            .and_then(|index| custom_values.get(index).copied())
            .unwrap_or(f64::INFINITY),
    }
}

pub(crate) fn exemplar_key(exemplar: &ExemplarRecord) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        labels_key(&exemplar.series_labels),
        labels_key(&exemplar.labels),
        exemplar.ts_ms,
        exemplar.value.to_bits()
    )
}

pub(crate) fn labels_key(labels: &Labels) -> String {
    labels.order_key()
}

pub(crate) fn native_histogram_json(histogram: &NativeHistogram) -> Value {
    let mut object = Map::new();
    object.insert("count".to_string(), json!(sample_string(histogram.count)));
    object.insert("sum".to_string(), json!(sample_string(histogram.sum)));
    let buckets = native_histogram_buckets_json(histogram);
    if !buckets.is_empty() {
        object.insert("buckets".to_string(), json!(buckets));
    }
    Value::Object(object)
}

pub(crate) fn sample_string(value: f64) -> String {
    format_sample_value(value)
}

pub(crate) fn success_data_response(data: impl serde::Serialize) -> Response {
    Json(json!({
        "status": "success",
        "data": data,
    }))
    .into_response()
}

/// Builds the Grafana Mimir `/cardinality/active_series` response.
///
/// The response is a bare object with one `data` array of flat label maps. It
/// has no `status` envelope and no `seriesLabels` or `metric` wrapper.
pub(crate) fn active_series_response(series: Vec<Labels>) -> Value {
    let data = series
        .into_iter()
        .map(|labels| labels_json(labels.iter()))
        .collect::<Vec<_>>();
    json!({ "data": data })
}

pub(crate) fn append_custom_histogram_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    hist: &NativeHistogram,
) {
    let custom_values = hist.custom_values.as_deref().unwrap_or_default();
    append_spanned_buckets(
        buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| HistogramBucketJson {
            boundary_rule: if index == 0 {
                BOUNDARY_CLOSED_BOTH
            } else {
                BOUNDARY_OPEN_LEFT
            },
            lower: custom_histogram_bound(index - 1, custom_values),
            upper: custom_histogram_bound(index, custom_values),
            count: 0.0,
        },
    );
}

/// Renders `f` in the Go `'e'` form, for example `1e+21`, `9.999e-07`, `-1.5e-07`.
///
/// The Rust `{:e}` format produces the same shortest mantissa but a bare
/// exponent, such as `1e21` and `9.999e-7`. Go always writes a sign and at least
/// two exponent digits. This function therefore re-assembles the exponent
/// suffix.
pub(crate) fn format_float_exponent(f: f64) -> String {
    let rust = format!("{f:e}");
    let exponent_marker = rust
        .find('e')
        .expect("Rust {:e} formatting always contains an exponent marker");
    let mantissa = &rust[..exponent_marker];
    let exponent: i32 = rust[exponent_marker + 1..]
        .parse()
        .expect("Rust {:e} exponent is always a valid integer");
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.abs())
}

/// Formats a float exactly like Prometheus `jsonutil.MarshalFloat`.
///
/// `jsonutil.MarshalFloat` calls the Go function
/// `strconv.AppendFloat(f, fmt, -1, 64)`. Go picks the `'e'` scientific notation
/// when the magnitude is `< 1e-6` or `>= 1e21`, and the `'f'` plain decimal
/// notation otherwise. Precision `-1` means the shortest representation that
/// round-trips back to the same `f64`.
pub(crate) fn format_sample_value(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f == f64::INFINITY {
        return "+Inf".to_string();
    }
    if f == f64::NEG_INFINITY {
        return "-Inf".to_string();
    }

    let abs = f.abs();
    if abs != 0.0 && !(1e-6..1e21).contains(&abs) {
        format_float_exponent(f)
    } else {
        // Rust's `Display` for `f64` already emits the shortest round-tripping
        // plain-decimal form (no exponent), matching Go's `'f'` form: `3.0` ->
        // "3", `1e20` -> "100000000000000000000", `0.000001` -> "0.000001".
        format!("{f}")
    }
}

/// Builds the JSON number token for a millisecond timestamp.
///
/// This function mirrors Prometheus `MarshalTimestamp`. It writes the sign, then
/// the absolute integer seconds, then a millisecond fraction with the trailing
/// zeros trimmed when that fraction is non-zero.
pub(crate) fn format_timestamp_token(ts_ms: i64) -> String {
    let mut out = String::new();
    if ts_ms < 0 {
        out.push('-');
    }
    let magnitude = ts_ms.unsigned_abs();
    let seconds = magnitude / 1000;
    let fraction = magnitude % 1000;
    out.push_str(&seconds.to_string());
    if fraction != 0 {
        out.push('.');
        let padded = format!("{fraction:03}");
        out.push_str(padded.trim_end_matches('0'));
    }
    out
}

/// Encodes a millisecond timestamp as the JSON number Prometheus emits.
///
/// Prometheus emits this number from `jsonutil.MarshalTimestamp`. The number is
/// a bare integer for whole seconds, and otherwise a fraction with the trailing
/// zeros trimmed. Examples: `10`, `1435781430.781`, `-0.5`.
///
/// `serde_json` renders an `f64` of `10` as `10.0`. This function therefore
/// carries the value as a pre-formatted
/// [`RawValue`](serde_json::value::RawValue) number token, which keeps the
/// output byte-exact.
pub(crate) fn timestamp_seconds(ts_ms: i64) -> Box<serde_json::value::RawValue> {
    let token = format_timestamp_token(ts_ms);
    serde_json::value::RawValue::from_string(token)
        .expect("timestamp token is always valid JSON number syntax")
}
