use std::collections::BTreeMap;

use axum::{
    body::Bytes,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use krabka_blockstore::SeriesFingerprint;
use num_traits::ToPrimitive;
use serde::Serialize;

use super::{
    ApiError, Arc, CardinalityParams, MetricStore, ParsedQuery, PrometheusApiState, RequestAuth,
    RequestCaller, authorized_cardinality_series, decode_float_samples, decode_native_histograms,
    enforce_selected_series_limit, parse_cardinality_form, selector_matchers,
};

#[derive(Serialize)]
struct ActiveNativeHistogramMetricsResponse {
    data: Vec<ActiveNativeHistogramMetric>,
}

#[derive(Default, Serialize)]
struct ActiveNativeHistogramMetric {
    metric: String,
    series_count: u64,
    bucket_count: u64,
    avg_bucket_count: f64,
    min_bucket_count: u64,
    max_bucket_count: u64,
}

enum LatestSample {
    Float { timestamp: i64 },
    Histogram { timestamp: i64, bucket_count: u64 },
}

impl LatestSample {
    const fn timestamp(&self) -> i64 {
        match self {
            Self::Float { timestamp } | Self::Histogram { timestamp, .. } => *timestamp,
        }
    }
}

fn keep_latest_sample(
    latest: &mut BTreeMap<SeriesFingerprint, LatestSample>,
    fingerprint: SeriesFingerprint,
    candidate: LatestSample,
) {
    if latest
        .get(&fingerprint)
        .is_none_or(|current| current.timestamp() < candidate.timestamp())
    {
        latest.insert(fingerprint, candidate);
    }
}

fn keep_decoded_samples<T>(
    decoded: Vec<T>,
    known_series: &BTreeMap<SeriesFingerprint, String>,
    latest: &mut BTreeMap<SeriesFingerprint, LatestSample>,
    mut identity: impl FnMut(&T) -> (SeriesFingerprint, i64),
    mut candidate: impl FnMut(T, i64) -> LatestSample,
) {
    for row in decoded {
        let (fingerprint, timestamp) = identity(&row);
        if known_series.contains_key(&fingerprint) {
            keep_latest_sample(latest, fingerprint, candidate(row, timestamp));
        }
    }
}

struct LatestHistogram {
    bucket_count: u64,
}

macro_rules! decode_cardinality_batch {
    ($decode:ident, $batch:expr) => {
        match $decode($batch) {
            Ok(decoded) => decoded,
            Err(error) => return ApiError::internal(error.to_string()).into_response(),
        }
    };
}

pub(crate) async fn cardinality_active_native_histogram_metrics<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<CardinalityParams>,
) -> Response {
    cardinality_active_native_histogram_metrics_inner(&state, caller.auth(), params).await
}

pub(crate) async fn cardinality_active_native_histogram_metrics_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_native_histogram_metrics_inner(&state, caller.auth(), params).await
}

async fn cardinality_active_native_histogram_metrics_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: CardinalityParams,
) -> Response {
    let selected = match authorized_cardinality_series(state, auth, &params).await {
        Ok(selected) => selected,
        Err(rejection) => return rejection.into_response(),
    };
    if let Err(error) =
        enforce_selected_series_limit(state, &selected.tenant, selected.series.len())
    {
        return error.into_response();
    }
    let active = match state
        .store
        .cardinality_active_series(selected.tenant.as_str())
        .await
    {
        Ok(series) => series
            .into_iter()
            .map(|labels| labels.fingerprint())
            .collect::<std::collections::BTreeSet<_>>(),
        Err(error) => return ApiError::from(error).into_response(),
    };
    let mut metric_by_fingerprint = BTreeMap::new();
    for labels in selected.series {
        let fingerprint = labels.fingerprint();
        if !active.contains(&fingerprint) {
            continue;
        }
        let Some(metric) = labels.get("__name__") else {
            continue;
        };
        metric_by_fingerprint.insert(fingerprint, metric.to_owned());
    }

    let matcher_sets = match params.selector.as_deref() {
        Some(selector) => match selector_matchers(selector) {
            Ok(matchers) => matchers,
            Err(error) => return ApiError::from(error).into_response(),
        },
        None => vec![Vec::new()],
    };
    let mut latest = BTreeMap::<SeriesFingerprint, LatestSample>::new();
    for matchers in matcher_sets {
        let scan = match state
            .store
            .scan(selected.tenant.as_str(), &matchers, i64::MIN, i64::MAX)
            .await
        {
            Ok(scan) => scan,
            Err(error) => return ApiError::from(error).into_response(),
        };
        if let Some(table) = scan.float_table {
            let batches = match table_batches(&scan.ctx, &table).await {
                Ok(batches) => batches,
                Err(error) => return error.into_response(),
            };
            for batch in batches {
                let decoded = decode_cardinality_batch!(decode_float_samples, &batch);
                keep_decoded_samples(
                    decoded,
                    &metric_by_fingerprint,
                    &mut latest,
                    |row| (row.fingerprint, row.timestamp_ms),
                    |_, timestamp| LatestSample::Float { timestamp },
                );
            }
        }
        let Some(table) = scan.histogram_table else {
            continue;
        };
        let batches = match table_batches(&scan.ctx, &table).await {
            Ok(batches) => batches,
            Err(error) => return error.into_response(),
        };
        for batch in batches {
            let decoded = decode_cardinality_batch!(decode_native_histograms, &batch);
            keep_decoded_samples(
                decoded,
                &metric_by_fingerprint,
                &mut latest,
                |row| (row.fingerprint, row.timestamp_ms),
                |row, timestamp| {
                    let histogram = row.hist;
                    let bucket_count = u64::try_from(
                        histogram.positive_counts.len() + histogram.negative_counts.len(),
                    )
                    .expect("bucket count fits in u64");
                    LatestSample::Histogram {
                        timestamp,
                        bucket_count,
                    }
                },
            );
        }
    }

    let mut metrics = BTreeMap::<String, ActiveNativeHistogramMetric>::new();
    for fingerprint in latest.keys().copied().collect::<Vec<_>>() {
        let sample = latest
            .remove(&fingerprint)
            .expect("fingerprint came from the latest sample map");
        let LatestSample::Histogram {
            timestamp: _,
            bucket_count,
        } = sample
        else {
            continue;
        };
        let histogram = LatestHistogram { bucket_count };
        let metric_name = metric_by_fingerprint
            .get(&fingerprint)
            .expect("latest histogram was selected from known series");
        let metric =
            metrics
                .entry(metric_name.clone())
                .or_insert_with(|| ActiveNativeHistogramMetric {
                    metric: metric_name.clone(),
                    min_bucket_count: histogram.bucket_count,
                    ..Default::default()
                });
        metric.series_count += 1;
        metric.bucket_count += histogram.bucket_count;
        metric.min_bucket_count = metric.min_bucket_count.min(histogram.bucket_count);
        metric.max_bucket_count = metric.max_bucket_count.max(histogram.bucket_count);
    }
    for metric in metrics.values_mut() {
        metric.avg_bucket_count = metric
            .bucket_count
            .to_f64()
            .expect("bucket count is representable as f64")
            / metric
                .series_count
                .to_f64()
                .expect("series count is representable as f64");
    }
    let response = ActiveNativeHistogramMetricsResponse {
        data: metrics.into_values().collect(),
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            header::HeaderName::from_static("x-mimir-response-streaming-enabled"),
            "true",
        )
        .body(axum::body::Body::from(
            serde_json::to_vec(&response).expect("cardinality response is serializable"),
        ))
        .expect("static cardinality response headers are valid")
}

/// Reads every row of a scan's registered `table`.
async fn table_batches(
    session: &datafusion::prelude::SessionContext,
    table: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>, ApiError> {
    let dataframe = session
        .sql(&format!("SELECT * FROM {table}"))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    dataframe
        .collect()
        .await
        .map_err(|error| ApiError::internal(error.to_string()))
}
