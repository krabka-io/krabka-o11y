use std::collections::BTreeMap;

use axum::{
    Extension,
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use krabka_blockstore::SeriesFingerprint;
use num_traits::ToPrimitive;
use serde::Serialize;

use super::{
    ApiError, Arc, CardinalityParams, MetricStore, Principal, PrometheusApiState,
    authorized_tenant_from_headers, cardinality_series, decode_native_histograms,
    enforce_selected_series_limit, parse_cardinality_form, parse_cardinality_params,
    selector_matchers,
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

struct LatestHistogram {
    timestamp: i64,
    bucket_count: u64,
}

pub(crate) async fn cardinality_active_native_histogram_metrics<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_native_histogram_metrics_inner(state, headers, principal, params).await
}

pub(crate) async fn cardinality_active_native_histogram_metrics_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_native_histogram_metrics_inner(state, headers, principal, params).await
}

async fn cardinality_active_native_histogram_metrics_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    principal: Principal,
    params: CardinalityParams,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = match cardinality_series(&state, tenant.as_str(), &params).await {
        Ok(series) => series,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = enforce_selected_series_limit(&state, &tenant, series.len()) {
        return error.into_response();
    }
    let metric_by_fingerprint = series
        .into_iter()
        .filter_map(|labels| {
            let metric = labels.get("__name__")?.to_owned();
            Some((labels.fingerprint(), metric))
        })
        .collect::<BTreeMap<_, _>>();

    let matcher_sets = match params.selector.as_deref() {
        Some(selector) => match selector_matchers(selector) {
            Ok(matchers) => matchers,
            Err(error) => return ApiError::from(error).into_response(),
        },
        None => vec![Vec::new()],
    };
    let mut latest = BTreeMap::<SeriesFingerprint, LatestHistogram>::new();
    for matchers in matcher_sets {
        let scan = match state
            .store
            .scan(tenant.as_str(), &matchers, i64::MIN, i64::MAX)
            .await
        {
            Ok(scan) => scan,
            Err(error) => return ApiError::from(error).into_response(),
        };
        let Some(table) = scan.histogram_table else {
            continue;
        };
        let dataframe = match scan.ctx.sql(&format!("SELECT * FROM {table}")).await {
            Ok(dataframe) => dataframe,
            Err(error) => return ApiError::internal(error.to_string()).into_response(),
        };
        let batches = match dataframe.collect().await {
            Ok(batches) => batches,
            Err(error) => return ApiError::internal(error.to_string()).into_response(),
        };
        for batch in batches {
            let decoded = match decode_native_histograms(&batch) {
                Ok(decoded) => decoded,
                Err(error) => return ApiError::internal(error.to_string()).into_response(),
            };
            for (fingerprint, timestamp, histogram) in decoded {
                if !metric_by_fingerprint.contains_key(&fingerprint) {
                    continue;
                }
                let bucket_count = u64::try_from(
                    histogram.positive_counts.len() + histogram.negative_counts.len(),
                )
                .expect("bucket count fits in u64");
                let candidate = LatestHistogram {
                    timestamp,
                    bucket_count,
                };
                if latest
                    .get(&fingerprint)
                    .is_none_or(|current| current.timestamp < timestamp)
                {
                    latest.insert(fingerprint, candidate);
                }
            }
        }
    }

    let mut metrics = BTreeMap::<String, ActiveNativeHistogramMetric>::new();
    for (fingerprint, histogram) in latest {
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
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json"),
            (
                header::HeaderName::from_static("x-mimir-response-streaming-enabled"),
                "true",
            ),
        ],
        serde_json::to_vec(&response).expect("cardinality response is serializable"),
    )
        .into_response()
}
