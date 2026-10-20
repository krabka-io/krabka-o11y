//! Measure the production OTLP HTTP route, including partial translation under
//! the tenant accumulator lock, validation, WAL conversion and response encoding.
//! Usage: `metrics_otlp_http_profile` [iterations=5]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use assert2::assert;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use bytes::Bytes;
use krabka_metrics::{
    Limits, WalRecord,
    distributor::{DistributorState, ProduceError, WalSink, router},
};
use krabka_observability::server_security::{ServerSecurity, authenticate_requests};
use krabka_units::{Frequency, convert::FrequencyExt as _};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceResponse,
    metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
        Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum, Summary, SummaryDataPoint, metric, number_data_point,
    },
};
use prost::Message as _;
use tower::ServiceExt as _;

/// Acknowledges converted records without retaining them between iterations.
struct DiscardSink(AtomicUsize);

#[async_trait::async_trait]
impl WalSink for DiscardSink {
    async fn append(&self, _key: Bytes, _record: WalRecord) -> Result<(), ProduceError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn app() -> (Router, Arc<DiscardSink>) {
    let sink = Arc::new(DiscardSink(AtomicUsize::new(0)));
    let state = DistributorState::new(sink.clone()).with_limits(Limits {
        ingestion_rate: Frequency::ZERO,
        ..Limits::default()
    });
    (
        authenticate_requests(router(Arc::new(state)), &ServerSecurity::default()),
        sink,
    )
}

fn fixture(kind: &str, points: usize) -> Vec<u8> {
    let number = || NumberDataPoint {
        time_unix_nano: 2_000_000,
        start_time_unix_nano: 1_000_000,
        value: Some(number_data_point::Value::AsDouble(2.0)),
        ..Default::default()
    };
    let data = match kind {
        "gauge" => metric::Data::Gauge(Gauge {
            data_points: (0..points).map(|_| number()).collect(),
        }),
        "sum" | "delta_sum" => metric::Data::Sum(Sum {
            data_points: (0..points).map(|_| number()).collect(),
            aggregation_temporality: if kind == "delta_sum" {
                AggregationTemporality::Delta as i32
            } else {
                AggregationTemporality::Cumulative as i32
            },
            is_monotonic: true,
        }),
        "histogram" => metric::Data::Histogram(Histogram {
            data_points: (0..points)
                .map(|_| HistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    count: 2,
                    sum: Some(2.0),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                    ..Default::default()
                })
                .collect(),
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        }),
        "exponential_histogram" => metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points: (0..points)
                .map(|_| ExponentialHistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    count: 2,
                    sum: Some(2.0),
                    zero_count: 2,
                    ..Default::default()
                })
                .collect(),
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        }),
        "summary" => metric::Data::Summary(Summary {
            data_points: (0..points)
                .map(|_| SummaryDataPoint {
                    time_unix_nano: 2_000_000,
                    count: 2,
                    sum: 2.0,
                    ..Default::default()
                })
                .collect(),
        }),
        _ => panic!("unknown fixture kind"),
    };
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "test.metric".into(),
                    description: "HTTP decode benchmark".into(),
                    data: Some(data),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

async fn push(app: Router, body: Bytes) {
    let request = Request::builder()
        .method("POST")
        .uri("/otlp/v1/metrics")
        .header("Content-Type", "application/x-protobuf")
        .header("X-Scope-OrgID", "benchmark")
        .body(Body::from(body))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert!(response.status() == StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let result = ExportMetricsServiceResponse::decode(bytes).unwrap();
    assert!(result.partial_success.is_none());
}

fn main() {
    let iterations = std::env::args()
        .nth(1)
        .map_or(5, |v| v.parse::<u32>().expect("iteration count"));
    assert!(iterations > 0);
    // Allocation counting is thread-local. Run every future on this thread.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    println!("kind,points,http_mean_us,allocations,allocated_bytes,peak_bytes");
    for kind in [
        "gauge",
        "sum",
        "delta_sum",
        "histogram",
        "exponential_histogram",
        "summary",
    ] {
        for points in [100, 1_000, 10_000] {
            let (app, sink) = app();
            let body = Bytes::from(fixture(kind, points));
            allocation_counter::opt_out(|| {
                runtime.block_on(push(app.clone(), body.clone()));
            });
            let records_per_point = match kind {
                "histogram" => 8, // Two buckets, count and sum, each with metadata.
                "summary" => 4,   // Count and sum, each with metadata.
                _ => 2,           // One sample or native histogram and its metadata.
            };
            assert!(sink.0.load(Ordering::Relaxed) == records_per_point * points);
            let started = Instant::now();
            allocation_counter::opt_out(|| {
                for _ in 0..iterations {
                    runtime.block_on(push(app.clone(), body.clone()));
                }
            });
            let mean_us = started.elapsed().as_secs_f64() * 1e6 / f64::from(iterations);
            // Separate from timing, as allocation accounting adds overhead.
            let allocations = allocation_counter::measure(|| {
                runtime.block_on(push(app.clone(), body.clone()));
            });
            println!(
                "{kind},{points},{mean_us:.2},{},{},{}",
                allocations.count_total, allocations.bytes_total, allocations.bytes_max
            );
        }
    }
}
