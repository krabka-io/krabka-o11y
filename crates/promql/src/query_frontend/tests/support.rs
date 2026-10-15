//! Shared test helpers for this test area.
use super::*;

pub(crate) mod native_histogram_with_positive_buckets {
    use super::*;

    pub(crate) fn native_histogram_with_positive_buckets(
        count: f64,
        sum: f64,
        positive_spans: Vec<BucketSpan>,
        positive_counts: Vec<f64>,
    ) -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans,
            positive_counts,
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        }
    }
}

pub(crate) mod range_query_plan_skips_unsupported_shards {
    use super::*;

    #[yare::parameterized(
    nested_average = { "avg(sum by (job)(up))" },
    unsupported_quantile = { "quantile(0.9, up)" },
)]
    fn range_query_plan_skips_shards_for_unsupported_reducers(query: &str) {
        let plan = plan_range_query(
            query,
            0,
            60_000,
            millis(60_000),
            QueryFrontendOptions {
                split_interval: millis(120_000),
                shard_count: 3,
            },
        )
        .unwrap();

        assert2::assert!(plan.len() == 1);
        assert2::assert!(plan[0].shard == None);
    }
}

pub(crate) mod recording_executor {
    use super::*;

    #[derive(Default)]
    pub(crate) struct RecordingExecutor {
        pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
    }

    #[async_trait]
    impl RangeQueryExecutor for RecordingExecutor {
        async fn execute_range_query(
            &self,
            _tenant: &TenantId,
            query: &FrontendRangeQuery,
        ) -> Result<AnnotatedQueryResult, PromqlError> {
            self.calls
                .lock()
                .expect("recording executor calls poisoned")
                .push(query.clone());
            Ok(up_api_matrix(&[FloatPoint {
                ts_ms: query.start_ms,
                value: 120_000.0,
            }]))
        }
    }
}
