/// Implements `MetricStore` for `$store` with the given methods, and answers
/// every label, series, exemplar, metadata, cardinality and TSDB lookup the
/// methods leave out with an empty result.
///
/// A macro rather than a fn because it writes trait methods, and it emits the
/// whole `#[async_trait]` impl so that the attribute sees the expanded methods.
macro_rules! metric_store_with_empty_lookups {
    ($store:ty { $($methods:tt)* }) => {
        #[async_trait::async_trait]
        impl $crate::MetricStore for $store {
            $($methods)*

            async fn label_names(
                &self,
                _tenant: &str,
                _matchers: &[$crate::PromqlMatcher],
                _start_ms: i64,
                _end_ms: i64,
            ) -> Result<Vec<String>, $crate::PromqlError> {
                Ok(Vec::new())
            }

            async fn label_values(
                &self,
                _tenant: &str,
                _name: &str,
                _matchers: &[$crate::PromqlMatcher],
                _start_ms: i64,
                _end_ms: i64,
            ) -> Result<Vec<krabka_metrics::MetricString>, $crate::PromqlError> {
                Ok(Vec::new())
            }

            async fn series(
                &self,
                _tenant: &str,
                _matchers: &[$crate::PromqlMatcher],
                _start_ms: i64,
                _end_ms: i64,
            ) -> Result<Vec<$crate::PromqlLabels>, $crate::PromqlError> {
                Ok(Vec::new())
            }

            async fn exemplars(
                &self,
                _tenant: &str,
                _matchers: &[$crate::PromqlMatcher],
                _start_ms: i64,
                _end_ms: i64,
            ) -> Result<$crate::ExemplarScan, $crate::PromqlError> {
                Ok($crate::ExemplarScan::default())
            }

            async fn metadata(
                &self,
                _tenant: &str,
                _metric: Option<&str>,
            ) -> Result<$crate::MetadataScan, $crate::PromqlError> {
                Ok($crate::MetadataScan::default())
            }

            async fn cardinality_label_names(
                &self,
                _tenant: &str,
            ) -> Result<Vec<$crate::LabelNameCardinality>, $crate::PromqlError> {
                Ok(Vec::new())
            }

            async fn cardinality_label_values(
                &self,
                _tenant: &str,
            ) -> Result<Vec<$crate::LabelValueCardinality>, $crate::PromqlError> {
                Ok(Vec::new())
            }

            async fn cardinality_active_series(
                &self,
                _tenant: &str,
            ) -> Result<Vec<$crate::PromqlLabels>, $crate::PromqlError> {
                Ok(Vec::new())
            }

            async fn tsdb_stats(
                &self,
                _tenant: &str,
            ) -> Result<$crate::TsdbStats, $crate::PromqlError> {
                Ok($crate::TsdbStats::empty())
            }

            async fn tsdb_blocks(
                &self,
                _tenant: &str,
            ) -> Result<Vec<$crate::TsdbBlock>, $crate::PromqlError> {
                Ok(Vec::new())
            }
        }
    };
}

pub(crate) use metric_store_with_empty_lookups;
