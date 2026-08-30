//! Data-access seam for the profiles engine.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use krabka_blockstore::LabelMatcher;

use crate::{error::ProfileError, frame::SymbolSource};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use datafusion::prelude::SessionContext;
    use krabka_blockstore::LabelMatcher;

    use super::*;
    use crate::SymbolDb;

    struct Empty;

    #[async_trait::async_trait]
    impl ProfileStore for Empty {
        async fn select(
            &self,
            _tenant: &str,
            _profile_type: &str,
            _matchers: &[LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ProfileScan, crate::ProfileError> {
            Ok(ProfileScan {
                ctx: SessionContext::new(),
                samples_table: "samples".to_string(),
                symbols: Arc::new(SymbolDb::new()),
            })
        }

        async fn label_names(
            &self,
            _tenant: &str,
            _matchers: &[LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, crate::ProfileError> {
            Ok(vec![])
        }

        async fn label_values(
            &self,
            _tenant: &str,
            _name: &str,
            _matchers: &[LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, crate::ProfileError> {
            Ok(vec![])
        }

        async fn profile_types(
            &self,
            _tenant: &str,
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, crate::ProfileError> {
            Ok(vec![])
        }

        async fn series(
            &self,
            _tenant: &str,
            _matchers: &[LabelMatcher],
            _label_names: &[String],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Vec<(String, String)>>, crate::ProfileError> {
            Ok(vec![])
        }

        async fn stats(
            &self,
            _tenant: &str,
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ProfileStats, crate::ProfileError> {
            Ok(ProfileStats::default())
        }
    }

    #[tokio::test]
    async fn trait_is_object_safe() {
        let store: Arc<dyn ProfileStore> = Arc::new(Empty);
        let scan = store
            .select(
                "t",
                "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                &[],
                0,
                1,
            )
            .await
            .unwrap();
        assert!(scan.samples_table == "samples");
        assert!(store.profile_types("t", 0, 1).await.unwrap().is_empty());
    }
}

// === split-modules: generated submodules ===
mod profile_scan;
mod profile_stats;
mod profile_store;

pub use profile_scan::ProfileScan;
pub use profile_stats::ProfileStats;
pub use profile_store::ProfileStore;
