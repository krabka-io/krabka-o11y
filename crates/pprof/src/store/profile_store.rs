use super::{LabelMatcher, ProfileError, ProfileQueryStats, ProfileScan, ProfileStats};

/// Resolves profile matchers to a `DataFusion` samples table over a tenant's data.
#[async_trait::async_trait]
pub trait ProfileStore: Send + Sync {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError>;

    /// Select cold samples with the source offsets represented by that snapshot.
    async fn select_with_source_ranges(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<(ProfileScan, Vec<krabka_blockstore::ProfileWalRange>), ProfileError> {
        Ok((
            self.select(tenant, profile_type, matchers, start_ms, end_ms)
                .await?,
            Vec::new(),
        ))
    }

    /// Select hot samples whose source records are absent from the cold snapshot.
    async fn select_excluding_source_ranges(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        bounds: (i64, i64),
        _ranges: &[krabka_blockstore::ProfileWalRange],
    ) -> Result<ProfileScan, ProfileError> {
        self.select(tenant, profile_type, matchers, bounds.0, bounds.1)
            .await
    }

    async fn query_stats(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileQueryStats, ProfileError>;

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError>;

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError>;
}
