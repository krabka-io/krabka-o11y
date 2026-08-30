use super::{
    Arc, BTreeSet, LabelMatcher, MemTable, ProfileError, ProfileScan, ProfileStats, ProfileStore,
    RecordBatch, SessionContext, UnionSymbols, collect_and_remap, max_option, min_option,
    profile_samples_schema, sorted_union,
};

#[derive(Clone)]
pub struct UnionProfileStore<H, C> {
    pub(crate) hot: Arc<H>,
    pub(crate) cold: Arc<C>,
}

impl<H, C> UnionProfileStore<H, C> {
    #[must_use]
    pub fn new(hot: Arc<H>, cold: Arc<C>) -> Self {
        Self { hot, cold }
    }
}

#[async_trait::async_trait]
impl<H, C> ProfileStore for UnionProfileStore<H, C>
where
    H: ProfileStore,
    C: ProfileStore,
{
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        let hot = self
            .hot
            .select(tenant, profile_type, matchers, start_ms, end_ms)
            .await?;
        let cold = self
            .cold
            .select(tenant, profile_type, matchers, start_ms, end_ms)
            .await?;

        let mut batches = Vec::new();
        let mut symbols = UnionSymbols::default();
        batches.extend(collect_and_remap(hot, 1, &mut symbols).await?);
        batches.extend(collect_and_remap(cold, 2, &mut symbols).await?);
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(profile_samples_schema()));
        }

        let table = MemTable::try_new(profile_samples_schema(), vec![batches])
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let ctx = SessionContext::new();
        let samples_table = "samples".to_string();
        ctx.register_table(&samples_table, Arc::new(table))
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        Ok(ProfileScan {
            ctx,
            samples_table,
            symbols: Arc::new(symbols),
        })
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let hot = self
            .hot
            .label_names(tenant, matchers, start_ms, end_ms)
            .await?;
        let cold = self
            .cold
            .label_names(tenant, matchers, start_ms, end_ms)
            .await?;
        Ok(sorted_union([hot, cold]))
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let hot = self
            .hot
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await?;
        let cold = self
            .cold
            .label_values(tenant, name, matchers, start_ms, end_ms)
            .await?;
        Ok(sorted_union([hot, cold]))
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let hot = self.hot.profile_types(tenant, start_ms, end_ms).await?;
        let cold = self.cold.profile_types(tenant, start_ms, end_ms).await?;
        Ok(sorted_union([hot, cold]))
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        let hot = self
            .hot
            .series(tenant, matchers, label_names, start_ms, end_ms)
            .await?;
        let cold = self
            .cold
            .series(tenant, matchers, label_names, start_ms, end_ms)
            .await?;
        let mut set = BTreeSet::new();
        set.extend(hot);
        set.extend(cold);
        Ok(set.into_iter().collect())
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        let hot = self.hot.stats(tenant, start_ms, end_ms).await?;
        let cold = self.cold.stats(tenant, start_ms, end_ms).await?;
        Ok(ProfileStats {
            data_ingested: hot.data_ingested || cold.data_ingested,
            oldest_profile_time: min_option(hot.oldest_profile_time, cold.oldest_profile_time),
            newest_profile_time: max_option(hot.newest_profile_time, cold.newest_profile_time),
        })
    }
}
