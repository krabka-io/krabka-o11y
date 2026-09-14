use async_trait::async_trait;
use krabka_query_frontend::{
    CacheKey, ExecutionOptions, InMemoryCache, PlannedQuery, QueryFrontend, QueryFrontendAdapter,
    QueryFrontendError,
};

use super::{
    Arc, BTreeMap, Duration, EngineOpts, FRONTEND_RESULT_CACHE_ENTRIES, FRONTEND_RESULT_CACHE_TTL,
    FlameGraph, FlameGraphDiff, Frame, Heatmap, LabelMatcher, LabeledHeatmap, MatchOp,
    NonZeroUsize, ProfileError, ProfileStore, ProfileType, SampleSelector, Series, SeriesAgg, Time,
    Tree, bin_heatmap, covering_range, diff_trees, fold_bucket, group_frame_name,
    heatmap_points_from_totals, merge_scan_to_tree, series_buckets_from_stacktrace_selector,
    series_buckets_from_totals, tree_to_pprof, tree_to_pprof_with_max_nodes, validate_range,
    validated_step,
};

/// Profiles flamegraph engine.
pub struct FlameEngine<S: ProfileStore> {
    pub(crate) store: Arc<S>,
    pub(crate) opts: EngineOpts,
    tree_frontend: QueryFrontend<InMemoryCache<Tree>>,
    series_frontend: QueryFrontend<InMemoryCache<Vec<Series>>>,
}

fn frontend_options() -> ExecutionOptions {
    ExecutionOptions {
        max_parallelism: NonZeroUsize::new(32).unwrap_or(NonZeroUsize::MIN),
        max_retries: 0,
        max_cache_freshness: Duration::ZERO,
    }
}

impl<S: ProfileStore> FlameEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self {
            store,
            opts,
            tree_frontend: QueryFrontend::new(
                InMemoryCache::new_bounded(
                    FRONTEND_RESULT_CACHE_TTL,
                    NonZeroUsize::new(FRONTEND_RESULT_CACHE_ENTRIES).unwrap_or(NonZeroUsize::MIN),
                ),
                frontend_options(),
            ),
            series_frontend: QueryFrontend::new(
                InMemoryCache::new_bounded(
                    FRONTEND_RESULT_CACHE_TTL,
                    NonZeroUsize::new(FRONTEND_RESULT_CACHE_ENTRIES).unwrap_or(NonZeroUsize::MIN),
                ),
                frontend_options(),
            ),
        }
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                (start_ms, end_ms),
                None,
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_flamegraph(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_grouped(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        range_ms: (i64, i64),
        max_nodes: i64,
        group_by: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        if group_by.is_empty() {
            return self
                .select_merge_stacktraces(
                    tenant,
                    profile_type,
                    label_selector,
                    range_ms.0,
                    range_ms.1,
                    max_nodes,
                )
                .await;
        }
        let base_matchers = crate::matcher::parse_label_selector(label_selector)?;
        let groups = self
            .store
            .series(tenant, &base_matchers, group_by, range_ms.0, range_ms.1)
            .await?;
        let mut tree = Tree::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, range_ms.0, range_ms.1)
                .await?;
            let prefix = vec![Frame {
                function: group_frame_name(&labels),
                file: String::new(),
                line: 0,
            }];
            merge_scan_to_tree(&scan, &mut tree, &prefix, SampleSelector::None, &[]).await?;
        }
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_flamegraph(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        range_ms: (i64, i64),
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        self.select_merge_stacktraces_with_selectors(
            (tenant, profile_type, label_selector),
            range_ms,
            max_nodes,
            call_sites,
            SampleSelector::None,
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_with_selectors(
        &self,
        query: (&str, &str, &str),
        range_ms: (i64, i64),
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<FlameGraph, ProfileError> {
        let (tenant, profile_type, label_selector) = query;
        let tree = self
            .merge_to_tree_with_sample_selector(
                tenant,
                profile_type,
                label_selector,
                range_ms,
                sample_selector,
                call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_flamegraph(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_tree_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        range_ms: (i64, i64),
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        self.select_merge_stacktraces_tree_with_selectors(
            (tenant, profile_type, label_selector),
            range_ms,
            max_nodes,
            call_sites,
            SampleSelector::None,
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_tree_with_selectors(
        &self,
        query: (&str, &str, &str),
        range_ms: (i64, i64),
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<Vec<u8>, ProfileError> {
        let (tenant, profile_type, label_selector) = query;
        let tree = self
            .merge_to_tree_with_sample_selector(
                tenant,
                profile_type,
                label_selector,
                range_ms,
                sample_selector,
                call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_pyroscope_tree_bytes(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[(i64, i64)],
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let merged = self
            .execute_tree_shards(
                tenant,
                profile_type,
                label_selector,
                ranges,
                SampleSelector::None,
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_flamegraph(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_with_stack_trace_selector_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[(i64, i64)],
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        self.select_merge_stacktraces_with_selectors_sharded(
            (tenant, profile_type, label_selector),
            ranges,
            max_nodes,
            call_sites,
            SampleSelector::None,
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_with_selectors_sharded(
        &self,
        query: (&str, &str, &str),
        ranges: &[(i64, i64)],
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<FlameGraph, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let (tenant, profile_type, label_selector) = query;
        let merged = self
            .execute_tree_shards(
                tenant,
                profile_type,
                label_selector,
                ranges,
                sample_selector,
                call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_flamegraph(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_tree_with_stack_trace_selector_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[(i64, i64)],
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        self.select_merge_stacktraces_tree_with_selectors_sharded(
            (tenant, profile_type, label_selector),
            ranges,
            max_nodes,
            call_sites,
            SampleSelector::None,
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_tree_with_selectors_sharded(
        &self,
        query: (&str, &str, &str),
        ranges: &[(i64, i64)],
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<Vec<u8>, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let (tenant, profile_type, label_selector) = query;
        let merged = self
            .execute_tree_shards(
                tenant,
                profile_type,
                label_selector,
                ranges,
                sample_selector,
                call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_pyroscope_tree_bytes(max_nodes))
    }

    async fn execute_tree_shards(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[(i64, i64)],
        sample_selector: SampleSelector<'_>,
        call_sites: &[String],
    ) -> Result<Tree, ProfileError> {
        let adapter = TreeShardAdapter {
            engine: self,
            tenant,
            profile_type,
            label_selector,
            ranges,
            sample_selector,
            call_sites,
        };
        self.tree_frontend
            .execute(&adapter, &())
            .await
            .map_err(frontend_error)
    }

    pub(crate) async fn merge_to_tree(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        range_ms: (i64, i64),
        span_ids: Option<&[u64]>,
        call_sites: &[String],
    ) -> Result<Tree, ProfileError> {
        self.merge_to_tree_with_sample_selector(
            tenant,
            profile_type,
            label_selector,
            range_ms,
            span_ids.map_or(SampleSelector::None, SampleSelector::Span),
            call_sites,
        )
        .await
    }

    pub(crate) async fn merge_to_tree_with_sample_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        range_ms: (i64, i64),
        sample_selector: SampleSelector<'_>,
        call_sites: &[String],
    ) -> Result<Tree, ProfileError> {
        match sample_selector {
            SampleSelector::Span([]) => {
                return Err(ProfileError::Plan(
                    "span selector must contain at least one span id".to_string(),
                ));
            }
            SampleSelector::Trace([]) => {
                return Err(ProfileError::Plan(
                    "trace selector must contain at least one trace id".to_string(),
                ));
            }
            SampleSelector::None | SampleSelector::Span(_) | SampleSelector::Trace(_) => {}
        }
        let matchers = crate::matcher::parse_label_selector(label_selector)?;
        let scan = self
            .store
            .select(tenant, profile_type, &matchers, range_ms.0, range_ms.1)
            .await?;
        let mut tree = Tree::new();
        merge_scan_to_tree(&scan, &mut tree, &[], sample_selector, call_sites).await?;
        Ok(tree)
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_series(
        &self,
        query: (&str, &str, &str),
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        range: (i64, i64),
    ) -> Result<Vec<Series>, ProfileError> {
        self.select_series_with_stack_trace_selector(query, group_by, step, agg, range, &[])
            .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_series_with_stack_trace_selector(
        &self,
        query: (&str, &str, &str),
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        range: (i64, i64),
        call_sites: &[String],
    ) -> Result<Vec<Series>, ProfileError> {
        let (tenant, profile_type, label_selector) = query;
        let (start_ms, end_ms) = range;
        let step = validated_step(step)?;
        let base_matchers = crate::matcher::parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };

        let mut out = Vec::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let buckets = if call_sites.is_empty() {
                series_buckets_from_totals(&scan, step).await?
            } else {
                series_buckets_from_stacktrace_selector(&scan, step, call_sites).await?
            };
            if buckets.is_empty() {
                continue;
            }
            out.push(Series {
                labels,
                points: buckets
                    .into_iter()
                    .map(|(bucket, values)| (bucket, fold_bucket(agg, &values)))
                    .collect(),
            });
        }
        Ok(out)
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_series_sharded(
        &self,
        query: (&str, &str, &str),
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        ranges: &[(i64, i64)],
    ) -> Result<Vec<Series>, ProfileError> {
        self.select_series_with_stack_trace_selector_sharded(
            query,
            group_by,
            step,
            agg,
            ranges,
            &[],
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_series_with_stack_trace_selector_sharded(
        &self,
        query: (&str, &str, &str),
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        ranges: &[(i64, i64)],
        call_sites: &[String],
    ) -> Result<Vec<Series>, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded series query requires at least one time range".to_string(),
            ));
        }
        let (start_ms, end_ms) = covering_range(ranges)?;
        if agg == SeriesAgg::Average {
            return self
                .select_series_with_stack_trace_selector(
                    query,
                    group_by,
                    step,
                    agg,
                    (start_ms, end_ms),
                    call_sites,
                )
                .await;
        }

        let adapter = SeriesShardAdapter {
            engine: self,
            query,
            group_by,
            step,
            agg,
            ranges,
            call_sites,
        };
        self.series_frontend
            .execute(&adapter, &())
            .await
            .map_err(frontend_error)
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn diff(
        &self,
        tenant: &str,
        left: (&str, &str, i64, i64),
        right: (&str, &str, i64, i64),
        max_nodes: i64,
    ) -> Result<FlameGraphDiff, ProfileError> {
        self.diff_with_stack_trace_selector(tenant, left, right, max_nodes, &[], &[])
            .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn diff_with_stack_trace_selector(
        &self,
        tenant: &str,
        left: (&str, &str, i64, i64),
        right: (&str, &str, i64, i64),
        max_nodes: i64,
        left_call_sites: &[String],
        right_call_sites: &[String],
    ) -> Result<FlameGraphDiff, ProfileError> {
        self.diff_with_selectors(
            tenant,
            (left, left_call_sites, SampleSelector::None),
            (right, right_call_sites, SampleSelector::None),
            max_nodes,
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn diff_with_selectors(
        &self,
        tenant: &str,
        left: ((&str, &str, i64, i64), &[String], SampleSelector<'_>),
        right: ((&str, &str, i64, i64), &[String], SampleSelector<'_>),
        max_nodes: i64,
    ) -> Result<FlameGraphDiff, ProfileError> {
        let (left, left_call_sites, left_selector) = left;
        let (right, right_call_sites, right_selector) = right;
        let left_tree = self
            .merge_to_tree_with_sample_selector(
                tenant,
                left.0,
                left.1,
                (left.2, left.3),
                left_selector,
                left_call_sites,
            )
            .await?;
        let right_tree = self
            .merge_to_tree_with_sample_selector(
                tenant,
                right.0,
                right.1,
                (right.2, right.3),
                right_selector,
                right_call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(diff_trees(&left_tree, &right_tree, max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_profile(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        self.select_merge_profile_with_stack_trace_selector(
            tenant,
            profile_type,
            label_selector,
            start_ms,
            end_ms,
            &[],
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_profile_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        let profile_type = ProfileType::parse(profile_type)?;
        let tree = self
            .merge_to_tree(
                tenant,
                &profile_type.to_string(),
                label_selector,
                (start_ms, end_ms),
                None,
                call_sites,
            )
            .await?;
        Ok(tree_to_pprof(&tree, &profile_type).encode())
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_profile_with_max_nodes_and_stack_trace_selector(
        &self,
        query: (&str, &str, &str),
        range: (i64, i64),
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        self.select_merge_profile_with_selectors(
            query,
            range,
            max_nodes,
            call_sites,
            SampleSelector::None,
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_profile_with_selectors(
        &self,
        query: (&str, &str, &str),
        range: (i64, i64),
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<Vec<u8>, ProfileError> {
        let (tenant, profile_type, label_selector) = query;
        let (start_ms, end_ms) = range;
        let profile_type = ProfileType::parse(profile_type)?;
        let tree = self
            .merge_to_tree_with_sample_selector(
                tenant,
                &profile_type.to_string(),
                label_selector,
                (start_ms, end_ms),
                sample_selector,
                call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree_to_pprof_with_max_nodes(&tree, &profile_type, max_nodes).encode())
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_span_profile(
        &self,
        query: (&str, &str, &str),
        span_selector: &[u64],
        range: (i64, i64),
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        let (tenant, profile_type, label_selector) = query;
        let (start_ms, end_ms) = range;
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                (start_ms, end_ms),
                Some(span_selector),
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_flamegraph(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_span_profile_tree(
        &self,
        query: (&str, &str, &str),
        span_selector: &[u64],
        range: (i64, i64),
        max_nodes: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        let (tenant, profile_type, label_selector) = query;
        let (start_ms, end_ms) = range;
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                (start_ms, end_ms),
                Some(span_selector),
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_pyroscope_tree_bytes(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_span_profile_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        span_selector: &[u64],
        ranges: &[(i64, i64)],
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        if matches!(span_selector, []) {
            return Err(ProfileError::Plan(
                "span selector must contain at least one span id".to_string(),
            ));
        }
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded span profile query requires at least one time range".to_string(),
            ));
        }
        let merged = self
            .execute_tree_shards(
                tenant,
                profile_type,
                label_selector,
                ranges,
                SampleSelector::Span(span_selector),
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_flamegraph(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_span_profile_tree_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        span_selector: &[u64],
        ranges: &[(i64, i64)],
        max_nodes: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        if matches!(span_selector, []) {
            return Err(ProfileError::Plan(
                "span selector must contain at least one span id".to_string(),
            ));
        }
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded span profile query requires at least one time range".to_string(),
            ));
        }
        let merged = self
            .execute_tree_shards(
                tenant,
                profile_type,
                label_selector,
                ranges,
                SampleSelector::Span(span_selector),
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_pyroscope_tree_bytes(max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_heatmap(
        &self,
        query: (&str, &str, &str),
        range: (i64, i64),
        time_buckets: usize,
        value_buckets: usize,
    ) -> Result<Heatmap, ProfileError> {
        let (start_ms, end_ms) = range;
        Ok(self
            .select_heatmaps(query, &[], range, time_buckets, value_buckets)
            .await?
            .into_iter()
            .next()
            .map_or_else(
                || bin_heatmap(&[], start_ms, end_ms, time_buckets, value_buckets),
                |item| item.heatmap,
            ))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_heatmaps(
        &self,
        query: (&str, &str, &str),
        group_by: &[String],
        range: (i64, i64),
        time_buckets: usize,
        value_buckets: usize,
    ) -> Result<Vec<LabeledHeatmap>, ProfileError> {
        let (tenant, profile_type, label_selector) = query;
        let (start_ms, end_ms) = range;
        let base_matchers = crate::matcher::parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };

        let mut out = Vec::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let points = heatmap_points_from_totals(&scan).await?;
            if points.is_empty() && !group_by.is_empty() {
                continue;
            }
            out.push(LabeledHeatmap {
                labels,
                heatmap: bin_heatmap(&points, start_ms, end_ms, time_buckets, value_buckets),
            });
        }
        Ok(out)
    }
}

struct TreeShardAdapter<'a, S: ProfileStore> {
    engine: &'a FlameEngine<S>,
    tenant: &'a str,
    profile_type: &'a str,
    label_selector: &'a str,
    ranges: &'a [(i64, i64)],
    sample_selector: SampleSelector<'a>,
    call_sites: &'a [String],
}

#[async_trait]
impl<S: ProfileStore> QueryFrontendAdapter for TreeShardAdapter<'_, S> {
    type Request = ();
    type Query = (i64, i64);
    type Output = Tree;
    type Response = Tree;
    type Error = ProfileError;

    fn plan(&self, (): &()) -> Result<Vec<PlannedQuery<Self::Query>>, Self::Error> {
        self.ranges
            .iter()
            .copied()
            .map(|range| {
                validate_range(range.0, range.1)?;
                Ok(PlannedQuery {
                    query: range,
                    cache_key: CacheKey::new(format!(
                        "profiles-tree\0{}\0{}\0{}\0{}\0{}\0{:?}\0{:?}",
                        self.tenant,
                        self.profile_type,
                        self.label_selector,
                        range.0,
                        range.1,
                        self.sample_selector,
                        self.call_sites,
                    )),
                    end_epoch_millis: range.1,
                })
            })
            .collect()
    }

    async fn execute(&self, range: &Self::Query) -> Result<Self::Output, Self::Error> {
        self.engine
            .merge_to_tree_with_sample_selector(
                self.tenant,
                self.profile_type,
                self.label_selector,
                *range,
                self.sample_selector,
                self.call_sites,
            )
            .await
    }

    fn is_retryable(&self, _error: &Self::Error) -> bool {
        false
    }

    fn merge(&self, (): &(), results: Vec<Self::Output>) -> Result<Self::Response, Self::Error> {
        let mut merged = Tree::new();
        for tree in results {
            merged.merge(&tree);
        }
        Ok(merged)
    }
}

struct SeriesShardAdapter<'a, S: ProfileStore> {
    engine: &'a FlameEngine<S>,
    query: (&'a str, &'a str, &'a str),
    group_by: &'a [String],
    step: Time,
    agg: SeriesAgg,
    ranges: &'a [(i64, i64)],
    call_sites: &'a [String],
}

#[async_trait]
impl<S: ProfileStore> QueryFrontendAdapter for SeriesShardAdapter<'_, S> {
    type Request = ();
    type Query = (i64, i64);
    type Output = Vec<Series>;
    type Response = Vec<Series>;
    type Error = ProfileError;

    fn plan(&self, (): &()) -> Result<Vec<PlannedQuery<Self::Query>>, Self::Error> {
        self.ranges
            .iter()
            .copied()
            .map(|range| {
                validate_range(range.0, range.1)?;
                Ok(PlannedQuery {
                    query: range,
                    cache_key: CacheKey::new(format!(
                        "profiles-series\0{}\0{}\0{}\0{:?}\0{:?}\0{:?}\0{}\0{}\0{:?}",
                        self.query.0,
                        self.query.1,
                        self.query.2,
                        self.group_by,
                        self.step,
                        self.agg,
                        range.0,
                        range.1,
                        self.call_sites,
                    )),
                    end_epoch_millis: range.1,
                })
            })
            .collect()
    }

    async fn execute(&self, range: &Self::Query) -> Result<Self::Output, Self::Error> {
        self.engine
            .select_series_with_stack_trace_selector(
                self.query,
                self.group_by,
                self.step,
                self.agg,
                *range,
                self.call_sites,
            )
            .await
    }

    fn is_retryable(&self, _error: &Self::Error) -> bool {
        false
    }

    fn merge(&self, (): &(), results: Vec<Self::Output>) -> Result<Self::Response, Self::Error> {
        let mut merged: BTreeMap<Vec<(String, String)>, BTreeMap<i64, f64>> = BTreeMap::new();
        for series in results {
            for item in series {
                let points = merged.entry(item.labels).or_default();
                for (timestamp, value) in item.points {
                    *points.entry(timestamp).or_default() += value;
                }
            }
        }
        Ok(merged
            .into_iter()
            .map(|(labels, points)| Series {
                labels,
                points: points.into_iter().collect(),
            })
            .collect())
    }
}

fn frontend_error(
    error: QueryFrontendError<ProfileError, std::convert::Infallible>,
) -> ProfileError {
    match error {
        QueryFrontendError::Adapter(error) => error,
        QueryFrontendError::Cache(never) => match never {},
    }
}
