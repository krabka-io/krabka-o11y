use async_trait::async_trait;
use krabka_query_frontend::{
    AdmissionLimits, CacheKey, CacheMetrics, ExecutionOptions, InMemoryCache, PlannedQuery,
    QueryFrontend, QueryFrontendAdapter, QueryFrontendError,
};
use krabka_units::{convert::TimeExt as _, millis};

use super::{
    Arc, BTreeMap, Duration, EngineOpts, FRONTEND_RESULT_CACHE_ENTRIES, FRONTEND_RESULT_CACHE_TTL,
    FlameGraph, FlameGraphDiff, Frame, Heatmap, LabelMatcher, LabeledHeatmap, MatchOp,
    NonZeroUsize, ProfileError, ProfileStore, ProfileType, SampleSelector, ScanMerge, Series,
    SeriesAgg, Time, Tree, bin_heatmap, covering_range, diff_trees, fold_bucket, group_frame_name,
    heatmap_points_from_totals, merge_scan_to_pprof, merge_scan_to_tree,
    series_buckets_from_stacktrace_selector, series_buckets_from_totals, validate_range,
    validated_step,
};
use crate::{ProfileScan, series_bucket_ms};

/// The profiles an engine query selects: one tenant's profiles of one type
/// whose labels match `label_selector`.
#[derive(Clone, Copy, Debug)]
pub struct ProfileSelection<'q> {
    pub tenant: &'q str,
    pub profile_type: &'q str,
    pub label_selector: &'q str,
}

impl<'q> ProfileSelection<'q> {
    #[must_use]
    pub const fn new(tenant: &'q str, profile_type: &'q str, label_selector: &'q str) -> Self {
        Self {
            tenant,
            profile_type,
            label_selector,
        }
    }
}

/// An inclusive time range in Unix milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MillisRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl MillisRange {
    #[must_use]
    pub const fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }
}

/// How many buckets a heatmap bins its points into on each axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeatmapGrid {
    /// Buckets along the time axis.
    pub time_buckets: usize,
    /// Buckets along the value axis.
    pub value_buckets: usize,
}

/// One heatmap query: the profiles it selects, the range it covers, and the
/// grid it bins them into.
#[derive(Clone, Copy, Debug)]
pub struct HeatmapQuery<'q> {
    pub selection: ProfileSelection<'q>,
    pub range: MillisRange,
    pub grid: HeatmapGrid,
}

/// One `group_by` group of a query: its label values, and the matchers that
/// select only its profiles.
struct GroupSelection {
    labels: Vec<crate::ProfileLabel>,
    matchers: Vec<LabelMatcher>,
}

#[derive(Clone, Copy, Debug)]
struct SeriesSampling {
    step: Time,
    aggregation: SeriesAgg,
}

#[derive(Clone, Debug)]
struct MergedSpanProfile {
    tree: Tree,
    max_nodes: i64,
}

/// A span-profile merge over several time-range shards.
#[derive(Clone, Copy, Debug)]
pub struct SpanProfileShards<'q> {
    pub selection: ProfileSelection<'q>,
    /// Keeps only samples of these span ids; must not be empty.
    pub span_selector: &'q [u64],
    /// Inclusive `(start_ms, end_ms)` shards, in Unix milliseconds.
    pub ranges: &'q [MillisRange],
    /// Non-positive values resolve to the engine default.
    pub max_nodes: i64,
}

/// One side of a profile diff query, including selection, range, and filters.
#[derive(Clone, Copy, Debug)]
pub struct DiffQuery<'q> {
    pub selection: ProfileSelection<'q>,
    pub range: MillisRange,
    pub call_sites: &'q [String],
    pub sample_selector: SampleSelector<'q>,
}

impl<'q> DiffQuery<'q> {
    #[must_use]
    pub const fn new(selection: ProfileSelection<'q>, range: MillisRange) -> Self {
        Self {
            selection,
            range,
            call_sites: &[],
            sample_selector: SampleSelector::None,
        }
    }
}

/// A merge of the samples that `sample_selector` and `call_sites` keep, from
/// the profiles `selection` picks over `range`.
#[derive(Clone, Copy)]
struct ProfileMerge<'q> {
    selection: ProfileSelection<'q>,
    range: MillisRange,
    sample_selector: SampleSelector<'q>,
    /// Keeps only stacks that match these call sites; empty keeps every stack.
    call_sites: &'q [String],
}

impl<'q> ProfileMerge<'q> {
    fn new(
        selection: ProfileSelection<'q>,
        range_ms: MillisRange,
        sample_selector: SampleSelector<'q>,
        call_sites: &'q [String],
    ) -> Self {
        Self {
            selection,
            range: MillisRange {
                start_ms: range_ms.start_ms,
                end_ms: range_ms.end_ms,
            },
            sample_selector,
            call_sites,
        }
    }
}

/// Profiles flamegraph engine.
pub struct FlameEngine<S: ProfileStore> {
    pub(crate) store: Arc<S>,
    pub(crate) opts: EngineOpts,
    tree_frontend: QueryFrontend<InMemoryCache<Tree>>,
    series_frontend: QueryFrontend<InMemoryCache<Vec<Series>>>,
    admission_limits: Arc<dyn Fn(&str) -> AdmissionLimits + Send + Sync>,
    cache_metrics: Arc<CacheMetrics>,
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
        let cache_metrics = Arc::new(CacheMetrics::default());
        Self {
            store,
            opts,
            tree_frontend: QueryFrontend::new(
                InMemoryCache::new_bounded(
                    FRONTEND_RESULT_CACHE_TTL,
                    NonZeroUsize::new(FRONTEND_RESULT_CACHE_ENTRIES).unwrap_or(NonZeroUsize::MIN),
                )
                .with_weigher(Tree::estimated_bytes)
                .with_metrics(Arc::clone(&cache_metrics)),
                frontend_options(),
            ),
            series_frontend: QueryFrontend::new(
                InMemoryCache::new_bounded(
                    FRONTEND_RESULT_CACHE_TTL,
                    NonZeroUsize::new(FRONTEND_RESULT_CACHE_ENTRIES).unwrap_or(NonZeroUsize::MIN),
                )
                .with_weigher(series_bytes)
                .with_metrics(Arc::clone(&cache_metrics)),
                frontend_options(),
            ),
            admission_limits: Arc::new(|_| AdmissionLimits::default()),
            cache_metrics,
        }
    }

    #[must_use]
    pub fn cache_metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.cache_metrics)
    }

    /// `max_nodes`, or the engine default when it is not positive.
    const fn max_nodes_or_default(&self, max_nodes: i64) -> i64 {
        if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        }
    }

    #[must_use]
    pub fn with_admission_limits(
        mut self,
        resolve: impl Fn(&str) -> AdmissionLimits + Send + Sync + 'static,
    ) -> Self {
        self.admission_limits = Arc::new(resolve);
        self
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
                MillisRange { start_ms, end_ms },
                None,
                &[],
            )
            .await?;
        Ok(tree.to_flamegraph(self.max_nodes_or_default(max_nodes)))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_grouped(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        range_ms: MillisRange,
        max_nodes: i64,
        group_by: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        if group_by.is_empty() {
            return self
                .select_merge_stacktraces(
                    tenant,
                    profile_type,
                    label_selector,
                    range_ms.start_ms,
                    range_ms.end_ms,
                    max_nodes,
                )
                .await;
        }
        let base_matchers = crate::matcher::parse_label_selector(label_selector)?;
        let groups = self
            .store
            .series(
                tenant,
                &base_matchers,
                group_by,
                range_ms.start_ms,
                range_ms.end_ms,
            )
            .await?;
        let mut tree = Tree::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(labels.iter().map(|label| {
                LabelMatcher::new(label.name.clone(), MatchOp::Eq, label.value.clone())
            }));
            let scan = self
                .store
                .select(
                    tenant,
                    profile_type,
                    &matchers,
                    range_ms.start_ms,
                    range_ms.end_ms,
                )
                .await?;
            let prefix = vec![Frame {
                function: group_frame_name(&labels),
                file: String::new(),
                line: 0,
            }];
            merge_scan_to_tree(
                ScanMerge {
                    scan: &scan,
                    sample_selector: SampleSelector::None,
                    call_sites: &[],
                },
                &mut tree,
                &prefix,
            )
            .await?;
        }
        Ok(tree.to_flamegraph(self.max_nodes_or_default(max_nodes)))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        range_ms: MillisRange,
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        self.select_merge_stacktraces_with_selectors(
            ProfileSelection::new(tenant, profile_type, label_selector),
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
        query: ProfileSelection<'_>,
        range_ms: MillisRange,
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<FlameGraph, ProfileError> {
        let ProfileSelection {
            tenant,
            profile_type,
            label_selector,
        } = query;
        let tree = self
            .merge_to_tree_with_sample_selector(ProfileMerge::new(
                ProfileSelection {
                    tenant,
                    profile_type,
                    label_selector,
                },
                range_ms,
                sample_selector,
                call_sites,
            ))
            .await?;
        Ok(tree.to_flamegraph(self.max_nodes_or_default(max_nodes)))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_tree_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        range_ms: MillisRange,
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        self.select_merge_stacktraces_tree_with_selectors(
            ProfileSelection::new(tenant, profile_type, label_selector),
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
        query: ProfileSelection<'_>,
        range_ms: MillisRange,
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<Vec<u8>, ProfileError> {
        let ProfileSelection {
            tenant,
            profile_type,
            label_selector,
        } = query;
        let tree = self
            .merge_to_tree_with_sample_selector(ProfileMerge::new(
                ProfileSelection {
                    tenant,
                    profile_type,
                    label_selector,
                },
                range_ms,
                sample_selector,
                call_sites,
            ))
            .await?;
        Ok(tree.to_pyroscope_tree_bytes(self.max_nodes_or_default(max_nodes)))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[MillisRange],
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        let merged = self
            .execute_tree_shards_for_query(
                ProfileSelection::new(tenant, profile_type, label_selector),
                ranges,
                SampleSelector::None,
                &[],
            )
            .await?;
        Ok(merged.to_flamegraph(self.max_nodes_or_default(max_nodes)))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_with_stack_trace_selector_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[MillisRange],
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        self.select_merge_stacktraces_with_selectors_sharded(
            ProfileSelection::new(tenant, profile_type, label_selector),
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
        query: ProfileSelection<'_>,
        ranges: &[MillisRange],
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<FlameGraph, ProfileError> {
        let merged = self
            .execute_tree_shards_for_query(query, ranges, sample_selector, call_sites)
            .await?;
        Ok(merged.to_flamegraph(self.max_nodes_or_default(max_nodes)))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_stacktraces_tree_with_stack_trace_selector_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[MillisRange],
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        self.select_merge_stacktraces_tree_with_selectors_sharded(
            ProfileSelection::new(tenant, profile_type, label_selector),
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
        query: ProfileSelection<'_>,
        ranges: &[MillisRange],
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<Vec<u8>, ProfileError> {
        let merged = self
            .execute_tree_shards_for_query(query, ranges, sample_selector, call_sites)
            .await?;
        Ok(merged.to_pyroscope_tree_bytes(self.max_nodes_or_default(max_nodes)))
    }

    async fn execute_tree_shards_for_query(
        &self,
        query: ProfileSelection<'_>,
        ranges: &[MillisRange],
        sample_selector: SampleSelector<'_>,
        call_sites: &[String],
    ) -> Result<Tree, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let ProfileSelection {
            tenant,
            profile_type,
            label_selector,
        } = query;
        self.execute_tree_shards(
            tenant,
            profile_type,
            label_selector,
            ranges,
            sample_selector,
            call_sites,
        )
        .await
    }

    async fn execute_tree_shards(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[MillisRange],
        sample_selector: SampleSelector<'_>,
        call_sites: &[String],
    ) -> Result<Tree, ProfileError> {
        let adapter = ShardAdapter {
            query: TreeShards {
                engine: self,
                tenant,
                profile_type,
                label_selector,
                sample_selector,
                call_sites,
            },
            ranges,
            admission_limits: (self.admission_limits)(tenant),
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
        range_ms: MillisRange,
        span_ids: Option<&[u64]>,
        call_sites: &[String],
    ) -> Result<Tree, ProfileError> {
        self.merge_to_tree_with_sample_selector(ProfileMerge {
            selection: ProfileSelection {
                tenant,
                profile_type,
                label_selector,
            },
            range: MillisRange {
                start_ms: range_ms.start_ms,
                end_ms: range_ms.end_ms,
            },
            sample_selector: span_ids.map_or(SampleSelector::None, SampleSelector::Span),
            call_sites,
        })
        .await
    }

    async fn merge_to_tree_with_sample_selector(
        &self,
        merge: ProfileMerge<'_>,
    ) -> Result<Tree, ProfileError> {
        let scan = self
            .select_for_sample_selector(merge.selection, merge.range, merge.sample_selector)
            .await?;
        let mut tree = Tree::new();
        merge_scan_to_tree(
            ScanMerge {
                scan: &scan,
                sample_selector: merge.sample_selector,
                call_sites: merge.call_sites,
            },
            &mut tree,
            &[],
        )
        .await?;
        Ok(tree)
    }

    /// Rejects an empty span or trace selector, then scans the profiles the
    /// label selector matches over `range`.
    async fn select_for_sample_selector(
        &self,
        selection: ProfileSelection<'_>,
        range: MillisRange,
        sample_selector: SampleSelector<'_>,
    ) -> Result<ProfileScan, ProfileError> {
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
        let matchers = crate::matcher::parse_label_selector(selection.label_selector)?;
        self.store
            .select(
                selection.tenant,
                selection.profile_type,
                &matchers,
                range.start_ms,
                range.end_ms,
            )
            .await
    }

    /// Merges into a pprof profile of `profile_type`, which
    /// `merge.selection` names in its string form.
    async fn merge_to_pprof(
        &self,
        merge: ProfileMerge<'_>,
        profile_type: &ProfileType,
        max_nodes: i64,
    ) -> Result<crate::PprofProfile, ProfileError> {
        let scan = self
            .select_for_sample_selector(merge.selection, merge.range, merge.sample_selector)
            .await?;
        merge_scan_to_pprof(
            ScanMerge {
                scan: &scan,
                sample_selector: merge.sample_selector,
                call_sites: merge.call_sites,
            },
            profile_type,
            max_nodes,
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_series(
        &self,
        query: ProfileSelection<'_>,
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        range: MillisRange,
    ) -> Result<Vec<Series>, ProfileError> {
        self.select_series_with_stack_trace_selector(query, group_by, step, agg, range, &[])
            .await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_series_with_stack_trace_selector(
        &self,
        query: ProfileSelection<'_>,
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        range: MillisRange,
        call_sites: &[String],
    ) -> Result<Vec<Series>, ProfileError> {
        self.select_series_with_anchor(
            query,
            group_by,
            SeriesSampling {
                step,
                aggregation: agg,
            },
            range,
            call_sites,
            range,
        )
        .await
    }

    /// The `group_by` groups that `selection` matches over `range`, each with
    /// the matchers that select only its profiles. With no `group_by` this is
    /// one ungrouped selection.
    async fn group_selections(
        &self,
        selection: ProfileSelection<'_>,
        group_by: &[String],
        range: MillisRange,
    ) -> Result<Vec<GroupSelection>, ProfileError> {
        let base_matchers = crate::matcher::parse_label_selector(selection.label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(
                    selection.tenant,
                    &base_matchers,
                    group_by,
                    range.start_ms,
                    range.end_ms,
                )
                .await?
        };
        Ok(groups
            .into_iter()
            .map(|labels| {
                let mut matchers = base_matchers.clone();
                matchers.extend(labels.iter().map(|label| {
                    LabelMatcher::new(label.name.clone(), MatchOp::Eq, label.value.clone())
                }));
                GroupSelection { labels, matchers }
            })
            .collect())
    }

    async fn select_series_with_anchor(
        &self,
        query: ProfileSelection<'_>,
        group_by: &[String],
        sampling: SeriesSampling,
        range: MillisRange,
        call_sites: &[String],
        anchor: MillisRange,
    ) -> Result<Vec<Series>, ProfileError> {
        let ProfileSelection {
            tenant,
            profile_type,
            label_selector,
        } = query;
        let MillisRange { start_ms, end_ms } = range;
        let SeriesSampling {
            step,
            aggregation: agg,
        } = sampling;
        let step = validated_step(step)?;
        validate_range(start_ms, end_ms)?;
        validate_range(anchor.start_ms, anchor.end_ms)?;
        // The first output point covers the one-step lookback through start.
        // Extend only the first shard so each profile is scanned once.
        let scan_start = if start_ms == anchor.start_ms {
            start_ms.saturating_sub(step.millis_i64())
        } else {
            start_ms
        };
        let groups = self
            .group_selections(
                ProfileSelection {
                    tenant,
                    profile_type,
                    label_selector,
                },
                group_by,
                MillisRange {
                    start_ms: scan_start,
                    end_ms,
                },
            )
            .await?;

        let mut out = Vec::new();
        for GroupSelection { labels, matchers } in groups {
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, scan_start, end_ms)
                .await?;
            // Retain raw per-profile timestamps and values until the global
            // query anchor is known. Epoch-flooring first loses bucket edges
            // and averaging per-timestamp means would lose profile weights.
            let raw_points = if call_sites.is_empty() {
                series_buckets_from_totals(&scan, millis(1)).await?
            } else {
                series_buckets_from_stacktrace_selector(&scan, millis(1), call_sites).await?
            };
            let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
            for point in raw_points {
                if let Some(endpoint) = series_bucket_ms(point.timestamp_ms, step, anchor) {
                    buckets.entry(endpoint).or_default().extend(point.values);
                }
            }
            if buckets.is_empty() {
                continue;
            }
            let bucket_endpoints = buckets.keys().copied().collect::<Vec<_>>();
            let mut points = Vec::with_capacity(bucket_endpoints.len());
            for bucket in bucket_endpoints {
                if let Some(values) = buckets.remove(&bucket) {
                    points.push(crate::SamplePoint {
                        timestamp_ms: bucket,
                        value: fold_bucket(agg, &values),
                    });
                }
            }
            out.push(Series { labels, points });
        }
        Ok(out)
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_series_sharded(
        &self,
        query: ProfileSelection<'_>,
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        ranges: &[MillisRange],
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
        query: ProfileSelection<'_>,
        group_by: &[String],
        step: Time,
        agg: SeriesAgg,
        ranges: &[MillisRange],
        call_sites: &[String],
    ) -> Result<Vec<Series>, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded series query requires at least one time range".to_string(),
            ));
        }
        let MillisRange { start_ms, end_ms } = covering_range(ranges)?;
        if agg == SeriesAgg::Average {
            return self
                .select_series_with_stack_trace_selector(
                    query,
                    group_by,
                    step,
                    agg,
                    MillisRange { start_ms, end_ms },
                    call_sites,
                )
                .await;
        }

        let adapter = ShardAdapter {
            query: SeriesShards {
                engine: self,
                query,
                group_by,
                step,
                agg,
                anchor: MillisRange { start_ms, end_ms },
                call_sites,
            },
            ranges,
            admission_limits: (self.admission_limits)(query.tenant),
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
        left: DiffQuery<'_>,
        right: DiffQuery<'_>,
        max_nodes: i64,
    ) -> Result<FlameGraphDiff, ProfileError> {
        self.diff_with_selectors(left, right, max_nodes).await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn diff_with_stack_trace_selector(
        &self,
        left: DiffQuery<'_>,
        right: DiffQuery<'_>,
        max_nodes: i64,
    ) -> Result<FlameGraphDiff, ProfileError> {
        self.diff_with_selectors(left, right, max_nodes).await
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn diff_with_selectors(
        &self,
        left: DiffQuery<'_>,
        right: DiffQuery<'_>,
        max_nodes: i64,
    ) -> Result<FlameGraphDiff, ProfileError> {
        let left_tree = self
            .merge_to_tree_with_sample_selector(ProfileMerge {
                selection: left.selection,
                range: left.range,
                sample_selector: left.sample_selector,
                call_sites: left.call_sites,
            })
            .await?;
        let right_tree = self
            .merge_to_tree_with_sample_selector(ProfileMerge {
                selection: right.selection,
                range: right.range,
                sample_selector: right.sample_selector,
                call_sites: right.call_sites,
            })
            .await?;
        let max_nodes = self.max_nodes_or_default(max_nodes);
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
        let profile = self
            .merge_to_pprof(
                ProfileMerge {
                    selection: ProfileSelection {
                        tenant,
                        profile_type: &profile_type.to_string(),
                        label_selector,
                    },
                    range: MillisRange { start_ms, end_ms },
                    sample_selector: SampleSelector::None,
                    call_sites,
                },
                &profile_type,
                i64::MAX,
            )
            .await?;
        Ok(profile.encode())
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_profile_with_max_nodes_and_stack_trace_selector(
        &self,
        query: ProfileSelection<'_>,
        range: MillisRange,
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
        query: ProfileSelection<'_>,
        range: MillisRange,
        max_nodes: i64,
        call_sites: &[String],
        sample_selector: SampleSelector<'_>,
    ) -> Result<Vec<u8>, ProfileError> {
        let ProfileSelection {
            tenant,
            profile_type,
            label_selector,
        } = query;
        let MillisRange { start_ms, end_ms } = range;
        let profile_type = ProfileType::parse(profile_type)?;
        let max_nodes = self.max_nodes_or_default(max_nodes);
        let profile = self
            .merge_to_pprof(
                ProfileMerge {
                    selection: ProfileSelection {
                        tenant,
                        profile_type: &profile_type.to_string(),
                        label_selector,
                    },
                    range: MillisRange { start_ms, end_ms },
                    sample_selector,
                    call_sites,
                },
                &profile_type,
                max_nodes,
            )
            .await?;
        Ok(profile.encode())
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_span_profile(
        &self,
        query: ProfileSelection<'_>,
        span_selector: &[u64],
        range: MillisRange,
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        let ProfileSelection {
            tenant,
            profile_type,
            label_selector,
        } = query;
        let MillisRange { start_ms, end_ms } = range;
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                MillisRange { start_ms, end_ms },
                Some(span_selector),
                &[],
            )
            .await?;
        Ok(tree.to_flamegraph(self.max_nodes_or_default(max_nodes)))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_span_profile_tree(
        &self,
        query: ProfileSelection<'_>,
        span_selector: &[u64],
        range: MillisRange,
        max_nodes: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        let ProfileSelection {
            tenant,
            profile_type,
            label_selector,
        } = query;
        let MillisRange { start_ms, end_ms } = range;
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                MillisRange { start_ms, end_ms },
                Some(span_selector),
                &[],
            )
            .await?;
        Ok(tree.to_pyroscope_tree_bytes(self.max_nodes_or_default(max_nodes)))
    }

    /// Merges the span profile over every range shard, and resolves a
    /// non-positive `max_nodes` to the engine default.
    async fn merge_span_profile_shards(
        &self,
        shards: SpanProfileShards<'_>,
    ) -> Result<MergedSpanProfile, ProfileError> {
        let SpanProfileShards {
            selection,
            span_selector,
            ranges,
            max_nodes,
        } = shards;
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
                selection.tenant,
                selection.profile_type,
                selection.label_selector,
                ranges,
                SampleSelector::Span(span_selector),
                &[],
            )
            .await?;
        let max_nodes = self.max_nodes_or_default(max_nodes);
        Ok(MergedSpanProfile {
            tree: merged,
            max_nodes,
        })
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_span_profile_sharded(
        &self,
        shards: SpanProfileShards<'_>,
    ) -> Result<FlameGraph, ProfileError> {
        let merged = self.merge_span_profile_shards(shards).await?;
        Ok(merged.tree.to_flamegraph(merged.max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_merge_span_profile_tree_sharded(
        &self,
        shards: SpanProfileShards<'_>,
    ) -> Result<Vec<u8>, ProfileError> {
        let merged = self.merge_span_profile_shards(shards).await?;
        Ok(merged.tree.to_pyroscope_tree_bytes(merged.max_nodes))
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub async fn select_heatmap(&self, query: HeatmapQuery<'_>) -> Result<Heatmap, ProfileError> {
        let MillisRange { start_ms, end_ms } = query.range;
        let HeatmapGrid {
            time_buckets,
            value_buckets,
        } = query.grid;
        Ok(self
            .select_heatmaps(query, &[])
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
        query: HeatmapQuery<'_>,
        group_by: &[String],
    ) -> Result<Vec<LabeledHeatmap>, ProfileError> {
        let HeatmapQuery {
            selection,
            range,
            grid:
                HeatmapGrid {
                    time_buckets,
                    value_buckets,
                },
        } = query;
        let points = self
            .select_heatmap_points(selection, group_by, range)
            .await?;
        Ok(points
            .into_iter()
            .map(|group| LabeledHeatmap {
                labels: group.labels,
                heatmap: bin_heatmap(
                    &group.points,
                    range.start_ms,
                    range.end_ms,
                    time_buckets,
                    value_buckets,
                ),
            })
            .collect())
    }

    /// Returns profile totals before binning so callers can use shared value
    /// boundaries and an explicit query resolution.
    ///
    /// # Errors
    /// Returns an error for invalid selectors or failed profile scans.
    pub async fn select_heatmap_points(
        &self,
        selection: ProfileSelection<'_>,
        group_by: &[String],
        range: MillisRange,
    ) -> Result<Vec<crate::LabeledHeatmapPoints>, ProfileError> {
        let ProfileSelection {
            tenant,
            profile_type,
            ..
        } = selection;
        let MillisRange { start_ms, end_ms } = range;
        let groups = self.group_selections(selection, group_by, range).await?;

        let mut out = Vec::new();
        for GroupSelection { labels, matchers } in groups {
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let points = heatmap_points_from_totals(&scan).await?;
            if points.is_empty() && !group_by.is_empty() {
                continue;
            }
            out.push(crate::LabeledHeatmapPoints { labels, points });
        }
        Ok(out)
    }
}

fn series_bytes(series: &Vec<Series>) -> usize {
    std::mem::size_of::<Vec<Series>>()
        + series.capacity() * std::mem::size_of::<Series>()
        + series
            .iter()
            .map(|series| {
                series.points.capacity() * std::mem::size_of::<crate::SamplePoint>()
                    + series.labels.capacity() * std::mem::size_of::<crate::ProfileLabel>()
                    + series
                        .labels
                        .iter()
                        .map(|label| label.name.capacity() + label.value.capacity())
                        .sum::<usize>()
            })
            .sum::<usize>()
}

/// One kind of sharded engine query: how a shard is keyed in the result
/// cache, run, and folded into the response.
#[async_trait]
trait ShardedQuery: Sync {
    type Output: Clone + Send + Sync;

    /// The tenant whose cache namespace the shards are keyed under.
    fn tenant(&self) -> &str;

    /// The cache key text of the shard over `range`, which has to name every
    /// input the shard's result depends on.
    fn shard_cache_key(&self, range: MillisRange) -> String;

    async fn execute_shard(&self, range: MillisRange) -> Result<Self::Output, ProfileError>;

    fn merge_shards(&self, results: Vec<Self::Output>) -> Self::Output;
}

/// Runs a [`ShardedQuery`] through the query frontend, one planned query per
/// inclusive `(start_ms, end_ms)` shard.
struct ShardAdapter<'a, Q> {
    query: Q,
    ranges: &'a [MillisRange],
    admission_limits: AdmissionLimits,
}

#[async_trait]
impl<Q: ShardedQuery> QueryFrontendAdapter for ShardAdapter<'_, Q> {
    type Request = ();
    type Query = MillisRange;
    type Output = Q::Output;
    type Response = Q::Output;
    type Error = ProfileError;

    fn plan(&self, (): &()) -> Result<Vec<PlannedQuery<Self::Query>>, Self::Error> {
        self.ranges
            .iter()
            .copied()
            .map(|MillisRange { start_ms, end_ms }| {
                validate_range(start_ms, end_ms)?;
                let range = MillisRange { start_ms, end_ms };
                Ok(PlannedQuery {
                    query: range,
                    cache_key: CacheKey::new(
                        self.query.tenant(),
                        self.query.shard_cache_key(range),
                    ),
                    end_epoch_millis: end_ms,
                    estimated_bytes: 0,
                })
            })
            .collect()
    }

    async fn execute(&self, range: &Self::Query) -> Result<Self::Output, Self::Error> {
        self.query.execute_shard(*range).await
    }

    fn is_retryable(&self, _error: &Self::Error) -> bool {
        false
    }

    fn admission_limits(&self, (): &()) -> Option<AdmissionLimits> {
        Some(self.admission_limits)
    }

    fn merge(&self, (): &(), results: Vec<Self::Output>) -> Result<Self::Response, Self::Error> {
        Ok(self.query.merge_shards(results))
    }
}

struct TreeShards<'a, S: ProfileStore> {
    engine: &'a FlameEngine<S>,
    tenant: &'a str,
    profile_type: &'a str,
    label_selector: &'a str,
    sample_selector: SampleSelector<'a>,
    call_sites: &'a [String],
}

#[async_trait]
impl<S: ProfileStore> ShardedQuery for TreeShards<'_, S> {
    type Output = Tree;

    fn tenant(&self) -> &str {
        self.tenant
    }

    fn shard_cache_key(&self, range: MillisRange) -> String {
        format!(
            "profiles-tree\0{}\0{}\0{}\0{}\0{:?}\0{:?}",
            self.profile_type,
            self.label_selector,
            range.start_ms,
            range.end_ms,
            self.sample_selector,
            self.call_sites,
        )
    }

    async fn execute_shard(&self, range: MillisRange) -> Result<Tree, ProfileError> {
        self.engine
            .merge_to_tree_with_sample_selector(ProfileMerge {
                selection: ProfileSelection {
                    tenant: self.tenant,
                    profile_type: self.profile_type,
                    label_selector: self.label_selector,
                },
                range,
                sample_selector: self.sample_selector,
                call_sites: self.call_sites,
            })
            .await
    }

    fn merge_shards(&self, results: Vec<Tree>) -> Tree {
        let mut merged = Tree::new();
        for tree in results {
            merged.merge(&tree);
        }
        merged
    }
}

struct SeriesShards<'a, S: ProfileStore> {
    engine: &'a FlameEngine<S>,
    query: ProfileSelection<'a>,
    group_by: &'a [String],
    step: Time,
    agg: SeriesAgg,
    anchor: MillisRange,
    call_sites: &'a [String],
}

#[async_trait]
impl<S: ProfileStore> ShardedQuery for SeriesShards<'_, S> {
    type Output = Vec<Series>;

    fn tenant(&self) -> &str {
        self.query.tenant
    }

    fn shard_cache_key(&self, range: MillisRange) -> String {
        format!(
            "profiles-series\0{}\0{}\0{:?}\0{:?}\0{:?}\0{}\0{}\0{:?}\0{:?}",
            self.query.profile_type,
            self.query.label_selector,
            self.group_by,
            self.step,
            self.agg,
            range.start_ms,
            range.end_ms,
            self.call_sites,
            self.anchor,
        )
    }

    async fn execute_shard(&self, range: MillisRange) -> Result<Vec<Series>, ProfileError> {
        self.engine
            .select_series_with_anchor(
                self.query,
                self.group_by,
                SeriesSampling {
                    step: self.step,
                    aggregation: self.agg,
                },
                range,
                self.call_sites,
                self.anchor,
            )
            .await
    }

    fn merge_shards(&self, results: Vec<Vec<Series>>) -> Vec<Series> {
        let mut merged: BTreeMap<Vec<crate::ProfileLabel>, BTreeMap<i64, f64>> = BTreeMap::new();
        for series in results {
            for item in series {
                let points = merged.entry(item.labels).or_default();
                for point in item.points {
                    *points.entry(point.timestamp_ms).or_default() += point.value;
                }
            }
        }
        let label_sets = merged.keys().cloned().collect::<Vec<_>>();
        let mut output = Vec::with_capacity(label_sets.len());
        for labels in label_sets {
            let Some(mut values_by_timestamp) = merged.remove(&labels) else {
                continue;
            };
            let timestamps = values_by_timestamp.keys().copied().collect::<Vec<_>>();
            let mut points = Vec::with_capacity(timestamps.len());
            for timestamp_ms in timestamps {
                if let Some(value) = values_by_timestamp.remove(&timestamp_ms) {
                    points.push(crate::SamplePoint {
                        timestamp_ms,
                        value,
                    });
                }
            }
            output.push(Series { labels, points });
        }
        output
    }
}

fn frontend_error(
    error: QueryFrontendError<ProfileError, std::convert::Infallible>,
) -> ProfileError {
    match error {
        QueryFrontendError::Adapter(error) => error,
        QueryFrontendError::Cache(never) => match never {},
        QueryFrontendError::Admission(error) => ProfileError::Overloaded {
            retry_after_seconds: match error {
                krabka_query_frontend::AdmissionError::QueueFull {
                    retry_after_seconds,
                }
                | krabka_query_frontend::AdmissionError::RequestTooLarge {
                    retry_after_seconds,
                } => retry_after_seconds,
            },
        },
    }
}
