use super::{
    Arc, BTreeMap, EngineOpts, FlameGraph, FlameGraphDiff, Frame, Heatmap, LabelMatcher,
    LabeledHeatmap, MatchOp, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION,
    PCOL_VALUE, ProfileError, ProfileStore, ProfileType, Series, SeriesAgg, Time, Tree,
    bin_heatmap, covering_range, diff_trees, fold_bucket, group_frame_name,
    heatmap_points_from_totals, merge_scan_to_tree, merge_sql_to_tree,
    series_buckets_from_stacktrace_selector, series_buckets_from_totals, tree_to_pprof,
    tree_to_pprof_with_max_nodes, validate_range, validated_step,
};

/// Profiles flamegraph engine.
pub struct FlameEngine<S: ProfileStore> {
    pub(crate) store: Arc<S>,
    pub(crate) opts: EngineOpts,
}

impl<S: ProfileStore> FlameEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
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
            merge_scan_to_tree(&scan, &mut tree, &prefix, None, &[]).await?;
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
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                range_ms,
                None,
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
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                range_ms,
                None,
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
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    (*start_ms, *end_ms),
                    None,
                    &[],
                )
                .await?;
            merged.merge(&tree);
        }
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
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    (*start_ms, *end_ms),
                    None,
                    call_sites,
                )
                .await?;
            merged.merge(&tree);
        }
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
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    (*start_ms, *end_ms),
                    None,
                    call_sites,
                )
                .await?;
            merged.merge(&tree);
        }
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_pyroscope_tree_bytes(max_nodes))
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
        if matches!(span_ids, Some(ids) if ids.is_empty()) {
            return Err(ProfileError::Plan(
                "span selector must contain at least one span id".to_string(),
            ));
        }
        let matchers = crate::matcher::parse_label_selector(label_selector)?;
        let scan = self
            .store
            .select(tenant, profile_type, &matchers, range_ms.0, range_ms.1)
            .await?;
        let span_where = span_ids.map_or_else(String::new, |ids| {
            format!(
                " WHERE {span} IN ({ids})",
                span = PCOL_SPAN_ID,
                ids = ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
            )
        });
        let sql = format!(
            "SELECT {partition}, {stacktrace}, SUM({value}) AS v \
             FROM {table}{span_where} GROUP BY {partition}, {stacktrace} \
             ORDER BY {partition}, {stacktrace}",
            partition = PCOL_STACKTRACE_PARTITION,
            stacktrace = PCOL_STACKTRACE_ID,
            value = PCOL_VALUE,
            table = scan.samples_table,
            span_where = span_where,
        );
        let mut tree = Tree::new();
        merge_sql_to_tree(&scan, &sql, &mut tree, &[], call_sites).await?;
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

        let mut merged: BTreeMap<Vec<(String, String)>, BTreeMap<i64, f64>> = BTreeMap::new();
        for (start_ms, end_ms) in ranges {
            let series = self
                .select_series_with_stack_trace_selector(
                    query,
                    group_by,
                    step,
                    agg,
                    (*start_ms, *end_ms),
                    call_sites,
                )
                .await?;
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
        let left_tree = self
            .merge_to_tree(
                tenant,
                left.0,
                left.1,
                (left.2, left.3),
                None,
                left_call_sites,
            )
            .await?;
        let right_tree = self
            .merge_to_tree(
                tenant,
                right.0,
                right.1,
                (right.2, right.3),
                None,
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
        let (tenant, profile_type, label_selector) = query;
        let (start_ms, end_ms) = range;
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
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    (*start_ms, *end_ms),
                    Some(span_selector),
                    &[],
                )
                .await?;
            merged.merge(&tree);
        }
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
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    (*start_ms, *end_ms),
                    Some(span_selector),
                    &[],
                )
                .await?;
            merged.merge(&tree);
        }
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
