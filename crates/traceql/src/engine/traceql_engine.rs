use super::{
    Arc, CompareSpec, DurationNanos, EngineOpts, MetricsRange, PlannerContext, Query, QueryHints,
    Result, ScanOptions, ScopedTag, SearchOptions, SearchResponse, SpanStore, TagScope,
    TraceMetricsResponse, TraceSpans, TypedValue, UnixNano, assemble_compare_response,
    assemble_metrics_response, assemble_search_response, collect_planned_batches,
    compare_span_identities, extend_metric_projection_matchers, hinted_max_exemplars, metric_plan,
    parse, plan_query, validate_compare_selection,
};

pub struct TraceqlEngine<S: SpanStore> {
    pub(crate) store: Arc<S>,
    pub(crate) opts: EngineOpts,
}

impl<S: SpanStore> TraceqlEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
    }

    #[must_use]
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    #[must_use]
    pub fn effective_search_limit(&self, limit: usize) -> usize {
        if limit == 0 {
            self.opts.default_limit
        } else {
            limit
        }
        .min(self.opts.max_traces)
    }

    #[must_use]
    pub fn max_traces(&self) -> usize {
        self.opts.max_traces
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn search(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<SearchResponse> {
        self.search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                ..SearchOptions::default()
            },
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn search_with_spss(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        spss: usize,
    ) -> Result<SearchResponse> {
        self.search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                spss,
                search_limit: None,
                scan_options: ScanOptions::default(),
            },
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn search_with_options(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        options: SearchOptions,
    ) -> Result<SearchResponse> {
        let q = parse(query)?;
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns: UnixNano(start_ns),
                end_ns: UnixNano(end_ns),
                scan_options: options.scan_options.clone(),
            },
            &q,
        )
        .await?;
        let batches = planned
            .ctx
            .execute_logical_plan(planned.plan)
            .await?
            .collect()
            .await?;
        let effective_limit = self.effective_search_limit(options.limit);
        let search_limit = options
            .search_limit
            .unwrap_or(effective_limit)
            .min(self.opts.max_traces);
        let effective_spss = if options.spss == 0 {
            self.opts.default_spss
        } else {
            options.spss
        };
        assemble_search_response(
            &batches,
            search_limit,
            effective_spss,
            q.hints.most_recent,
            planned.inspected,
        )
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn query_range(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
    ) -> Result<TraceMetricsResponse> {
        self.query_range_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            step_ns,
            ScanOptions::default(),
        )
        .await
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn query_range_with_options(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        scan_options: ScanOptions,
    ) -> Result<TraceMetricsResponse> {
        let q = parse(query)?;
        let metric = metric_plan(&q)?;
        let max_exemplars = hinted_max_exemplars(self.opts.max_exemplars, q.hints.exemplars);
        if let Some(compare) = metric.compare.clone() {
            return self
                .query_range_compare(
                    tenant,
                    q.root,
                    compare,
                    MetricsRange {
                        scan_start: UnixNano(start_ns),
                        scan_end: UnixNano(end_ns),
                        output_start: UnixNano(start_ns),
                        step: DurationNanos(step_ns),
                    },
                    scan_options,
                )
                .await;
        }

        let mut scan_options = scan_options;
        extend_metric_projection_matchers(&mut scan_options, &metric);
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns: UnixNano(start_ns),
                end_ns: UnixNano(end_ns),
                scan_options,
            },
            &Query {
                root: q.root,
                pipeline: Vec::new(),
                hints: QueryHints::default(),
            },
        )
        .await?;
        let batches = collect_planned_batches(planned).await?;
        assemble_metrics_response(
            &batches,
            UnixNano(start_ns),
            UnixNano(end_ns),
            DurationNanos(step_ns),
            &metric,
            (max_exemplars, &self.opts.histogram_buckets),
            UnixNano(start_ns),
        )
    }

    /// Executes Tempo's attribute-comparison `compare()` metric.
    ///
    /// This method scans every span that matches the outer spanset over the
    /// query range. It then splits the matched spans into two groups. The
    /// `selection` group holds the spans that also match the compare
    /// `selection` spanset, and the optional `[start, end]` sub-window narrows
    /// that group. The `baseline` group holds the rest. For every attribute on
    /// the spans, the method counts how many spans in each group carry each
    /// distinct value per step bucket. It keeps the `top_n` most-frequent
    /// values for each group and attribute, then emits per-value series and
    /// per-group total series.
    pub(crate) async fn query_range_compare(
        &self,
        tenant: &str,
        root: crate::ast::SpansetExpr,
        compare: CompareSpec,
        range: MetricsRange,
        scan_options: ScanOptions,
    ) -> Result<TraceMetricsResponse> {
        // Simple selections use the inexpensive per-row evaluator. Structural,
        // parent, event, and link selections are planned normally and reduced
        // to the selected span identities before the comparison aggregation.
        let selection_needs_planner = validate_compare_selection(&compare.selection).is_err();

        // No attribute projection is added here: the store already supplies
        // every attribute unconditionally. The block attr-list columns
        // (`attr_keys`/`attr_value*`) carry all unpromoted attrs for the real
        // store, and promoted `attr.<key>` columns are present in the scan
        // schema regardless of the filter/by matchers. `compare_row` merges the
        // promoted and block sources and dedups them.
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns: range.scan_start,
                end_ns: range.scan_end,
                scan_options: scan_options.clone(),
            },
            &Query {
                root,
                pipeline: Vec::new(),
                hints: QueryHints::default(),
            },
        )
        .await?;
        let batches = collect_planned_batches(planned).await?;
        let selected_spans = if selection_needs_planner {
            let selection = plan_query(
                self.store.as_ref(),
                &PlannerContext {
                    tenant: tenant.to_string(),
                    start_ns: range.scan_start,
                    end_ns: range.scan_end,
                    scan_options,
                },
                &Query {
                    root: compare.selection.clone(),
                    pipeline: Vec::new(),
                    hints: QueryHints::default(),
                },
            )
            .await?;
            Some(compare_span_identities(
                &collect_planned_batches(selection).await?,
            )?)
        } else {
            None
        };
        assemble_compare_response(
            &batches,
            &compare,
            range,
            self.opts.compare_max_values_per_attr,
            selected_spans.as_ref(),
        )
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>> {
        self.store.trace_by_id(tenant, trace_id).await
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn trace_by_id_within(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Option<TraceSpans>> {
        self.store
            .trace_by_id_within(tenant, trace_id, start_ns, end_ns)
            .await
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        self.store.tag_names(tenant, scope, start_ns, end_ns).await
    }

    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        self.store.tag_values(tenant, tag, start_ns, end_ns).await
    }
}
