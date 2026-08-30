use super::*;

/// Query-side span store that merges sealed blocks with an optional live tier.
pub struct KrabkaSpanStore {
    pub(crate) blocks: Arc<BlockStore>,
    pub(crate) trace_index: SharedTraceIndex,
    pub(crate) live: Option<LiveTier>,
    pub(crate) scan_concat_max: ByteSize,
}

impl KrabkaSpanStore {
    #[must_use]
    pub fn new(
        blocks: Arc<BlockStore>,
        trace_index: SharedTraceIndex,
        live: Option<LiveTier>,
    ) -> Self {
        Self::new_with_scan_concat_max(blocks, trace_index, live, DEFAULT_SCAN_CONCAT_MAX)
    }

    #[must_use]
    pub fn new_with_scan_concat_max(
        blocks: Arc<BlockStore>,
        trace_index: SharedTraceIndex,
        live: Option<LiveTier>,
        scan_concat_max: ByteSize,
    ) -> Self {
        Self {
            blocks,
            trace_index,
            live,
            scan_concat_max,
        }
    }

    pub(crate) async fn cold_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
        job: Option<&ScanJob>,
    ) -> Result<Vec<RecordBatch>, TraceqlError> {
        if end_ns < start_ns {
            return Ok(Vec::new());
        }
        let trace_index = self.trace_index.load();
        let (ctx, table) = if let Some(job) = job {
            if !trace_index.trace_blocks(tenant).iter().any(|block| {
                block.object_key == job.object_key
                    && block.min_ts <= end_ns
                    && block.max_ts >= start_ns
            }) {
                return Ok(Vec::new());
            }
            let row_groups = (job.row_group_start..job.row_group_end).collect::<Vec<_>>();
            self.blocks
                .scan_block_row_groups(&job.object_key, &row_groups, span_block_schema())
                .await
                .map_err(|err| block_err(&err))?
        } else {
            let keys = trace_index.candidate_blocks(tenant, start_ns, end_ns);
            self.blocks
                .scan_block_keys(&keys, span_block_schema())
                .await
                .map_err(|err| block_err(&err))?
        };
        collect_table(&ctx, &table).await
    }

    pub(crate) async fn scan_inner(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
        options: &ScanOptions,
    ) -> Result<ScanResult, TraceqlError> {
        let scan_job = options.job.as_ref();
        let (cold_end, live_start) = if scan_job.is_some() {
            (end_ns, end_ns.saturating_add(1))
        } else {
            self.live.as_ref().map_or((end_ns, end_ns + 1), |live| {
                let frontier = live.block_builder_frontier_ns(tenant);
                (
                    end_ns.min(frontier.saturating_sub(1)),
                    start_ns.max(frontier),
                )
            })
        };

        let mut batches = self
            .cold_batches(tenant, start_ns, cold_end, scan_job)
            .await?;
        if let Some(live) = &self.live
            && live_start <= end_ns
        {
            batches.extend(live.span_batches(tenant, live_start, end_ns).await?);
        }
        // What this scan inspected: the decoded size of the cold+live data read,
        // before filtering (surfaced as the Tempo search `metrics.inspectedBytes`).
        let inspected = batches
            .iter()
            .map(|b| {
                ByteSize::from_bytes(u64::try_from(b.get_array_memory_size()).unwrap_or(u64::MAX))
            })
            .sum();
        let batches = recompute_scan_nested_sets(batches, self.scan_concat_max)?;
        let batches = filter_batches_by_matchers(batches, matchers)?;
        let mut expansion_matchers = matchers.to_vec();
        expansion_matchers.extend(options.projection_matchers.clone());
        let batches = add_nested_intrinsic_columns(batches, &expansion_matchers)?;
        let batches = add_span_attr_columns(batches, &options.projection_matchers)?;

        let schema = batches
            .first()
            .map_or_else(span_schema, RecordBatch::schema);
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, partitions)?;
        ctx.register_table("spans", Arc::new(table))?;
        Ok(ScanResult {
            ctx,
            span_table: "spans".into(),
            inspected,
        })
    }

    pub(crate) async fn trace_by_id_inner(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        let trace_index = self.trace_index.load();
        let keys = trace_index.candidate_blocks_for_trace(tenant, trace_id, start_ns, end_ns);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        let mut spans = trace_from_batches(trace_id, collect_table(&ctx, &table).await?)?;

        if let Some(live_trace) = match &self.live {
            Some(live) => live.trace_spans(tenant, trace_id).await?,
            None => None,
        } {
            if spans.is_none() {
                spans = Some(TraceSpans {
                    trace_id: live_trace.trace_id,
                    root_service_name: live_trace.root_service_name.clone(),
                    root_trace_name: live_trace.root_trace_name.clone(),
                    resource_attributes: live_trace.resource_attributes.clone(),
                    spans: Vec::new(),
                });
            }
            if let Some(out) = &mut spans {
                if out.resource_attributes.is_empty() {
                    out.resource_attributes = live_trace.resource_attributes;
                }
                out.spans.extend(live_trace.spans);
                deduplicate_trace_spans(&mut out.spans);
            }
        }

        // `start_ns`/`end_ns` are a block/candidate-selection HINT for a by-id
        // lookup (already applied above via `candidate_blocks_for_trace`), NOT a
        // hard span-level filter. Real Tempo returns the *whole* trace for a
        // by-id request even when Grafana sends a narrow window, so spans that
        // start outside the window (a trace straddling the window edge) are kept
        // and the assembled trace is returned intact (the caller labels it
        // COMPLETE). Clipping here would silently drop straddling spans while
        // still reporting COMPLETE.
        Ok(spans)
    }
}

#[async_trait::async_trait]
impl SpanStore for KrabkaSpanStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult, TraceqlError> {
        self.scan_inner(tenant, matchers, start_ns, end_ns, &ScanOptions::default())
            .await
    }

    async fn scan_with_options(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
        options: &ScanOptions,
    ) -> Result<ScanResult, TraceqlError> {
        self.scan_inner(tenant, matchers, start_ns, end_ns, options)
            .await
    }

    async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        self.trace_by_id_inner(tenant, trace_id, 0, i64::MAX).await
    }

    async fn trace_by_id_within(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        self.trace_by_id_inner(tenant, trace_id, start_ns, end_ns)
            .await
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>, TraceqlError> {
        let trace_index = self.trace_index.load();
        let mut by_scope: BTreeMap<&'static str, (TagScope, BTreeSet<String>)> = BTreeMap::new();
        let has_cold_blocks = !trace_index
            .candidate_blocks(tenant, start_ns, end_ns)
            .is_empty();
        let cold_index_tags = trace_index.tag_names(tenant, start_ns, end_ns);
        let needs_scoped_cold_scan = matches!(
            scope,
            None | Some(
                TagScope::Resource | TagScope::Event | TagScope::Link | TagScope::Instrumentation,
            )
        );
        if has_cold_blocks && !cold_index_tags.is_empty() && needs_scoped_cold_scan {
            let cold_scoped = self
                .cold_attribute_tag_names(tenant, start_ns, end_ns)
                .await?;
            merge_dynamic_scope(
                &mut by_scope,
                scope,
                TagScope::Resource,
                cold_scoped.resource,
            );
            merge_dynamic_scope(&mut by_scope, scope, TagScope::Span, cold_scoped.span);
            merge_dynamic_scope(&mut by_scope, scope, TagScope::Event, cold_scoped.event);
            merge_dynamic_scope(&mut by_scope, scope, TagScope::Link, cold_scoped.link);
            merge_dynamic_scope(
                &mut by_scope,
                scope,
                TagScope::Instrumentation,
                cold_scoped.instrumentation,
            );
        } else if matches!(scope, None | Some(TagScope::Span)) {
            let (_, tags) = by_scope
                .entry("span")
                .or_insert((TagScope::Span, BTreeSet::new()));
            tags.extend(
                cold_index_tags
                    .into_iter()
                    .filter(|tag| !is_intrinsic_tag(tag)),
            );
        }
        if has_cold_blocks {
            merge_static_scope(&mut by_scope, scope, TagScope::Intrinsic, INTRINSIC_TAGS);
            merge_static_scope(&mut by_scope, scope, TagScope::Event, EVENT_TAGS);
            merge_static_scope(&mut by_scope, scope, TagScope::Link, LINK_TAGS);
            merge_static_scope(
                &mut by_scope,
                scope,
                TagScope::Instrumentation,
                INSTRUMENTATION_TAGS,
            );
        }
        if let Some(live) = &self.live {
            for scoped in live.tag_names(tenant, scope, start_ns, end_ns).await? {
                let key = tag_scope_key(scoped.scope);
                let (_, tags) = by_scope
                    .entry(key)
                    .or_insert((scoped.scope, BTreeSet::new()));
                tags.extend(scoped.tags);
            }
        }
        Ok(SCOPE_ORDER
            .iter()
            .filter_map(|scope| by_scope.remove(tag_scope_key(*scope)))
            .filter_map(|(scope, tags)| {
                (!tags.is_empty()).then_some(ScopedTag {
                    scope,
                    tags: tags.into_iter().collect(),
                })
            })
            .collect())
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        let index_tag = tag.strip_prefix("instrumentation.").map_or_else(
            || unscoped_attribute_tag(tag).to_string(),
            |tag| format!("{INSTRUMENTATION_ATTR_PREFIX}{tag}"),
        );
        if is_nested_intrinsic_tag(tag) {
            return self
                .nested_intrinsic_tag_values(tenant, tag, start_ns, end_ns)
                .await;
        }
        if is_intrinsic_tag(tag) {
            let scan = self.scan(tenant, &[], start_ns, end_ns).await?;
            let batches = collect_table(&scan.ctx, &scan.span_table).await?;
            return intrinsic_values_from_batches(tag, &batches);
        }
        let mut values = self
            .cold_attribute_tag_values(tenant, tag, &index_tag, start_ns, end_ns)
            .await?;
        if let Some(live) = &self.live {
            values.extend(
                live.tag_values(tenant, tag, start_ns, end_ns)
                    .await?
                    .into_iter()
                    .map(|value| (value.type_, value.value)),
            );
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| TypedValue { type_, value })
            .collect())
    }
}

impl KrabkaSpanStore {
    pub(crate) async fn cold_attribute_tag_names(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ColdAttributeTagNames, TraceqlError> {
        let trace_index = self.trace_index.load();
        let keys = trace_index.candidate_blocks(tenant, start_ns, end_ns);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        let batches = collect_table(&ctx, &table).await?;
        let mut names = ColdAttributeTagNames::default();
        for batch in &batches {
            collect_attribute_tag_names(batch, &mut names)?;
        }
        Ok(names)
    }

    pub(crate) async fn cold_attribute_tag_values(
        &self,
        tenant: &str,
        tag: &str,
        index_tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<BTreeSet<(String, String)>, TraceqlError> {
        let trace_index = self.trace_index.load();
        let keys = trace_index.prune_blocks_by_tag(tenant, index_tag, None, start_ns, end_ns);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        let batches = collect_table(&ctx, &table).await?;
        let mut values = BTreeSet::new();
        for batch in &batches {
            collect_attribute_tag_values(batch, tag, index_tag, &mut values)?;
        }
        Ok(values)
    }

    pub(crate) async fn nested_intrinsic_tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
        let trace_index = self.trace_index.load();
        let mut values: BTreeSet<(String, String)> = trace_index
            .tag_values(tenant, tag, start_ns, end_ns)
            .into_iter()
            .map(|value| ("string".to_string(), value))
            .collect();
        if let Some(live) = &self.live {
            values.extend(
                live.tag_values(tenant, tag, start_ns, end_ns)
                    .await?
                    .into_iter()
                    .map(|value| (value.type_, value.value)),
            );
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| TypedValue { type_, value })
            .collect())
    }
}
