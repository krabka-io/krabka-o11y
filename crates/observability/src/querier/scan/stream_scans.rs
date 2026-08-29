use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_stream_query(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_stream_query_with_deletes(root, plan, label_index, &[]).await
}

pub(crate) async fn execute_stream_query_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        label_index,
        &[],
        &CompactionFrontier::new(i64::MAX),
        delete_filters,
    )
    .await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_stream_query_with_hot_tail(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        label_index,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
        &[],
    )
    .await
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_stream_query_with_hot_tail_frontier(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        label_index,
        hot_tail,
        frontier,
        &[],
    )
    .await
}

pub(crate) async fn execute_stream_query_with_hot_tail_frontier_and_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        let ctx = SessionContext::new();
        register_log_blocks(&ctx, "logs", root, &plan.blocks)?;
        let sql = stream_plan_scan_sql(plan);
        let batches = ctx.sql(&sql).await?.collect().await?;
        append_matching_log_batches(&mut streams, plan, label_index, &batches, delete_filters)?;
    }

    for record in hot_tail {
        append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
    }
    sort_loki_stream_values(&mut streams);

    Ok(loki_streams_response(streams))
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_stream_query_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_stream_query_from_object_store_with_hot_tail_frontier(
        store,
        prefix,
        plan,
        label_index,
        QueryHotTail {
            records: &[],
            frontier: &CompactionFrontier::new(i64::MAX),
            delete_filters: &[],
        },
    )
    .await
}

pub(crate) struct ObjectStoreStreamScan {
    pub(crate) value: Value,
    pub(crate) scanned_blocks: Vec<BlockDescriptor>,
}

#[derive(Clone, Copy)]
pub(crate) struct QueryHotTail<'a> {
    pub(crate) records: &'a [WalLogRecord],
    pub(crate) frontier: &'a CompactionFrontier,
    pub(crate) delete_filters: &'a [ActiveLogDeleteFilter],
}

#[derive(Clone, Copy)]
pub(crate) struct StreamScanOptions {
    pub(crate) direction: LokiDirection,
    pub(crate) limit: Option<usize>,
    pub(crate) end_exclusive: Option<i64>,
    pub(crate) allow_limit_short_circuit: bool,
    pub(crate) block_fetch_concurrency: NonZeroUsize,
}

impl StreamScanOptions {
    pub(crate) fn exhaustive() -> Self {
        Self {
            direction: LokiDirection::Forward,
            limit: None,
            end_exclusive: None,
            allow_limit_short_circuit: false,
            block_fetch_concurrency: NonZeroUsize::new(8)
                .expect("default block fetch concurrency is nonzero"),
        }
    }

    pub(crate) fn from_stream_options(
        direction: LokiDirection,
        limit: Option<usize>,
        interval: Option<i64>,
        end_exclusive: Option<i64>,
    ) -> Self {
        Self {
            direction,
            limit,
            end_exclusive,
            allow_limit_short_circuit: limit.is_some() && interval.is_none(),
            block_fetch_concurrency: NonZeroUsize::new(8)
                .expect("default block fetch concurrency is nonzero"),
        }
    }

    pub(crate) fn with_block_fetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.block_fetch_concurrency = concurrency;
        self
    }

    pub(crate) fn reached_limit(self, streams: &BTreeMap<Labels, Vec<[String; 2]>>) -> bool {
        self.allow_limit_short_circuit
            && self
                .limit
                .is_some_and(|limit| count_stream_map_lines(streams, self.end_exclusive) >= limit)
    }

    pub(crate) fn block_fetch_concurrency(self) -> usize {
        if !self.allow_limit_short_circuit {
            return self.block_fetch_concurrency.get();
        }
        self.limit
            .map_or(self.block_fetch_concurrency.get(), |limit| {
                self.block_fetch_concurrency.get().min(limit.max(1))
            })
    }
}

pub(crate) fn count_stream_map_lines(
    streams: &BTreeMap<Labels, Vec<[String; 2]>>,
    end_exclusive: Option<i64>,
) -> usize {
    streams
        .values()
        .map(|values| {
            values
                .iter()
                .filter(|entry| {
                    end_exclusive.is_none_or(|end_exclusive| {
                        entry[0]
                            .parse::<i64>()
                            .map_or(true, |timestamp| timestamp < end_exclusive)
                    })
                })
                .count()
        })
        .fold(0_usize, usize::saturating_add)
}

pub(crate) fn object_store_stream_blocks_in_scan_order(
    blocks: &[BlockDescriptor],
    direction: LokiDirection,
) -> Vec<&BlockDescriptor> {
    let mut blocks = blocks.iter().collect::<Vec<_>>();
    match direction {
        LokiDirection::Forward => {
            blocks.sort_by_key(|block| {
                (
                    block.key.time_range.start_ns,
                    block.key.time_range.end_ns,
                    block.key.partition,
                    block.key.first_offset,
                )
            });
        }
        LokiDirection::Backward => {
            blocks.sort_by_key(|block| {
                (
                    std::cmp::Reverse(block.key.time_range.end_ns),
                    std::cmp::Reverse(block.key.time_range.start_ns),
                    std::cmp::Reverse(block.key.partition),
                    std::cmp::Reverse(block.key.last_offset),
                )
            });
        }
    }
    blocks
}

#[must_use]
pub fn stream_plan_scan_sql(plan: &StreamPlan) -> String {
    stream_plan_scan_sql_for_time_range(plan, plan.time_range)
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn metric_plan_scan_sql(
    plan: &StreamPlan,
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<String, QueryError> {
    let scan_range = metric_scan_range(query, eval_range)?;
    Ok(stream_plan_scan_sql_for_time_range(plan, scan_range))
}

pub(crate) fn metric_scan_range(
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<TimeRange, QueryError> {
    let scan_end_ns = eval_range.end_ns.saturating_sub(query.offset_ns.0);
    let scan_start_ns = eval_range
        .start_ns
        .saturating_sub(query.offset_ns.0)
        .saturating_sub(query.range_ns.0);
    Ok(TimeRange::new(scan_start_ns, scan_end_ns)?)
}

pub(crate) fn stream_plan_scan_sql_for_time_range(
    plan: &StreamPlan,
    time_range: TimeRange,
) -> String {
    let mut predicates = vec![format!(
        "timestamp_ns >= {} and timestamp_ns <= {}",
        time_range.start_ns, time_range.end_ns
    )];
    if !plan.fingerprints.is_empty() {
        let fingerprints = plan
            .fingerprints
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        predicates.push(format!("series_fingerprint in ({fingerprints})"));
    }
    predicates.extend(literal_line_filter_sql_predicates(&plan.query.pipeline));
    format!(
        "select series_fingerprint, timestamp_ns, line, structured_metadata \
         from logs \
         where {} \
         order by series_fingerprint, timestamp_ns",
        predicates.join(" and ")
    )
}

pub(crate) fn literal_line_filter_sql_predicates(pipeline: &[PipelineStage]) -> Vec<String> {
    let mut predicates = Vec::new();
    for stage in pipeline {
        if stage.mutates_line() {
            break;
        }
        if let Some(predicate) = {
            let PipelineStage::LineFilter(filter) = stage else {
                continue;
            };
            if filter.is_ip_matcher() {
                continue;
            }
            match filter.op {
                LineFilterOp::Contains => Some(format!(
                    "line like '%{}%'",
                    sql_like_pattern_literal(&filter.pattern)
                )),
                LineFilterOp::NotContains => Some(format!(
                    "line not like '%{}%'",
                    sql_like_pattern_literal(&filter.pattern)
                )),
                LineFilterOp::Regex
                | LineFilterOp::NotRegex
                | LineFilterOp::Pattern
                | LineFilterOp::NotPattern => None,
            }
        } {
            predicates.push(predicate);
        }
    }
    predicates
}

pub(crate) fn sql_like_pattern_literal(value: &str) -> String {
    sql_string_literal(value)
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(crate) fn sql_string_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

pub(crate) async fn execute_stream_query_from_object_store_with_hot_tail_frontier(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_>,
) -> Result<Value, QueryError> {
    Ok(
        execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            store,
            prefix,
            plan,
            label_index,
            hot_tail,
            StreamScanOptions::exhaustive(),
        )
        .await?
        .value,
    )
}

pub(crate) async fn execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_>,
    options: StreamScanOptions,
) -> Result<ObjectStoreStreamScan, QueryError> {
    if plan.blocks.is_empty() || plan.fingerprints.is_empty() {
        let mut streams = BTreeMap::new();
        for record in hot_tail.records {
            append_matching_hot_log_record(
                &mut streams,
                plan,
                record,
                hot_tail.frontier,
                hot_tail.delete_filters,
            );
        }
        sort_loki_stream_values(&mut streams);
        return Ok(ObjectStoreStreamScan {
            value: loki_streams_response(streams),
            scanned_blocks: Vec::new(),
        });
    }

    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut scanned_blocks = Vec::new();

    if matches!(options.direction, LokiDirection::Backward) {
        for record in hot_tail.records {
            append_matching_hot_log_record(
                &mut streams,
                plan,
                record,
                hot_tail.frontier,
                hot_tail.delete_filters,
            );
        }
    }

    if !options.reached_limit(&streams) {
        let ordered_blocks =
            object_store_stream_blocks_in_scan_order(&plan.blocks, options.direction);
        for block_batch in ordered_blocks.chunks(options.block_fetch_concurrency()) {
            if options.reached_limit(&streams) {
                break;
            }
            let results = futures_util::future::join_all(block_batch.iter().map(|block| {
                let store = Arc::clone(&store);
                let block = *block;
                async move {
                    let result =
                        collect_object_store_stream_log_batches(store, prefix, block, plan).await;
                    (block, result)
                }
            }))
            .await;

            for (block, result) in results {
                scanned_blocks.push(block.clone());
                let Ok(batches) = result else {
                    warnings.push(format!("failed to read block {}", block.key.object_key()));
                    continue;
                };
                append_matching_log_batches(
                    &mut streams,
                    plan,
                    label_index,
                    &batches,
                    hot_tail.delete_filters,
                )?;
            }
        }
    }

    if matches!(options.direction, LokiDirection::Forward) && !options.reached_limit(&streams) {
        for record in hot_tail.records {
            append_matching_hot_log_record(
                &mut streams,
                plan,
                record,
                hot_tail.frontier,
                hot_tail.delete_filters,
            );
        }
    }
    sort_loki_stream_values(&mut streams);

    Ok(ObjectStoreStreamScan {
        value: loki_streams_response_with_warnings(streams, &warnings),
        scanned_blocks,
    })
}

pub(crate) async fn collect_object_store_stream_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
) -> Result<Vec<RecordBatch>, QueryError> {
    let ctx = SessionContext::new();
    register_log_blocks_from_object_store(
        &ctx,
        "logs",
        store,
        prefix.clone(),
        std::slice::from_ref(block),
    )?;
    Ok(ctx
        .sql(&stream_plan_scan_sql(plan))
        .await?
        .collect()
        .await?)
}
