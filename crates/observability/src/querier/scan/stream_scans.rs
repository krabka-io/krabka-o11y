use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashSet},
};
use std::borrow::Borrow;
use super::registered_local_log_session;
use crate::{
    ActiveLogDeleteFilter, Arc, BTreeMap, BlockDescriptor, CompactionFrontier, FsPath, LabelIndex,
    Labels, LineFilter, LineFilterOp, LokiDirection, LokiStreamEncoding, LokiStreamEntry,
    LokiStreamOptions, MetricQuery, NonZeroUsize, ObjectPath, ObjectStore, PipelineStage,
    QueryError, RecordBatch, StreamPlan, TimeRange, Value, WalLogRecord,
    append_matching_hot_log_record, append_matching_log_batches, apply_distinct_to_streams,
    loki_streams_response, loki_streams_response_with_warnings, sort_loki_stream_values,
};



/// How many cold blocks a scan fetches at once when nothing configures it.
///
/// Every cold path -- stream and metric alike -- fetches its planned blocks in
/// batches of this size unless the querier's
/// `querier_cold_block_fetch_concurrency` setting says otherwise. The entry
/// points that take no querier state have no setting to read, so they read
/// this.
pub(crate) fn default_block_fetch_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(8).expect("default block fetch concurrency is nonzero")
}

pub(crate) fn sql_like_pattern_literal(value: &str) -> String {
    sql_string_literal(value)
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(crate) fn sql_string_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

#[must_use]
pub fn stream_plan_scan_sql(plan: &StreamPlan) -> String {
    stream_plan_scan_sql_for_time_range(plan, plan.time_range)
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn execute_stream_query(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_stream_query_with_deletes(root, plan, label_index, &[], LokiStreamEncoding::Folded)
        .await
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
        QueryHotTail::<crate::WalLogRecord> {
            records: &[],
            frontier: &CompactionFrontier::new(i64::MAX),
            delete_filters: &[],
        },
    )
    .await
}

pub(crate) async fn execute_stream_query_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    delete_filters: &[ActiveLogDeleteFilter],
    encoding: LokiStreamEncoding,
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes::<crate::WalLogRecord>(
        root,
        plan,
        label_index,
        &[],
        &CompactionFrontier::new(i64::MAX),
        delete_filters,
        encoding,
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
        LokiStreamEncoding::Folded,
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
        LokiStreamEncoding::Folded,
    )
    .await
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

/// Whether a `|~` pattern can be handed to `regexp_like` without risking a
/// wrong answer.
///
/// `DataFusion` does not always run a literal regex as a regex. When the
/// pattern parses to literal text it simplifies `regexp_like(col, '...')` into
/// `col LIKE '%...%'`, and builds that `LIKE` pattern by copying the regex's
/// literal characters across untouched -- `is_safe_for_like` in
/// `simplify_expressions::regex` rejects `%` and `_` and nothing else. A
/// backslash among them arrives at arrow's `LIKE` as arrow's own escape
/// character and swallows the character after it, so the regex `C:\\Users`
/// becomes a `LIKE` that matches `C:Users` and keeps none of the lines it
/// should. The scan may never drop a row the pipeline would have kept, so a
/// pattern that can put a literal backslash into the match stays in Rust.
///
/// Two escapes in the `regex` crate's syntax can produce one: `\\`, and a hex
/// escape such as `\x5c` or `\x{5c}`. Both are refused by looking for the
/// escape rather than for what it denotes, which over-refuses -- `\x41` is an
/// `A` and would have been fine -- and never under-refuses. `\d`, `\s`, `\w`,
/// `\.` and the rest put no backslash into the match and push down unchanged.
pub(crate) fn regex_line_filter_is_pushdown_safe(pattern: &str) -> bool {
    !pattern.contains("\\\\") && !pattern.contains("\\x")
}

pub(crate) async fn collect_object_store_stream_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
) -> Result<Vec<RecordBatch>, QueryError> {
    super::collect_object_store_log_batches(
        store,
        prefix,
        block,
        &stream_plan_scan_sql(plan),
    )
    .await
}

pub(crate) async fn execute_stream_query_from_object_store_with_hot_tail_frontier<
    R: Borrow<WalLogRecord> + Sync,
>(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_, R>,
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

struct BlockCollectionResult<'a> {
    block: &'a BlockDescriptor,
    result: Result<Vec<RecordBatch>, QueryError>,
}

pub(crate) async fn execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options<
    R: Borrow<WalLogRecord> + Sync,
>(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: QueryHotTail<'_, R>,
    options: StreamScanOptions,
) -> Result<ObjectStoreStreamScan, QueryError> {
    if plan.blocks.is_empty() || plan.fingerprints.is_empty() {
        let mut streams = BTreeMap::new();
        for record in hot_tail.records {
            let record: &WalLogRecord = record.borrow();
            append_matching_hot_log_record(
                &mut streams,
                plan,
                record,
                hot_tail.frontier,
                hot_tail.delete_filters,
                false,
            );
        }
        sort_loki_stream_values(&mut streams);
        apply_distinct_to_streams(&mut streams, &plan.query);
        options.trim_before_encoding(&mut streams);
        return Ok(ObjectStoreStreamScan {
            value: loki_streams_response(streams, options.encoding),
            scanned_blocks: Vec::new(),
        });
    }

    let mut streams: BTreeMap<Labels, Vec<LokiStreamEntry>> = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut scanned_blocks = Vec::new();
    let can_short_circuit = !plan
        .query
        .pipeline
        .iter()
        .any(|stage| matches!(stage, krabka_logql::PipelineStage::Distinct(_)));

    // A WAL arrival can be older or newer than cold rows. Evaluate it before
    // deciding which timestamp ranges can no longer affect the global limit.
    // Keep it separate to preserve the existing cold/hot order for equal times.
    let mut hot_streams = BTreeMap::new();
    for record in hot_tail.records {
        let record: &WalLogRecord = record.borrow();
        append_matching_hot_log_record(
            &mut hot_streams,
            plan,
            record,
            hot_tail.frontier,
            hot_tail.delete_filters,
            false,
        );
    }

    let ordered_blocks = object_store_stream_blocks_in_scan_order(&plan.blocks, options.direction);
    for block_batch in ordered_blocks.chunks(options.block_fetch_concurrency()) {
        if can_short_circuit
            && options.can_skip_remaining_blocks(&streams, &hot_streams, block_batch[0])
        {
            break;
        }
        let results = futures_util::future::join_all(block_batch.iter().map(|block| {
            let store = Arc::clone(&store);
            let block = *block;
            async move {
                let result =
                    collect_object_store_stream_log_batches(store, prefix, block, plan).await;
                BlockCollectionResult { block, result }
            }
        }))
        .await;

        for result in results {
            let block = result.block;
            scanned_blocks.push(block.clone());
            let Ok(batches) = result.result else {
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

    match options.direction {
        LokiDirection::Forward => {
            for mut entry in hot_streams {
                streams.entry(entry.0).or_default().append(&mut entry.1);
            }
        }
        LokiDirection::Backward => {
            for mut entry in streams {
                hot_streams.entry(entry.0).or_default().append(&mut entry.1);
            }
            streams = hot_streams;
        }
    }
    sort_loki_stream_values(&mut streams);
    apply_distinct_to_streams(&mut streams, &plan.query);
    options.trim_before_encoding(&mut streams);

    Ok(ObjectStoreStreamScan {
        value: loki_streams_response_with_warnings(streams, &warnings, options.encoding),
        scanned_blocks,
    })
}

pub(crate) async fn execute_stream_query_with_hot_tail_frontier_and_deletes<
    R: Borrow<WalLogRecord> + Sync,
>(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[R],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
    encoding: LokiStreamEncoding,
) -> Result<Value, QueryError> {
    let mut streams: BTreeMap<Labels, Vec<LokiStreamEntry>> = BTreeMap::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        let ctx = registered_local_log_session(root, &plan.blocks)?;
        let sql = stream_plan_scan_sql(plan);
        let batches = ctx.sql(&sql).await?.collect().await?;
        append_matching_log_batches(&mut streams, plan, label_index, &batches, delete_filters)?;
    }

    for record in hot_tail {
        let record: &WalLogRecord = record.borrow();
        append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters, false);
    }
    sort_loki_stream_values(&mut streams);
    apply_distinct_to_streams(&mut streams, &plan.query);

    Ok(loki_streams_response(streams, encoding))
}

/// One line filter as a SQL predicate over `line`, where it has one.
///
/// `None` where the filter has no SQL form the scan can be trusted with: an
/// `ip(...)` matcher, a regex `DataFusion` may rewrite into a `LIKE` that
/// means something else, or a `|>` pattern with no literal run in it.
pub(crate) fn line_filter_sql_predicate(filter: &LineFilter) -> Option<String> {
    if filter.is_ip_matcher() {
        return None;
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
        LineFilterOp::Regex => regex_line_filter_is_pushdown_safe(&filter.pattern).then(|| {
            format!(
                "regexp_like(line, '{}')",
                sql_regex_pattern_literal(&filter.pattern)
            )
        }),
        LineFilterOp::NotRegex => regex_line_filter_is_pushdown_safe(&filter.pattern).then(|| {
            format!(
                "not regexp_like(line, '{}')",
                sql_regex_pattern_literal(&filter.pattern)
            )
        }),
        LineFilterOp::Pattern => pattern_line_filter_like_pattern(&filter.pattern)
            .map(|like| format!("line like '{like}'")),
        LineFilterOp::NotPattern => pattern_line_filter_like_pattern(&filter.pattern)
            .map(|like| format!("line not like '{like}'")),
    }
}

/// The line filters at the head of `pipeline`, as SQL the block scan can push
/// into Parquet.
///
/// Every predicate here is also applied row by row afterwards, by
/// [`matching_loki_stream_entry`] over the whole pipeline. So this is a
/// pruning filter and nothing rests on it being complete -- but everything
/// rests on it never rejecting a row the pipeline would have kept. Each arm of
/// [`line_filter_sql_predicate`] is therefore either exactly its Rust
/// counterpart or, for the non-negated forms, wider than it:
///
/// - `|=` and `!=` are `str::contains`, and `LIKE '%literal%'` with the
///   wildcards escaped is the same test.
/// - `|~` and `!~` compile the pattern with the `regex` crate and ask
///   `is_match`. `regexp_like` is the same crate and the same call, on the
///   same pattern text, so the two cannot disagree -- except where
///   `DataFusion` rewrites the call into something that is not a regex at all,
///   which is what [`regex_line_filter_is_pushdown_safe`] rules out.
/// - `|>` and `!>` become `LIKE`; see [`pattern_line_filter_like_pattern`].
/// - `ip(...)` filters have no SQL form and stay in Rust.
///
/// The walk stops at the first stage that rewrites the line -- a `line_format`
/// or a parser's output -- because past that point `line` in the block is no
/// longer the text the filter sees.
///
/// [`matching_loki_stream_entry`]: crate::matching_loki_stream_entry
/// [`regex_line_filter_is_pushdown_safe`]: regex_line_filter_is_pushdown_safe
/// [`pattern_line_filter_like_pattern`]: pattern_line_filter_like_pattern
pub(crate) fn line_filter_sql_predicates(pipeline: &[PipelineStage]) -> Vec<String> {
    let mut predicates = Vec::new();
    for stage in pipeline {
        if stage.mutates_line() {
            break;
        }
        let PipelineStage::LineFilter(filter) = stage else {
            continue;
        };
        if let Some(predicate) = line_filter_sql_predicate(filter) {
            predicates.push(predicate);
        }
    }
    predicates
}

pub(crate) fn object_store_stream_blocks_in_scan_order(
    blocks: &[BlockDescriptor],
    direction: LokiDirection,
) -> Vec<&BlockDescriptor> {
    let mut blocks = blocks.iter().collect::<Vec<_>>();
    match direction {
        LokiDirection::Forward => {
            blocks.sort_by(|left, right| {
                left.key
                    .time_range
                    .start_ns
                    .cmp(&right.key.time_range.start_ns)
                    .then_with(|| left.key.time_range.end_ns.cmp(&right.key.time_range.end_ns))
                    .then_with(|| left.key.partition.cmp(&right.key.partition))
                    .then_with(|| left.key.first_offset.cmp(&right.key.first_offset))
            });
        }
        LokiDirection::Backward => {
            blocks.sort_by(|left, right| {
                right
                    .key
                    .time_range
                    .end_ns
                    .cmp(&left.key.time_range.end_ns)
                    .then_with(|| {
                        right
                            .key
                            .time_range
                            .start_ns
                            .cmp(&left.key.time_range.start_ns)
                    })
                    .then_with(|| right.key.partition.cmp(&left.key.partition))
                    .then_with(|| right.key.last_offset.cmp(&left.key.last_offset))
            });
        }
    }
    blocks
}

pub(crate) struct ObjectStoreStreamScan {
    pub(crate) value: Value,
    pub(crate) scanned_blocks: Vec<BlockDescriptor>,
}

/// The `LIKE` pattern equivalent to a `LogQL` `|>` pattern filter, if one exists.
///
/// A pattern filter matches when the pattern's literal runs occur in the line
/// in order, with anything at all between them -- `<_>` is a placeholder, and
/// the ends are not anchored. `LIKE '%a%b%'` says exactly that: arrow lowers
/// it to the unanchored, dot-all regex `a.*b`, so the two agree line for line,
/// including on lines that span newlines.
///
/// `None` when the pattern has no literal run to match on. `<_>` alone matches
/// every line and the empty pattern matches none, and neither is worth a
/// predicate: the first prunes nothing, and the second is rare enough that the
/// row-by-row pass can keep it. Returning `None` for both also keeps the
/// negated form honest -- `NOT LIKE '%%'` would reject every row, while
/// `!> ""` in fact rejects none.
pub(crate) fn pattern_line_filter_like_pattern(pattern: &str) -> Option<String> {
    let literals = pattern
        .split("<_>")
        .filter(|literal| !literal.is_empty())
        .collect::<Vec<_>>();
    if literals.is_empty() {
        return None;
    }

    let mut like = String::from("%");
    for literal in literals {
        like.push_str(&sql_like_pattern_literal(literal));
        like.push('%');
    }
    Some(like)
}

pub(crate) struct QueryHotTail<'a, R> {
    pub(crate) records: &'a [R],
    pub(crate) frontier: &'a CompactionFrontier,
    pub(crate) delete_filters: &'a [ActiveLogDeleteFilter],
}

/// The body of the SQL string literal that carries a regex to `regexp_like`.
///
/// `regexp_like` hands its second argument straight to the `regex` crate, so,
/// unlike a `LIKE` pattern, nothing un-escapes it on the way in.
/// [`sql_like_pattern_literal`] doubles backslashes because arrow's `LIKE`
/// treats `\` as its own escape and halves them again; doing that here would
/// turn `\d` into `\\d` and match a literal backslash instead of a digit.
///
/// The quote is the only character that needs escaping. `DataFusion` parses
/// with sqlparser's generic dialect, whose
/// `supports_string_literal_backslash_escape` is `false`, so a backslash
/// inside `'...'` is an ordinary character and `''` is the only escape the
/// lexer honours.
///
/// [`sql_like_pattern_literal`]: sql_like_pattern_literal
pub(crate) fn sql_regex_pattern_literal(pattern: &str) -> String {
    pattern.replace('\'', "''")
}

// DataFusion deduplicates literal IN values with quadratic comparisons. Keep
// moderately sparse selections exact: coarse ranges decode extra Parquet rows.
// The large-query benchmark measures that tradeoff through one million rows.
// The appender checks exact membership after a range, so its extra rows cannot
// admit an unrelated stream into the result.
const MAX_FINGERPRINT_IN_LIST: usize = 4_096;

/// The SQL one planned block scan runs.
///
/// # Why there is no `LIMIT`
///
/// A Loki stream query carries a `limit`, and [`StreamScanOptions`] holds it,
/// but it stops the block loop between chunks and never bounds what a single
/// block returns. Pushing it into this statement would be wrong twice over,
/// and neither problem is fixable from here:
///
/// - **The scan is not the whole filter.** Only the leading line filters reach
///   SQL. Label filters, parser stages, `ip(...)` matchers and delete filters
///   are applied row by row afterwards, by `matching_loki_stream_entry` and
///   `is_deleted_log_entry`. A `LIMIT n` truncates before any of them run, so
///   a block whose first `n` surviving rows are all dropped in Rust yields
///   nothing, while the rows that would have matched were discarded unread.
///   That is a wrong answer, not a smaller one.
/// - **The scan's order is not the answer's order.** `apply_loki_stream_limit`
///   fills the response stream by stream in label order, giving each stream
///   everything that is left of the budget. Labels are not fingerprints and
///   are not timestamps, so no `ORDER BY` this statement can carry makes the
///   first `n` rows of a block a superset of the rows that truncation keeps.
///   Take the newest `n` of a block and a stream that sorts early loses its
///   entries to a busier stream that sorts late.
///
/// Both conditions do lift for one shape -- a single planned fingerprint, a
/// pipeline of nothing but pushed line filters, no delete filters and no
/// `end_exclusive` -- and only for that shape. It is not the shape a Grafana
/// logs panel sends, so the pushdown stays unwritten rather than sitting
/// behind a guard that is wrong the moment any one of those four stops
/// holding.
///
/// [`StreamScanOptions`]: StreamScanOptions
pub(crate) fn stream_plan_scan_sql_for_time_range(
    plan: &StreamPlan,
    time_range: TimeRange,
) -> String {
    let mut predicates = vec![format!(
        "timestamp_ns >= {} and timestamp_ns <= {}",
        time_range.start_ns, time_range.end_ns
    )];
    if let Some(first) = plan.fingerprints.first()
        && let Some(last) = plan.fingerprints.last()
    {
        if plan.fingerprints.len() > MAX_FINGERPRINT_IN_LIST {
            predicates.push(format!("series_fingerprint between {first} and {last}"));
        } else {
            let fingerprints = plan
                .fingerprints
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            predicates.push(format!("series_fingerprint in ({fingerprints})"));
        }
    }
    predicates.extend(line_filter_sql_predicates(&plan.query.pipeline));
    format!(
        "select series_fingerprint, timestamp_ns, line, structured_metadata \
         from logs \
         where {} \
         order by series_fingerprint, timestamp_ns",
        predicates.join(" and ")
    )
}

#[derive(Clone, Copy)]
pub(crate) struct StreamScanOptions {
    pub(crate) direction: LokiDirection,
    pub(crate) limit: Option<usize>,
    pub(crate) end_exclusive: Option<i64>,
    pub(crate) allow_limit_short_circuit: bool,
    pub(crate) block_fetch_concurrency: NonZeroUsize,
    pub(crate) encoding: LokiStreamEncoding,
}

#[derive(Hash, PartialEq, Eq)]
struct StreamEntryIdentity<'a, Timestamp> {
    labels: &'a Labels,
    timestamp_ns: &'a Timestamp,
    line: &'a str,
}

#[derive(Hash, PartialEq, Eq)]
struct LocalEntryIdentity<'a, Timestamp> {
    timestamp_ns: &'a Timestamp,
    line: &'a str,
}

struct StreamEntryRef<'a> {
    labels: &'a Labels,
    entry: &'a LokiStreamEntry,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
struct StreamHeapEntry {
    priority: i128,
    stream_index: usize,
    entry_index: usize,
}

impl StreamScanOptions {
    pub(crate) fn exhaustive() -> Self {
        Self {
            direction: LokiDirection::Forward,
            limit: None,
            end_exclusive: None,
            allow_limit_short_circuit: false,
            block_fetch_concurrency: default_block_fetch_concurrency(),
            encoding: LokiStreamEncoding::Folded,
        }
    }

    pub(crate) fn from_stream_options(
        options: LokiStreamOptions,
        end_exclusive: Option<i64>,
    ) -> Self {
        let LokiStreamOptions {
            direction,
            limit,
            interval,
        } = options;
        Self {
            direction,
            limit,
            end_exclusive,
            allow_limit_short_circuit: limit.is_some() && interval.is_none(),
            block_fetch_concurrency: default_block_fetch_concurrency(),
            encoding: LokiStreamEncoding::Folded,
        }
    }

    pub(crate) fn with_block_fetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.block_fetch_concurrency = concurrency;
        self
    }

    pub(crate) fn with_encoding(mut self, encoding: LokiStreamEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Ordered blocks after `next_block` cannot outrank `limit` strict winners.
    /// Equal timestamps must still be read for stream and entry tie ordering.
    pub(crate) fn can_skip_remaining_blocks(
        self,
        streams: &BTreeMap<Labels, Vec<LokiStreamEntry>>,
        hot_streams: &BTreeMap<Labels, Vec<LokiStreamEntry>>,
        next_block: &BlockDescriptor,
    ) -> bool {
        if !self.allow_limit_short_circuit || self.encoding != LokiStreamEncoding::Folded {
            return false;
        }
        let Some(limit) = self.limit else {
            return false;
        };
        // A cold row can still be in the hot tail until the frontier advances.
        let mut seen = HashSet::new();
        [streams, hot_streams]
            .into_iter()
            .flat_map(|stream_map| stream_map.keys().zip(stream_map.values()))
            .flat_map(|stream| {
                stream.1.iter().map(move |entry| StreamEntryRef {
                    labels: stream.0,
                    entry,
                })
            })
            .filter(|candidate| {
                candidate
                    .entry
                    .parsed_timestamp_ns()
                    .is_some_and(|timestamp| {
                        self.end_exclusive.is_none_or(|end| timestamp < end)
                            && match self.direction {
                                LokiDirection::Forward => {
                                    timestamp < next_block.key.time_range.start_ns
                                }
                                LokiDirection::Backward => {
                                    timestamp > next_block.key.time_range.end_ns
                                }
                            }
                    })
            })
            .filter(|candidate| {
                seen.insert(StreamEntryIdentity {
                    labels: candidate.labels,
                    timestamp_ns: &candidate.entry.timestamp_ns,
                    line: candidate.entry.line.as_str(),
                })
            })
            .take(limit)
            .count()
            == limit
    }

    /// Spend the folded response's limit before allocating its JSON tree.
    /// Values must already be sorted and distinct stages already applied.
    pub(crate) fn trim_before_encoding(self, streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>) {
        // Intervals select rows later, and categorized labels regroup streams.
        if !self.allow_limit_short_circuit || self.encoding != LokiStreamEncoding::Folded {
            return;
        }
        let Some(mut remaining) = self.limit else {
            return;
        };
        // Equal timestamps can interleave cold and hot copies of different lines.
        for entries in streams.values_mut() {
            let mut keep = {
                let mut seen = HashSet::new();
                entries
                    .iter()
                    .map(|entry| {
                        seen.insert(LocalEntryIdentity {
                            timestamp_ns: &entry.timestamp_ns,
                            line: entry.line.as_str(),
                        })
                    })
                    .collect::<Vec<_>>()
            }
            .into_iter();
            entries.retain(|_| keep.next().expect("each entry has a retention flag"));
        }
        if streams.len() > 1 {
            self.trim_multiple_streams_before_encoding(streams, remaining);
            return;
        }
        streams.retain(|_, entries| {
            if let Some(end) = self.end_exclusive {
                entries.retain(|entry| entry.parsed_timestamp_ns().is_none_or(|ts| ts < end));
            }
            match self.direction {
                LokiDirection::Forward => entries.truncate(remaining),
                LokiDirection::Backward => {
                    entries.drain(..entries.len().saturating_sub(remaining));
                }
            }
            remaining -= entries.len();
            !entries.is_empty()
        });
    }

    fn trim_multiple_streams_before_encoding(
        self,
        streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
        limit: usize,
    ) {
        if limit == 0 {
            streams.clear();
            return;
        }
        let entry_count = streams.values().fold(0_usize, |count, entries| {
            count.saturating_add(entries.len())
        });
        if entry_count <= limit {
            return;
        }
        // Production entries have integer timestamps. Keep the JSON fallback
        // for malformed entries, whose global limiter uses a different key.
        if streams
            .values()
            .flatten()
            .any(|entry| entry.parsed_timestamp_ns().is_none())
        {
            return;
        }
        if let Some(end) = self.end_exclusive {
            for entries in streams.values_mut() {
                entries.retain(|entry| entry.parsed_timestamp_ns().is_some_and(|ts| ts < end));
            }
        }
        let priority = |entry: &LokiStreamEntry| {
            let timestamp = i128::from(entry.parsed_timestamp_ns().expect("checked above"));
            match self.direction {
                LokiDirection::Forward => timestamp,
                LokiDirection::Backward => -timestamp,
            }
        };
        // These indices match the folded response, which iterates the same
        // map. Sorting serialized labels would change escaped-label ties.
        let by_stream = streams.values().collect::<Vec<_>>();
        let mut selected = vec![0; by_stream.len()];
        let mut heads = BinaryHeap::with_capacity(by_stream.len());
        for (stream_index, entries) in by_stream.iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let entry_index = match self.direction {
                LokiDirection::Forward => 0,
                LokiDirection::Backward => entries.len() - 1,
            };
            heads.push(Reverse(StreamHeapEntry {
                priority: priority(&entries[entry_index]),
                stream_index,
                entry_index,
            }));
        }
        for _ in 0..limit {
            let Some(Reverse(candidate)) = heads.pop() else {
                break;
            };
            let stream_index = candidate.stream_index;
            let entry_index = candidate.entry_index;
            selected[stream_index] += 1;
            let entries = by_stream[stream_index];
            let next = match self.direction {
                LokiDirection::Forward => {
                    (entry_index + 1 < entries.len()).then_some(entry_index + 1)
                }
                LokiDirection::Backward => entry_index.checked_sub(1),
            };
            if let Some(next) = next {
                heads.push(Reverse(StreamHeapEntry {
                    priority: priority(&entries[next]),
                    stream_index,
                    entry_index: next,
                }));
            }
        }
        let mut counts = selected.into_iter();
        streams.retain(|_, entries| {
            let count = counts.next().expect("one count per stream");
            match self.direction {
                LokiDirection::Forward => entries.truncate(count),
                LokiDirection::Backward => {
                    entries.drain(..entries.len() - count);
                }
            }
            !entries.is_empty()
        });
    }

    pub(crate) fn block_fetch_concurrency(self) -> usize {
        if !self.allow_limit_short_circuit || self.encoding != LokiStreamEncoding::Folded {
            return self.block_fetch_concurrency.get();
        }
        self.limit
            .map_or(self.block_fetch_concurrency.get(), |limit| {
                self.block_fetch_concurrency.get().min(limit.max(1))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{apply_loki_stream_options, loki_streams_response};

    struct TestLabel {
        name: &'static str,
        value: &'static str,
    }

    fn test_labels(entries: impl IntoIterator<Item = TestLabel>) -> Labels {
        let mut labels = Labels::new();
        for entry in entries {
            labels.insert(entry.name.into(), entry.value.into());
        }
        labels
    }

    struct TestStream {
        app: &'static str,
        timestamps: Vec<i64>,
    }

    struct TestEntry {
        app: &'static str,
        timestamp_ns: i64,
    }

    struct LimitCase {
        limit: Option<usize>,
        end: Option<i64>,
    }

    struct ConcurrencyCase {
        encoding: LokiStreamEncoding,
        limit: Option<usize>,
        interval: Option<i64>,
        configured: usize,
        expected: usize,
    }

    fn streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let source_streams = [
            TestStream {
                app: "a",
                timestamps: vec![1, 5, 8, 8],
            },
            TestStream {
                app: "b",
                timestamps: vec![3, 6, 9],
            },
        ];
        let mut streams = BTreeMap::new();
        for stream in source_streams {
            let labels = test_labels([TestLabel {
                name: "app",
                value: stream.app,
            }]);
            let mut index = 0;
            let entries = stream
                .timestamps
                .into_iter()
                .map(|timestamp_ns| {
                    let entry_index = index;
                    index += 1;
                    LokiStreamEntry::new(
                        timestamp_ns,
                        format!("{}-{entry_index}", stream.app),
                        Labels::new(),
                        Labels::new(),
                    )
                })
                .collect();
            streams.insert(labels, entries);
        }
        streams
    }

    fn escaped_and_unicode_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let mut streams = BTreeMap::new();
        for app in ["\n", "\"", "/", "\\", "é", "λ", "🎉"] {
            let labels = test_labels([TestLabel {
                name: "app",
                value: app,
            }]);
            let mut index = 0;
            let entries: Vec<_> = [1, 5, 5, 7, 9]
                .into_iter()
                .map(|timestamp| {
                    let entry_index = index;
                    index += 1;
                    LokiStreamEntry::new(
                        timestamp,
                        format!("{app}-{entry_index}"),
                        test_labels([TestLabel {
                            name: "source",
                            value: "metadata",
                        }]),
                        test_labels([TestLabel {
                            name: "parser",
                            value: "value",
                        }]),
                    )
                })
                .collect();
            streams.insert(labels, entries);
        }
        // A map and its strict extension sort differently as label maps and
        // serialized JSON objects. Equal times expose that distinction.
        let entries = streams
            .get(&test_labels([TestLabel {
                name: "app",
                value: "/",
            }]))
            .unwrap()
            .clone();
        streams.insert(
            test_labels([
                TestLabel {
                    name: "app",
                    value: "/",
                },
                TestLabel {
                    name: "zone",
                    value: "é",
                },
            ]),
            entries,
        );
        streams
    }

    fn extreme_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let source_entries = [
            TestEntry {
                app: "a",
                timestamp_ns: i64::MIN,
            },
            TestEntry {
                app: "a",
                timestamp_ns: -1,
            },
            TestEntry {
                app: "a",
                timestamp_ns: 0,
            },
            TestEntry {
                app: "a",
                timestamp_ns: i64::MAX,
            },
            TestEntry {
                app: "b",
                timestamp_ns: i64::MIN,
            },
            TestEntry {
                app: "b",
                timestamp_ns: 0,
            },
            TestEntry {
                app: "b",
                timestamp_ns: i64::MAX,
            },
        ];
        let mut streams = BTreeMap::new();
        for source_entry in source_entries {
            let labels = test_labels([TestLabel {
                name: "app",
                value: source_entry.app,
            }]);
            streams.entry(labels).or_insert_with(Vec::new).push({
                LokiStreamEntry::new(
                    source_entry.timestamp_ns,
                    format!("{}-{}", source_entry.app, source_entry.timestamp_ns),
                    Labels::new(),
                    Labels::new(),
                )
            });
        }
        streams
    }

    fn malformed_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let mut streams = extreme_streams();
        let mut malformed =
            LokiStreamEntry::new(0, "malformed".into(), Labels::new(), Labels::new());
        malformed.timestamp_ns = "not-a-timestamp".into();
        streams.insert(
            test_labels([TestLabel {
                name: "app",
                value: "c",
            }]),
            vec![malformed],
        );
        streams
    }

    #[test]
    fn typed_limit_matches_the_full_json_response() {
        for direction in [LokiDirection::Forward, LokiDirection::Backward] {
            for case in [
                LimitCase {
                    limit: Some(1),
                    end: None,
                },
                LimitCase {
                    limit: Some(2),
                    end: None,
                },
                LimitCase {
                    limit: Some(3),
                    end: None,
                },
                LimitCase {
                    limit: Some(9),
                    end: None,
                },
                LimitCase {
                    limit: Some(3),
                    end: Some(7),
                },
                LimitCase {
                    limit: Some(0),
                    end: None,
                },
                LimitCase {
                    limit: Some(10),
                    end: Some(7),
                },
                LimitCase {
                    limit: None,
                    end: None,
                },
            ] {
                let LimitCase { limit, end } = case;
                let stream_options = LokiStreamOptions {
                    direction,
                    limit,
                    interval: None,
                };
                let options = StreamScanOptions::from_stream_options(stream_options, end);
                for full in [
                    streams(),
                    streams().into_iter().take(1).collect::<BTreeMap<_, _>>(),
                    escaped_and_unicode_streams(),
                    extreme_streams(),
                    malformed_streams(),
                ] {
                    let expected = apply_loki_stream_options(
                        loki_streams_response(full.clone(), options.encoding),
                        stream_options,
                        end,
                    );
                    let mut bounded = full;
                    options.trim_before_encoding(&mut bounded);
                    let actual = apply_loki_stream_options(
                        loki_streams_response(bounded, options.encoding),
                        stream_options,
                        end,
                    );
                    assert2::check!(actual == expected);
                }
            }
        }
    }

    #[test]
    fn intervals_and_categorized_labels_keep_every_row_for_later_transforms() {
        let backward_three = LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(3),
            interval: None,
        };
        for options in [
            StreamScanOptions::from_stream_options(backward_three, None)
                .with_encoding(LokiStreamEncoding::CategorizeLabels),
            StreamScanOptions::from_stream_options(
                LokiStreamOptions {
                    interval: Some(2),
                    ..backward_three
                },
                None,
            ),
        ] {
            let mut bounded = streams();
            options.trim_before_encoding(&mut bounded);
            assert2::check!(bounded == streams());
        }
    }

    #[test]
    fn block_fetch_concurrency_keeps_the_configured_bound_for_exhaustive_scans() {
        for case in [
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(1),
                interval: None,
                configured: 8,
                expected: 1,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: Some(1),
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(0),
                interval: None,
                configured: 8,
                expected: 1,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: Some(0),
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(3),
                interval: None,
                configured: 8,
                expected: 3,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(12),
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: None,
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: None,
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(1),
                interval: Some(2),
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: Some(1),
                interval: Some(2),
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(0),
                interval: Some(0),
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(1),
                interval: None,
                configured: 1,
                expected: 1,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: Some(1),
                interval: None,
                configured: 1,
                expected: 1,
            },
        ] {
            let ConcurrencyCase {
                encoding,
                limit,
                interval,
                configured,
                expected,
            } = case;
            let options = StreamScanOptions::from_stream_options(
                LokiStreamOptions {
                    direction: LokiDirection::Forward,
                    limit,
                    interval,
                },
                None,
            )
            .with_encoding(encoding)
            .with_block_fetch_concurrency(NonZeroUsize::new(configured).unwrap());
            assert2::check!(options.block_fetch_concurrency() == expected);
        }

        let mut options = StreamScanOptions::from_stream_options(
            LokiStreamOptions {
                direction: LokiDirection::Forward,
                limit: Some(1),
                interval: None,
            },
            None,
        )
        .with_block_fetch_concurrency(NonZeroUsize::new(8).unwrap());
        options.allow_limit_short_circuit = false;
        assert2::check!(options.block_fetch_concurrency() == 8);
    }
}
