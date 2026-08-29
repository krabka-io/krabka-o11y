use super::*;

/// Snapshots the hot-tail records that overlap `time_range`, plus the
/// compaction frontier.
///
/// `time_range` must be the planned scan range (`plan.time_range`). Every
/// hot-tail query path re-applies the exact per-record time bound downstream,
/// and that bound is always inside the plan's scan range, which is the stream
/// query range, or [`metric_scan_range`] for metric queries. A prune to the
/// plan range drops only records the downstream filter would reject
/// anyway. Results are identical to a full-buffer scan, and a narrow window
/// avoids a touch of the whole retained buffer.
pub(crate) fn hot_tail_snapshot(
    state: &QuerierState,
    time_range: TimeRange,
) -> (Vec<WalLogRecord>, CompactionFrontier) {
    state.hot_tail.as_ref().map_or(
        (Vec::new(), CompactionFrontier::new(i64::MAX)),
        |hot_tail| {
            (
                hot_tail
                    .source
                    .records_in_range(time_range.start_ns, time_range.end_ns),
                hot_tail.frontier.snapshot(),
            )
        },
    )
}

pub(crate) struct TailStream {
    pub(crate) plan: StreamPlan,
    pub(crate) source: Option<Arc<dyn LogHotTail>>,
    pub(crate) frontier: CompactionFrontierSource,
    pub(crate) delete_filters: Vec<ActiveLogDeleteFilter>,
    pub(crate) limit: Option<usize>,
    pub(crate) delay_for: i64,
}

pub(crate) async fn prepare_http_tail(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &QueryParams,
) -> Result<TailStream, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = optional_start_end_range(params.start, params.since, params.end)?;
    let delay_for = params.delay_for.unwrap_or(0);
    validate_loki_tail_delay_for(delay_for)?;
    validate_query_length_limit(state, &params.query)?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let delete_filters = active_log_delete_filters(state, tenant, time_range)?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    let (source, frontier) = state.hot_tail.as_ref().map_or(
        (
            None,
            CompactionFrontierSource::Snapshot(CompactionFrontier::new(i64::MAX)),
        ),
        |hot_tail| (Some(hot_tail.source.clone()), hot_tail.frontier.clone()),
    );

    Ok(TailStream {
        plan,
        source,
        frontier,
        delete_filters,
        limit: Some(params.limit.unwrap_or(LOKI_DEFAULT_TAIL_LIMIT)),
        delay_for,
    })
}

pub(crate) async fn send_tail_stream(mut socket: WebSocket, tail: TailStream) {
    let Some(source) = tail.source else {
        let _ = send_tail_frame(&mut socket, json!({ "streams": [] })).await;
        return;
    };
    let mut sent_records = 0;

    loop {
        let records = source.records();
        if records.len() < sent_records {
            sent_records = 0;
        }
        // Both comparisons here are permanent mutation survivors, each
        // neutralised by the step below it. Loosening the first admits an
        // unchanged buffer, whose remaining slice is empty and counts zero
        // eligible records; loosening the second admits a zero count, which
        // builds a frame over an empty slice, leaves the cursor where it was,
        // and is dropped by the empty-frame check before any send.
        if records.len() > sent_records {
            let eligible = eligible_tail_record_count(&records[sent_records..], tail.delay_for);
            if eligible > 0 {
                let eligible_end = sent_records + eligible;
                let frontier = tail.frontier.snapshot();
                let frame = execute_tail_query_with_frontier_and_deletes(
                    &tail.plan,
                    &records[sent_records..eligible_end],
                    &frontier,
                    &tail.delete_filters,
                );
                sent_records = eligible_end;
                let frame = apply_loki_tail_frame_limit(frame, tail.limit);
                if !tail_frame_is_empty(&frame) && !send_tail_frame(&mut socket, frame).await {
                    return;
                }
            }
        }

        sleep(Duration::from_millis(50)).await;
    }
}

pub(crate) fn eligible_tail_record_count(records: &[WalLogRecord], delay_for: i64) -> usize {
    if delay_for <= 0 {
        return records.len();
    }

    let cutoff = current_unix_time_ns().saturating_sub(delay_for);
    records
        .iter()
        .take_while(|record| record.timestamp_ns <= cutoff)
        .count()
}

pub(crate) async fn send_tail_frame(socket: &mut WebSocket, frame: Value) -> bool {
    socket
        .send(Message::Text(frame.to_string().into()))
        .await
        .is_ok()
}

pub(crate) fn tail_frame_is_empty(frame: &Value) -> bool {
    frame
        .get("streams")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty)
}

pub(crate) fn apply_loki_tail_frame_limit(mut frame: Value, limit: Option<usize>) -> Value {
    let Some(limit) = limit else {
        return frame;
    };
    let Some(streams) = frame.get_mut("streams").and_then(Value::as_array_mut) else {
        return frame;
    };

    let mut remaining = limit;
    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        // No guards: `truncate` is a no-op when the stream is shorter than
        // what is left, and `truncate(0)` clears exactly as `clear()` would.
        values.truncate(remaining);
        remaining = remaining.saturating_sub(values.len());
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });

    frame
}
