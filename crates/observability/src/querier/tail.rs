use axum::body::Bytes;

use crate::{
    ActiveLogDeleteFilter, Arc, CompactionFrontier, CompactionFrontierSource, Duration, HeaderMap,
    HttpQueryError, LOKI_DEFAULT_TAIL_LIMIT, LogHotTail, LokiStreamEncoding, Message, QuerierState,
    QueryParams, RequestSecurity, StreamPlan, TenantErrorSurface, TimeRange, Value, WalLogRecord,
    WebSocket, active_log_delete_filters, add_loki_tail_encoding_flags, authorized_tenant,
    current_unix_time_ns, execute_tail_query_with_frontier_and_deletes, json, loki_encoding_flags,
    loki_stream_encoding_for_headers, optional_start_end_range, parse_query, plan_stream_query,
    validate_loki_tail_delay_for, validate_query_string_bytes_limit,
};

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

pub(crate) struct HotTailSnapshot {
    pub(crate) records: Vec<Arc<WalLogRecord>>,
    pub(crate) frontier: CompactionFrontier,
}

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
pub(crate) fn hot_tail_snapshot(state: &QuerierState, time_range: TimeRange) -> HotTailSnapshot {
    state.hot_tail.as_ref().map_or(
        HotTailSnapshot {
            records: Vec::new(),
            frontier: CompactionFrontier::new(i64::MAX),
        },
        |hot_tail| HotTailSnapshot {
            records: hot_tail
                .source
                .records_shared_in_range(time_range.start_ns, time_range.end_ns),
            frontier: hot_tail.frontier.snapshot(),
        },
    )
}

pub(crate) async fn prepare_http_tail(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &QueryParams,
) -> Result<TailStream, HttpQueryError> {
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Tail).await?;
    // One resolution for the whole request: every check below reads the
    // tenant's limits from this state.
    let state = &state.with_tenant_limits(&tenant);
    let tenant = tenant.as_str();
    let time_range = optional_start_end_range(params.start, params.since, params.end)?;
    let delay_for = params.delay_for.unwrap_or(0);
    validate_loki_tail_delay_for(delay_for)?;
    validate_query_string_bytes_limit(state, &params.query)?;
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
    let source;
    let frontier;
    if let Some(hot_tail) = state.hot_tail.as_ref() {
        source = Some(hot_tail.source.clone());
        frontier = hot_tail.frontier.clone();
    } else {
        source = None;
        frontier = CompactionFrontierSource::Snapshot(CompactionFrontier::new(i64::MAX));
    }

    Ok(TailStream {
        plan,
        source,
        frontier,
        delete_filters,
        limit: Some(params.limit.unwrap_or(LOKI_DEFAULT_TAIL_LIMIT)),
        delay_for,
        encoding: loki_stream_encoding_for_headers(headers),
        encoding_flags: loki_encoding_flags(headers),
    })
}

pub(crate) async fn send_tail_stream(mut socket: WebSocket, tail: TailStream) {
    let Some(source) = tail.source else {
        let mut frame = json!({ "streams": [] });
        add_loki_tail_encoding_flags(&mut frame, &tail.encoding_flags);
        let _ = send_tail_frame(&mut socket, frame).await;
        return;
    };
    let initial_records = source.records().len();
    let mut sent_records = 0;
    let mut poll = tokio::time::interval(Duration::from_millis(50));
    let mut keepalive = tokio::time::interval(Duration::from_secs(15));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    poll.tick().await;
    keepalive.tick().await;

    loop {
        tokio::select! {
            message = socket.recv() => match message {
                Some(Ok(Message::Ping(payload))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        return;
                    }
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                Some(Ok(Message::Text(_) | Message::Binary(_) | Message::Pong(_))) => {}
            },
            _ = keepalive.tick() => {
                if socket.send(Message::Ping(Bytes::default())).await.is_err() {
                    return;
                }
            },
            _ = poll.tick() => {
                let records = source.records();
                if records.len() < sent_records {
                    sent_records = 0;
                }
                if records.len() > sent_records {
                    let eligible = eligible_tail_record_count(&records[sent_records..], tail.delay_for);
                    if eligible > 0 {
                        let eligible_end = sent_records + eligible;
                        let split = initial_records.clamp(sent_records, eligible_end);
                        for (start, end, live) in [
                            (sent_records, split, false),
                            (split, eligible_end, true),
                        ] {
                            if start < end {
                                let frontier = tail.frontier.snapshot();
                                let frame = execute_tail_query_with_frontier_and_deletes(
                                    &tail.plan,
                                    &records[start..end],
                                    &frontier,
                                    &tail.delete_filters,
                                    tail.encoding,
                                    live,
                                );
                                let mut frame = apply_loki_tail_frame_limit(frame, tail.limit);
                                if !tail_frame_is_empty(&frame) {
                                    add_loki_tail_encoding_flags(&mut frame, &tail.encoding_flags);
                                    if !send_tail_frame(&mut socket, frame).await {
                                        return;
                                    }
                                }
                            }
                        }
                        sent_records = eligible_end;
                    }
                }
            },
        };
    }
}

pub(crate) struct TailStream {
    pub(crate) plan: StreamPlan,
    pub(crate) source: Option<Arc<dyn LogHotTail>>,
    pub(crate) frontier: CompactionFrontierSource,
    pub(crate) delete_filters: Vec<ActiveLogDeleteFilter>,
    pub(crate) limit: Option<usize>,
    pub(crate) delay_for: i64,
    pub(crate) encoding: LokiStreamEncoding,
    pub(crate) encoding_flags: Vec<String>,
}
