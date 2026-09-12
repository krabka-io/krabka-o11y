use axum::body::Bytes;

use super::{
    Duration, Message, TailStream, WebSocket, add_loki_tail_encoding_flags,
    apply_loki_tail_frame_limit, eligible_tail_record_count,
    execute_tail_query_with_frontier_and_deletes, json, send_tail_frame, tail_frame_is_empty,
};

pub(crate) async fn send_tail_stream(mut socket: WebSocket, tail: TailStream) {
    let Some(source) = tail.source else {
        let mut frame = json!({ "streams": [], "dropped_entries": [] });
        add_loki_tail_encoding_flags(&mut frame, &tail.encoding_flags);
        let _ = send_tail_frame(&mut socket, frame).await;
        return;
    };
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
                        let frontier = tail.frontier.snapshot();
                        let mut frame = execute_tail_query_with_frontier_and_deletes(
                            &tail.plan,
                            &records[sent_records..eligible_end],
                            &frontier,
                            &tail.delete_filters,
                            tail.encoding,
                        );
                        sent_records = eligible_end;
                        frame["dropped_entries"] = json!([]);
                        let mut frame = apply_loki_tail_frame_limit(frame, tail.limit);
                        if !tail_frame_is_empty(&frame) {
                            add_loki_tail_encoding_flags(&mut frame, &tail.encoding_flags);
                            if !send_tail_frame(&mut socket, frame).await {
                                return;
                            }
                        }
                    }
                }
            },
        };
    }
}
