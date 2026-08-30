use super::*;

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
