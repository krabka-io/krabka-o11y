use super::*;

pub(crate) async fn clocks_push_inner(
    state: &DistributorState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    let tenant = tenant_from_headers(headers)?;
    require_snappy_encoding(headers)?;
    let readings = decode_clock_readings(body, state.max_decompressed)?;
    // Stamp the receive time once for the whole request. A per-record stamp
    // would spread the decode cost of the batch across the readings and report
    // a skew that grows with the batch size.
    let ingest_unix_nanos = ingest_stamp();

    let items = readings.len() as u64;
    // Backfill the decoded reading count onto the enclosing span.
    tracing::Span::current().record("krabka.ingest.series", items);

    if !append_clock_readings(state, tenant, &readings, ingest_unix_nanos).await? {
        return Ok((PushSuccess::Accepted { counts: None }, items));
    }

    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant, items);
    }
    Ok((PushSuccess::NoContent { counts: None }, items))
}
