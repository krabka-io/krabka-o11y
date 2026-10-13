use super::{
    DistributorState, HeaderMap, PushError, PushOutcome, TenantId, append_clock_readings,
    decode_clock_readings, ingest_stamp, require_snappy_encoding, series_push_result,
};

pub(crate) async fn clocks_push_inner(
    state: &DistributorState,
    tenant: &TenantId,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<PushOutcome, PushError> {
    require_snappy_encoding(headers)?;
    let readings = decode_clock_readings(body, state.max_decompressed)?;
    // Stamp the receive time once for the whole request. A per-record stamp
    // would spread the decode cost of the batch across the readings and report
    // a skew that grows with the batch size.
    let ingest_unix_nanos = ingest_stamp();

    let items = readings.len() as u64;
    // Backfill the decoded reading count onto the enclosing span.
    tracing::Span::current().record("krabka.ingest.series", items);

    let appended = append_clock_readings(state, tenant, &readings, ingest_unix_nanos).await?;
    Ok(series_push_result(state, tenant, items, appended))
}
