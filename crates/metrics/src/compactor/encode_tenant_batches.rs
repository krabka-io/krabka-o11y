use super::*;

/// Encodes one tenant's sorted rows into Arrow batches for the block writer.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_tenant_batches(
    rows: &TenantCompactionRows,
) -> Result<TenantBatches, HistogramCodecError> {
    let float = if rows.float_rows.is_empty() {
        None
    } else {
        let tuples = rows
            .float_rows
            .iter()
            .map(|row| (row.fingerprint, row.timestamp_ms, row.value))
            .collect::<Vec<_>>();
        Some(encode_float_samples(&tuples)?)
    };

    let native_histograms = if rows.histogram_rows.is_empty() {
        None
    } else {
        let tuples = rows
            .histogram_rows
            .iter()
            .map(|row| (row.fingerprint, row.timestamp_ms, row.hist.clone()))
            .collect::<Vec<_>>();
        Some(encode_native_histograms(&tuples)?)
    };

    let exemplars = if rows.exemplar_rows.is_empty() {
        None
    } else {
        Some(encode_exemplar_rows(&rows.exemplar_rows)?)
    };

    let metadata = if rows.metadata_rows.is_empty() {
        None
    } else {
        Some(encode_metadata_rows(&rows.metadata_rows)?)
    };

    let clock_readings = if rows.clock_rows.is_empty() {
        None
    } else {
        Some(encode_clock_reading_rows(&rows.clock_rows)?)
    };

    Ok(TenantBatches {
        float,
        native_histograms,
        exemplars,
        metadata,
        clock_readings,
    })
}
