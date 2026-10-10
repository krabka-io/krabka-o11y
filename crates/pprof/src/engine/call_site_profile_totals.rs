use super::{
    Arc, AsArray, BTreeMap, COL_FINGERPRINT, COL_TIMESTAMP, Int64Type, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_VALUE, ProfileError, UInt64Type, stack_matches_call_sites,
};

/// Sums each profile's samples whose stack starts with `call_sites` (root
/// first), as `(timestamp_ms, total)` points ordered by timestamp, then series
/// fingerprint.
///
/// A profile is one `(timestamp, series)` pair; a profile with no matching
/// stack yields no point.
///
/// # Errors
///
/// Returns [`ProfileError::Plan`] or [`ProfileError::Exec`] when the scan
/// query fails, and [`ProfileError::Symbolize`] when a stacktrace id is out of
/// range or the symbolization worker fails.
pub async fn call_site_profile_totals(
    scan: &crate::ProfileScan,
    call_sites: &[String],
) -> Result<Vec<(i64, i64)>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table} GROUP BY {timestamp}, {fingerprint}, {partition}, {stacktrace} \
         ORDER BY {timestamp}, {fingerprint}, {partition}, {stacktrace}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan.collect_sql(&sql).await?;

    let mut per_profile: BTreeMap<(i64, u64), i64> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let fingerprints = batch.column(1).as_primitive::<UInt64Type>();
        let partitions = batch.column(2).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(3).as_primitive::<UInt64Type>();
        let values = batch.column(4).as_primitive::<Int64Type>();
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            rows.push((
                timestamps.value(row),
                fingerprints.value(row),
                partition,
                stacktrace_id,
                values.value(row),
            ));
        }
        let symbols = Arc::clone(&scan.symbols);
        let resolved = tokio::task::spawn_blocking(move || {
            rows.into_iter()
                .map(
                    |(timestamp, fingerprint, partition, stacktrace_id, value)| {
                        (
                            timestamp,
                            fingerprint,
                            symbols.resolve(partition, stacktrace_id),
                            value,
                        )
                    },
                )
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|err| ProfileError::Symbolize(format!("symbolization worker failed: {err}")))?;
        for (timestamp, fingerprint, frames, value) in resolved {
            if stack_matches_call_sites(&frames, call_sites) {
                *per_profile.entry((timestamp, fingerprint)).or_default() += value;
            }
        }
    }

    Ok(per_profile
        .into_iter()
        .map(|((timestamp, _fingerprint), total)| (timestamp, total))
        .collect())
}
