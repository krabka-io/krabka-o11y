use super::{
    Arc, AsArray, BTreeMap, COL_FINGERPRINT, COL_TIMESTAMP, Frame, Int64Type, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_VALUE, ProfileError, TimestampTotalPoint, UInt64Type,
    stack_matches_call_sites,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ProfileTimelineKey {
    timestamp_ms: i64,
    fingerprint: u64,
}

struct CallSiteSampleRow {
    timestamp_ms: i64,
    fingerprint: u64,
    partition: u64,
    stacktrace_id: u32,
    value: i64,
}

struct ResolvedCallSiteSample {
    timestamp_ms: i64,
    fingerprint: u64,
    frames: Vec<Frame>,
    value: i64,
}

/// Sums each profile's samples whose stack starts with `call_sites` (root
/// first), as timestamp-total points ordered by timestamp, then series
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
) -> Result<Vec<TimestampTotalPoint>, ProfileError> {
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

    let mut per_profile: BTreeMap<ProfileTimelineKey, i64> = BTreeMap::new();
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
            rows.push(CallSiteSampleRow {
                timestamp_ms: timestamps.value(row),
                fingerprint: fingerprints.value(row),
                partition,
                stacktrace_id,
                value: values.value(row),
            });
        }
        let symbols = Arc::clone(&scan.symbols);
        let resolved = super::symbolization_worker(move || {
            rows.into_iter()
                .map(|row| ResolvedCallSiteSample {
                    timestamp_ms: row.timestamp_ms,
                    fingerprint: row.fingerprint,
                    frames: symbols.resolve(row.partition, row.stacktrace_id),
                    value: row.value,
                })
                .collect::<Vec<_>>()
        })
        .await?;
        for sample in resolved {
            if stack_matches_call_sites(&sample.frames, call_sites) {
                *per_profile
                    .entry(ProfileTimelineKey {
                        timestamp_ms: sample.timestamp_ms,
                        fingerprint: sample.fingerprint,
                    })
                    .or_default() += sample.value;
            }
        }
    }

    let keys = per_profile.keys().copied().collect::<Vec<_>>();
    let mut points = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some(total) = per_profile.remove(&key) {
            points.push(TimestampTotalPoint {
                timestamp_ms: key.timestamp_ms,
                total,
            });
        }
    }
    Ok(points)
}
