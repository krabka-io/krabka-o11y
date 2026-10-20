use krabka_pprof::{TimestampTotalPoint, call_site_profile_totals, timestamp_total_points};

use super::*;

pub(crate) async fn individual_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    labels: &[krabka_pprof::ProfileLabel],
    profile_id: &str,
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    if call_sites.is_empty() {
        return individual_exemplars_from_totals(
            scan,
            step,
            IndividualProfile { profile_id, labels },
        )
        .await;
    }
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for point in call_site_profile_totals(scan, call_sites).await? {
        let timestamp = point.timestamp_ms;
        let value = point.total;
        out.entry(step_bucket_ms(timestamp, step))
            .or_default()
            .push(pb::types::v1::Exemplar {
                timestamp,
                profile_id: profile_id.to_string(),
                span_id: String::new(),
                trace_id: String::new(),
                value,
                labels: label_pairs.clone(),
            });
    }
    Ok(out)
}

pub(crate) async fn individual_exemplars_from_totals(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    profile: IndividualProfile<'_>,
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    let IndividualProfile { profile_id, labels } = profile;
    let label_pairs = types_label_pairs(labels.to_vec());
    bucket_exemplars(BucketExemplars {
        scan,
        source: ExemplarSource::Profiles,
        bucket: |timestamp| Some(step_bucket_ms(timestamp, step)),
        exemplar: |row: ExemplarRow| pb::types::v1::Exemplar {
            timestamp: row.timestamp,
            profile_id: profile_id.to_string(),
            span_id: row.span_id,
            trace_id: row.trace_id,
            value: row.value,
            labels: label_pairs.clone(),
        },
    })
    .await
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SpanKey {
    timestamp: i64,
    fingerprint: u64,
    span_id: u64,
    trace_id: Option<Vec<u8>>,
}

pub(crate) async fn span_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    labels: &[krabka_pprof::ProfileLabel],
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    if call_sites.is_empty() {
        return span_exemplars_from_totals(scan, step, labels).await;
    }
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {span}, {trace}, {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span}, {trace}, {partition}, {stacktrace} \
         ORDER BY {timestamp}, {fingerprint}, {span}, {trace}, {partition}, {stacktrace}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        span = PCOL_SPAN_ID,
        trace = PCOL_TRACE_ID,
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan.collect_sql(&sql).await?;
    let mut per_span: BTreeMap<SpanKey, i64> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let fingerprints = batch.column(1).as_primitive::<UInt64Type>();
        let span_ids = batch.column(2).as_primitive::<UInt64Type>();
        let trace_ids = batch.column(3).as_binary::<i32>() as &BinaryArray;
        let partitions = batch.column(4).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(5).as_primitive::<UInt64Type>();
        let values = batch.column(6).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            if span_ids.is_null(row) {
                continue;
            }
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            let frames = scan.symbols.resolve(partition, stacktrace_id);
            if frames_match_call_sites(&frames, call_sites) {
                *per_span
                    .entry(SpanKey {
                        timestamp: timestamps.value(row),
                        fingerprint: fingerprints.value(row),
                        span_id: span_ids.value(row),
                        trace_id: (!trace_ids.is_null(row)).then(|| trace_ids.value(row).to_vec()),
                    })
                    .or_default() += values.value(row);
            }
        }
    }
    let label_pairs = types_label_pairs(labels.to_vec());
    let mut out: BTreeMap<i64, Vec<pb::types::v1::Exemplar>> = BTreeMap::new();
    for span_entry in per_span {
        let span_key = span_entry.0;
        let value = span_entry.1;
        out.entry(step_bucket_ms(span_key.timestamp, step))
            .or_default()
            .push(pb::types::v1::Exemplar {
                timestamp: span_key.timestamp,
                profile_id: String::new(),
                span_id: span_id_hex_from_u64(span_key.span_id),
                trace_id: span_key.trace_id.map_or_else(Default::default, hex::encode),
                value,
                labels: label_pairs.clone(),
            });
    }
    Ok(out)
}

pub(crate) async fn span_exemplars_from_totals(
    scan: &krabka_pprof::ProfileScan,
    step: Time,
    labels: &[krabka_pprof::ProfileLabel],
) -> Result<BTreeMap<i64, Vec<pb::types::v1::Exemplar>>, ProfileError> {
    let label_pairs = types_label_pairs(labels.to_vec());
    bucket_exemplars(BucketExemplars {
        scan,
        source: ExemplarSource::SpansByTotal,
        bucket: |timestamp| Some(step_bucket_ms(timestamp, step)),
        exemplar: |row: ExemplarRow| pb::types::v1::Exemplar {
            timestamp: row.timestamp,
            profile_id: String::new(),
            span_id: row.span_id,
            trace_id: row.trace_id,
            value: row.value,
            labels: label_pairs.clone(),
        },
    })
    .await
}

pub(crate) async fn span_heatmap_points_from_scan(
    scan: &krabka_pprof::ProfileScan,
) -> Result<Vec<TimestampTotalPoint>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, SUM({total}) AS total \
         FROM {table} WHERE {span} IS NOT NULL \
         GROUP BY {timestamp}, {fingerprint}, {span}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_VALUE,
        table = scan.samples_table,
        span = PCOL_SPAN_ID,
        fingerprint = COL_FINGERPRINT,
    );
    timestamp_total_points(scan, &sql).await
}

pub(crate) async fn heatmap_individual_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    slots: HeatmapSlotsMillis,
    profile: IndividualProfile<'_>,
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let HeatmapSlotsMillis {
        start: start_ms,
        end: end_ms,
        step: step_ms,
    } = slots;
    let IndividualProfile { profile_id, labels } = profile;
    let labels = label_pairs(labels.to_vec());
    bucket_exemplars(BucketExemplars {
        scan,
        source: ExemplarSource::Profiles,
        bucket: |timestamp| {
            heatmap_slot_timestamp(start_ms.saturating_add(step_ms), end_ms, step_ms, timestamp)
        },
        exemplar: |row: ExemplarRow| pb::querier::v1::Exemplar {
            timestamp: row.timestamp,
            profile_id: profile_id.to_string(),
            span_id: row.span_id,
            trace_id: row.trace_id,
            value: row.value,
            labels: labels.clone(),
        },
    })
    .await
}

pub(crate) async fn heatmap_span_exemplars_from_scan(
    scan: &krabka_pprof::ProfileScan,
    slots: HeatmapSlotsMillis,
    labels: &[krabka_pprof::ProfileLabel],
) -> Result<BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>, ProfileError> {
    let HeatmapSlotsMillis {
        start: start_ms,
        end: end_ms,
        step: step_ms,
    } = slots;
    let labels = label_pairs(labels.to_vec());
    bucket_exemplars(BucketExemplars {
        scan,
        source: ExemplarSource::SpansBySampleValue,
        bucket: |timestamp| {
            heatmap_slot_timestamp(start_ms.saturating_add(step_ms), end_ms, step_ms, timestamp)
        },
        exemplar: |row: ExemplarRow| pb::querier::v1::Exemplar {
            timestamp: row.timestamp,
            profile_id: String::new(),
            span_id: row.span_id,
            trace_id: row.trace_id,
            value: row.value,
            labels: labels.clone(),
        },
    })
    .await
}

pub(crate) fn heatmap_time_buckets(
    start_ms: StartMs,
    end_ms: EndMs,
    step: Time,
    max_buckets: usize,
) -> Result<usize, ProfileError> {
    if start_ms.0 >= end_ms.0 {
        return Err(ProfileError::Plan(
            "heatmap start must be before end".to_string(),
        ));
    }
    // The bounds are instants and the step is an extent: only the step converts,
    // and the bucket walk stays exact integer arithmetic. `step_from_secs` has
    // already rejected a sub-millisecond step at the Connect boundary; this
    // guard keeps the division below safe for any other caller.
    if step < millis(1) {
        return Err(ProfileError::Plan("step must be >= 1ms".to_string()));
    }
    let step_ms = step.millis_i64();
    let span_ms = end_ms
        .0
        .checked_sub(start_ms.0)
        .ok_or_else(|| ProfileError::Plan("heatmap time range is too large".to_string()))?;
    let buckets = (span_ms / step_ms + i64::from(span_ms % step_ms != 0)).max(1);
    Ok(usize::try_from(buckets)
        .unwrap_or(max_buckets)
        .min(max_buckets))
}

pub(crate) fn heatmap_y_mins(
    min_value: MinValue,
    max_value: MaxValue,
    value_buckets: usize,
) -> Vec<f64> {
    if value_buckets == 0 {
        return Vec::new();
    }
    let span = max_value
        .0
        .checked_sub(min_value.0)
        .unwrap_or(i64::MAX)
        .max(0)
        .to_f64()
        .unwrap_or(f64::MAX);
    let min_value = min_value.0.to_f64().unwrap_or_else(|| {
        if min_value.0.is_negative() {
            f64::MIN
        } else {
            f64::MAX
        }
    });
    let bucket_count = value_buckets.to_f64().unwrap_or(f64::MAX);
    (0..value_buckets)
        .map(|bucket| min_value + span * bucket.to_f64().unwrap_or(f64::MAX) / bucket_count)
        .collect()
}
