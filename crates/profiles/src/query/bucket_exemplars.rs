use super::{
    Array, AsArray, BTreeMap, BinaryArray, COL_FINGERPRINT, COL_TIMESTAMP, Int64Type, PCOL_SPAN_ID,
    PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, ProfileError, UInt64Type, span_id_hex_from_u64,
};

/// Which rows of a scan become exemplars, and how each one is valued.
#[derive(Clone, Copy)]
pub(crate) enum ExemplarSource {
    /// One exemplar per series and timestamp, valued at its largest total.
    Profiles,
    /// One exemplar per series, timestamp, span and trace, valued at the sum
    /// of its sample values.
    SpansBySampleValue,
    /// One exemplar per series, timestamp, span and trace, valued at its
    /// largest total.
    SpansByTotal,
}

/// One exemplar read from a scan. Span and trace ids are empty for
/// [`ExemplarSource::Profiles`], and the trace id is empty when a span has
/// none.
pub(crate) struct ExemplarRow {
    pub timestamp: i64,
    pub value: i64,
    pub span_id: String,
    pub trace_id: String,
}

/// How `bucket_exemplars` reads a scan's exemplars and groups them.
pub(crate) struct BucketExemplars<'a, B, X> {
    pub(crate) scan: &'a krabka_pprof::ProfileScan,
    pub(crate) source: ExemplarSource,
    /// Assigns a timestamp its bucket, or `None` to drop the exemplar.
    pub(crate) bucket: B,
    /// Builds the output exemplar from a row.
    pub(crate) exemplar: X,
}

/// Reads the exemplars of `scan` and groups them by the bucket that
/// `bucket` assigns to their timestamp, dropping those it assigns none.
pub(crate) async fn bucket_exemplars<E, B, X>(
    request: BucketExemplars<'_, B, X>,
) -> Result<BTreeMap<i64, Vec<E>>, ProfileError>
where
    B: Fn(i64) -> Option<i64>,
    X: Fn(ExemplarRow) -> E,
{
    let BucketExemplars {
        scan,
        source,
        bucket,
        exemplar,
    } = request;
    let sql = match source {
        ExemplarSource::Profiles => format!(
            "SELECT {timestamp}, MAX({total}) AS total \
             FROM {table} GROUP BY {timestamp}, {fingerprint} \
             ORDER BY {timestamp}, {fingerprint}",
            timestamp = COL_TIMESTAMP,
            total = PCOL_TOTAL_VALUE,
            table = scan.samples_table,
            fingerprint = COL_FINGERPRINT,
        ),
        ExemplarSource::SpansBySampleValue | ExemplarSource::SpansByTotal => {
            let (aggregate, total) = if matches!(source, ExemplarSource::SpansBySampleValue) {
                ("SUM", PCOL_VALUE)
            } else {
                ("MAX", PCOL_TOTAL_VALUE)
            };
            format!(
                "SELECT {timestamp}, {fingerprint}, {span}, {trace}, {aggregate}({total}) AS total \
                 FROM {table} WHERE {span} IS NOT NULL \
                 GROUP BY {timestamp}, {fingerprint}, {span}, {trace} \
                 ORDER BY {timestamp}, {fingerprint}, {span}, {trace}",
                timestamp = COL_TIMESTAMP,
                fingerprint = COL_FINGERPRINT,
                span = PCOL_SPAN_ID,
                trace = PCOL_TRACE_ID,
                table = scan.samples_table,
            )
        }
    };
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let mut out: BTreeMap<i64, Vec<E>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let spans = match source {
            ExemplarSource::Profiles => None,
            ExemplarSource::SpansBySampleValue | ExemplarSource::SpansByTotal => Some((
                batch.column(2).as_primitive::<UInt64Type>(),
                batch.column(3).as_binary::<i32>() as &BinaryArray,
            )),
        };
        let totals = batch
            .column(if spans.is_some() { 4 } else { 1 })
            .as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let (span_id, trace_id) = match spans {
                None => (String::new(), String::new()),
                Some((span_ids, _)) if span_ids.is_null(row) => continue,
                Some((span_ids, trace_ids)) => (
                    span_id_hex_from_u64(span_ids.value(row)),
                    if trace_ids.is_null(row) {
                        String::new()
                    } else {
                        hex::encode(trace_ids.value(row))
                    },
                ),
            };
            let timestamp = timestamps.value(row);
            let Some(slot) = bucket(timestamp) else {
                continue;
            };
            out.entry(slot).or_default().push(exemplar(ExemplarRow {
                timestamp,
                value: totals.value(row),
                span_id,
                trace_id,
            }));
        }
    }
    Ok(out)
}
