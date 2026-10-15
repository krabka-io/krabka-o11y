use super::*;
use crate::{FloatSampleRow, numeric_sample_columns::encode_numeric_sample_columns};

/// Encodes sorted clock rows into a block against
/// [`clock_reading_schema`](crate::schema::clock_reading_schema).
pub(crate) fn encode_clock_reading_rows(
    rows: &[ClockReadingRow],
) -> Result<RecordBatch, HistogramCodecError> {
    let mut columns = ClockColumns::new();
    for row in rows {
        columns.append(row);
    }

    let schema = clock_reading_schema();
    let mut named = columns.finish();
    named.sort_by_key(|column| schema.index_of(column.name).unwrap_or(usize::MAX));
    // `index_of` returns an error for a name the schema does not declare, and
    // the sort parks such a column last. Ask again here so the mismatch becomes
    // an error rather than a block with its columns in the wrong order.
    for column in &named {
        schema.index_of(column.name)?;
    }

    Ok(RecordBatch::try_new(
        Arc::clone(&schema),
        named.into_iter().map(|column| column.array).collect(),
    )?)
}

/// Encodes exemplar rows into one record batch of [`exemplar_schema`].
///
/// # Errors
///
/// Returns an error when Arrow rejects the label map or the assembled batch.
pub fn encode_exemplar_rows(rows: &[ExemplarRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut trace_ids = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut labels = MapBuilder::new(
        Some(arrow::array::builder::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Field::new("value", DataType::Utf8, false));

    for row in rows {
        match &row.trace_id {
            Some(trace_id) => trace_ids.append_value(trace_id),
            None => trace_ids.append_null(),
        }
        match &row.span_id {
            Some(span_id) => span_ids.append_value(span_id),
            None => span_ids.append_null(),
        }
        for pair in &row.labels {
            labels.keys().append_value(&pair.name);
            labels.values().append_value(&pair.value);
        }
        labels.append(true)?;
    }

    let mut columns: Vec<ArrayRef> = Vec::from(encode_numeric_sample_columns(rows));
    columns.push(Arc::new(trace_ids.finish()));
    columns.push(Arc::new(span_ids.finish()));
    columns.push(Arc::new(labels.finish()));

    Ok(RecordBatch::try_new(exemplar_schema(), columns)?)
}

/// Encodes metric-family metadata rows into one record batch of
/// [`metadata_schema`].
///
/// # Errors
///
/// Returns an error when Arrow rejects the assembled batch.
pub fn encode_metadata_rows(rows: &[MetadataRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut names = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut helps = StringBuilder::new();
    let mut units = StringBuilder::new();

    for row in rows {
        fingerprints.append_value(row.fingerprint);
        timestamps.append_value(0);
        names.append_value(&row.metric_family_name);
        types.append_value(&row.metric_type);
        helps.append_value(&row.help);
        units.append_value(&row.unit);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(names.finish()),
        Arc::new(types.finish()),
        Arc::new(helps.finish()),
        Arc::new(units.finish()),
    ];

    Ok(RecordBatch::try_new(metadata_schema(), columns)?)
}

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
            .map(|row| FloatSampleRow {
                fingerprint: row.fingerprint,
                timestamp_ms: row.timestamp_ms,
                value: row.value,
                start_timestamp_ms: row.start_timestamp_ms,
            })
            .collect::<Vec<_>>();
        Some(encode_float_samples(&tuples)?)
    };

    let native_histograms = if rows.histogram_rows.is_empty() {
        None
    } else {
        let tuples = rows.histogram_rows.clone();
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
