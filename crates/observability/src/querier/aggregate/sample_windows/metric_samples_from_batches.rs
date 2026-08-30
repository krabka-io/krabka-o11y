use super::*;

pub(crate) fn metric_samples_from_batches(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_times: &[i64],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<MetricSamples, QueryError> {
    let mut samples: MetricSamples = BTreeMap::new();

    for batch in batches {
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "series_fingerprint",
                expected: "UInt64",
            })?;
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "timestamp_ns",
                expected: "Int64",
            })?;
        let lines = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(QueryError::InvalidColumn {
                column: "line",
                expected: "Utf8",
            })?;
        let metadata = batch.column(3).as_any().downcast_ref::<MapArray>().ok_or(
            QueryError::InvalidColumn {
                column: "structured_metadata",
                expected: "Map<Utf8, Utf8>",
            },
        )?;

        for row in 0..batch.num_rows() {
            let structured_metadata = structured_metadata_value(metadata, row)?;
            append_matching_metric_row(
                &mut samples,
                plan,
                label_index,
                QueryRow {
                    fingerprint: fingerprints.value(row),
                    timestamp_ns: timestamps.value(row),
                    line: lines.value(row),
                    structured_metadata: &structured_metadata,
                },
                MetricWindow {
                    query,
                    eval_times,
                    range_ns: query.range_ns.0,
                    delete_filters,
                },
            )?;
        }
    }

    Ok(samples)
}
