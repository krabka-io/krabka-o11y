use std::collections::BTreeMap;

use arrow::{array::BooleanArray, compute::filter_record_batch};
use datafusion::arrow::array::AsArray;

use super::{Result, ScanOptions, SessionContext, TraceqlError, collect_table, register_batches};

/// Pinned Tempo samples the first entry, then every floor(1/fraction) entry,
/// and scales by the measured inspected/admitted ratio. This runs before the
/// selector so rejected spans still contribute to that measured ratio.
pub(crate) async fn sample_metric_scan(
    ctx: &SessionContext,
    table: &str,
    options: &ScanOptions,
) -> Result<(String, f64)> {
    if options.sample_fraction.is_none() && options.trace_sample_fraction.is_none() {
        return Ok((table.into(), 1.0));
    }
    let interval = options
        .sample_fraction
        .map(sampling_interval)
        .transpose()?
        .unwrap_or(1);
    let trace_interval = options
        .trace_sample_fraction
        .map(sampling_interval)
        .transpose()?;
    let batches = collect_table(ctx, table).await?;
    let mut traces = BTreeMap::<Vec<u8>, bool>::new();
    let mut traces_inspected = 0_usize;
    let mut traces_admitted = 0_usize;
    let mut inspected = 0_usize;
    let mut admitted = 0_usize;
    let mut output = Vec::new();
    for batch in batches {
        let ids = trace_interval
            .map(|_| {
                batch
                    .column_by_name(crate::span_columns::COL_TRACE_ID)
                    .ok_or_else(|| {
                        TraceqlError::Exec("trace sampler requires trace identity".into())
                    })
                    .map(AsArray::as_fixed_size_binary)
            })
            .transpose()?;
        let mut mask = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let trace_selected = if let Some(ids) = ids {
                *traces.entry(ids.value(row).to_vec()).or_insert_with(|| {
                    let include = traces_inspected
                        .is_multiple_of(trace_interval.expect("trace sampler interval exists"));
                    traces_inspected += 1;
                    traces_admitted += usize::from(include);
                    include
                })
            } else {
                true
            };
            let include = trace_selected && {
                let include = inspected.is_multiple_of(interval);
                inspected += 1;
                admitted += usize::from(include);
                include
            };
            mask.push(include);
        }
        output.push(
            filter_record_batch(&batch, &BooleanArray::from(mask))
                .map_err(|error| TraceqlError::Exec(error.to_string()))?,
        );
    }
    let factor =
        sampling_factor(inspected, admitted) * sampling_factor(traces_inspected, traces_admitted);
    register_batches(ctx, "metric_sampled_spans", output)?;
    Ok(("metric_sampled_spans".into(), factor))
}

fn sampling_interval(fraction: f64) -> Result<usize> {
    if !fraction.is_finite() || fraction <= 0.0 || fraction >= 1.0 {
        return Err(TraceqlError::Plan(
            "sampling fraction must be between zero and one".into(),
        ));
    }
    format!("{:.0}", fraction.recip().floor())
        .parse::<usize>()
        .map_err(|error| TraceqlError::Plan(format!("sampling interval: {error}")))
}

fn sampling_factor(inspected: usize, admitted: usize) -> f64 {
    if admitted == 0 {
        return 1.0;
    }
    // Avoid platform-dependent usize casts in the metric conversion.
    let number = |value: usize| value.to_string().parse::<f64>().expect("usize is finite");
    number(inspected) / number(admitted)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, FixedSizeBinaryBuilder, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use assert2::assert;
    use datafusion::arrow::array::AsArray;

    use super::{
        ScanOptions, SessionContext, collect_table, register_batches, sample_metric_scan,
        sampling_interval,
    };

    #[tokio::test]
    async fn periodic_sampling_crosses_batches_and_measures_actual_admitted_ratio() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batches = [vec![10, 20, 30], vec![40, 50]].map(|values| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values)) as ArrayRef],
            )
            .unwrap()
        });
        register_batches(&ctx, "input", batches.to_vec()).unwrap();
        let (table, factor) = sample_metric_scan(
            &ctx,
            "input",
            &ScanOptions {
                sample_fraction: Some(0.5),
                ..ScanOptions::default()
            },
        )
        .await
        .unwrap();
        let values = collect_table(&ctx, &table)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert!(values == vec![10, 30, 50]);
        assert!((factor - 5.0 / 3.0).abs() < f64::EPSILON);
        assert!(sampling_interval(0.4).unwrap() == 2);
        for invalid in [0.0, 1.0, -0.5, f64::NAN, f64::INFINITY] {
            assert!(sampling_interval(invalid).is_err());
        }
    }

    #[tokio::test]
    async fn trace_sampling_retains_entire_interleaved_traces_and_counts_each_once() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            crate::span_columns::COL_TRACE_ID,
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let mut ids = FixedSizeBinaryBuilder::with_capacity(5, 16);
        for id in [1, 2, 1, 3, 2] {
            ids.append_value([id; 16]).unwrap();
        }
        register_batches(
            &ctx,
            "input",
            vec![RecordBatch::try_new(schema, vec![Arc::new(ids.finish())]).unwrap()],
        )
        .unwrap();
        let (table, factor) = sample_metric_scan(
            &ctx,
            "input",
            &ScanOptions {
                trace_sample_fraction: Some(0.5),
                ..ScanOptions::default()
            },
        )
        .await
        .unwrap();
        let ids = collect_table(&ctx, &table)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|batch| {
                let values = batch.column(0).as_fixed_size_binary();
                (0..batch.num_rows())
                    .map(|row| values.value(row)[0])
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert!(ids == vec![1, 1, 3]);
        assert!((factor - 1.5).abs() < f64::EPSILON);
    }
}
