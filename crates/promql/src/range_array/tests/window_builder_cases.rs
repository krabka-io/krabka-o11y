use super::*;

mod dict_array_round_trips_through_recordbatch_column {
use super::*;

#[derive(bon::Builder)]
struct RoundTripWindowExpectation {
    index: usize,
    values: Vec<f64>,
}

#[test]
pub(crate) fn dict_array_round_trips_through_recordbatch_column() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let range_array =
        RangeArray::from_ranges(values, [RangeWindow::new(0, 2), RangeWindow::new(1, 3)]).unwrap();
    let dict = range_array.clone().into_dict_array().unwrap();
    let back = RangeArray::try_from_dict_array(&dict).unwrap();
    assert2::assert!(back.len() == range_array.len());

    for expectation in [
        RoundTripWindowExpectation::builder()
            .index(0)
            .values(vec![1.0, 2.0])
            .build(),
        RoundTripWindowExpectation::builder()
            .index(1)
            .values(vec![2.0, 3.0, 4.0])
            .build(),
    ] {
        assert2::assert!(window_floats(&back, expectation.index) == expectation.values);
    }
}

}

mod iter_float_cells_visits_every_window {
use super::*;

#[test]
pub(crate) fn iter_float_cells_visits_every_window() {
    let range_array = float_range_array();

    let collected = range_array
        .iter_value_slices()
        .unwrap()
        .map(<[f64]>::to_vec)
        .collect::<Vec<_>>();
    assert2::assert!(collected == vec![vec![1.0, 2.0], vec![2.0, 3.0, 4.0]]);
}

}

mod iter_int_cells_visits_every_window {
use super::*;

#[test]
pub(crate) fn iter_int_cells_visits_every_window() {
    let range_array = int_range_array();

    let collected = range_array
        .iter_timestamp_slices()
        .unwrap()
        .map(<[i64]>::to_vec)
        .collect::<Vec<_>>();
    assert2::assert!(collected == vec![vec![0, 15], vec![30, 45]]);
}

}

mod out_of_bounds_window_is_rejected {
use super::*;

#[test]
pub(crate) fn out_of_bounds_window_is_rejected() {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef;
    assert2::assert!(RangeArray::from_ranges(values, [RangeWindow::new(1, 5)]).is_err());
}

}

mod paired_builder_rejects_length_mismatch {
use super::*;

#[test]
pub(crate) fn paired_builder_rejects_length_mismatch() {
    let values = Float64Array::from(vec![1.0, 2.0, 3.0]);
    let timestamps = Int64Array::from(vec![0_i64, 1]);
    assert2::assert!(
        RangeArray::from_paired_ranges(values, timestamps, [RangeWindow::new(0, 2)]).is_err()
    );
}

}

mod paired_builder_shares_window_offsets_across_value_and_timestamp {
use super::*;

#[derive(bon::Builder)]
struct PairedWindowExpectation {
    index: usize,
    values: [f64; 3],
    timestamps: [i64; 3],
}

#[test]
pub(crate) fn paired_builder_shares_window_offsets_across_value_and_timestamp() {
    let values = Float64Array::from(vec![10.0, 11.0, 12.0, 13.0, 14.0]);
    let timestamps = Int64Array::from(vec![0_i64, 15, 30, 45, 60]);
    let paired = RangeArray::from_paired_ranges(
        values,
        timestamps,
        [RangeWindow::new(0, 3), RangeWindow::new(2, 3)],
    )
    .unwrap();
    let value_ranges = paired.values;
    let ts_ranges = paired.timestamps;

    assert2::assert!(value_ranges.ranges() == ts_ranges.ranges());
    assert2::assert!(value_ranges.len() == 2);

    for expectation in [
        PairedWindowExpectation::builder()
            .index(0)
            .values([10.0, 11.0, 12.0])
            .timestamps([0_i64, 15, 30])
            .build(),
        PairedWindowExpectation::builder()
            .index(1)
            .values([12.0, 13.0, 14.0])
            .timestamps([30, 45, 60])
            .build(),
    ] {
        assert2::assert!(
            value_ranges.value_slice(expectation.index).unwrap() == expectation.values
        );
        assert2::assert!(
            ts_ranges.timestamp_slice(expectation.index).unwrap() == expectation.timestamps
        );
    }
}

}

mod survives_datafusion_projection_as_a_column {
use super::*;

#[tokio::test]
pub(crate) async fn survives_datafusion_projection_as_a_column() {
    use datafusion::{
        datasource::memory::MemorySourceConfig, physical_plan::collect, prelude::SessionContext,
    };

    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    let value_ra =
        RangeArray::from_ranges(values, [RangeWindow::new(0, 2), RangeWindow::new(1, 3)]).unwrap();
    let ts_ra =
        RangeArray::from_ranges(timestamps, [RangeWindow::new(0, 2), RangeWindow::new(1, 3)])
            .unwrap();

    let value_col: ArrayRef = Arc::new(value_ra.clone().into_dict_array().unwrap());
    let ts_col: ArrayRef = Arc::new(ts_ra.clone().into_dict_array().unwrap());
    let schema = Arc::new(Schema::new(vec![
        Field::new("values", value_col.data_type().clone(), false),
        Field::new("timestamps", ts_col.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![value_col, ts_col]).unwrap();

    // Run the batch through a trivial DataFusion projection (identity column scan).
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
    let ctx = SessionContext::new();
    let out = collect(source, ctx.task_ctx()).await.unwrap();
    let merged = &out[0];

    let value_dict = merged
        .column_by_name("values")
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .unwrap();
    let ts_dict = merged
        .column_by_name("timestamps")
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .unwrap();

    let value_back = RangeArray::try_from_dict_array(value_dict).unwrap();
    let ts_back = RangeArray::try_from_dict_array(ts_dict).unwrap();

    assert2::assert!(
        value_back
            .iter_value_slices()
            .unwrap()
            .map(<[f64]>::to_vec)
            .collect::<Vec<_>>()
            == vec![vec![1.0, 2.0], vec![2.0, 3.0, 4.0]]
    );
    assert2::assert!(
        ts_back
            .iter_timestamp_slices()
            .unwrap()
            .map(<[i64]>::to_vec)
            .collect::<Vec<_>>()
            == vec![vec![0, 15], vec![15, 30, 45]]
    );
}

}

mod windows_slice_the_backing_array {
use super::*;

#[derive(bon::Builder)]
struct WindowExpectation {
    index: usize,
    values: Vec<f64>,
}

#[test]
pub(crate) fn windows_slice_the_backing_array() {
    let mut builder = Float64Builder::new();
    for value in [10.0, 11.0, 12.0, 13.0, 14.0] {
        builder.append_value(value);
    }
    let values = Arc::new(builder.finish()) as ArrayRef;

    let range_array =
        RangeArray::from_ranges(values, [RangeWindow::new(0, 3), RangeWindow::new(2, 3)]).unwrap();
    assert2::assert!(range_array.len() == 2);

    for expectation in [
        WindowExpectation::builder()
            .index(0)
            .values(vec![10.0, 11.0, 12.0])
            .build(),
        WindowExpectation::builder()
            .index(1)
            .values(vec![12.0, 13.0, 14.0])
            .build(),
    ] {
        assert2::assert!(window_floats(&range_array, expectation.index) == expectation.values);
    }
}

}
