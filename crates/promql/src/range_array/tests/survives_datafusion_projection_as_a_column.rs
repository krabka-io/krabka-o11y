use super::*;

#[tokio::test]
pub(crate) async fn survives_datafusion_projection_as_a_column() {
    use datafusion::{
        datasource::memory::MemorySourceConfig, physical_plan::collect, prelude::SessionContext,
    };

    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    let value_ra = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();
    let ts_ra = RangeArray::from_ranges(timestamps, [(0_u32, 2_u32), (1, 3)]).unwrap();

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
