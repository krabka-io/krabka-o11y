use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray,
    StringBuilder,
};
use krabka_column_macros::ColumnBuilders;

#[derive(ColumnBuilders)]
#[columns(args = "capacity: usize")]
struct OrderedColumns {
    label: StringBuilder,
    #[column(init = "FixedSizeBinaryBuilder::with_capacity(capacity, 8)")]
    identifier: FixedSizeBinaryBuilder,
    count: Int64Builder,
}

#[derive(ColumnBuilders)]
#[columns(named)]
struct NamedColumns {
    #[column(name = "\"second\"")]
    second: Int64Builder,
    #[column(name = "\"first\"")]
    first: Int64Builder,
}

#[test]
fn finishes_distinct_values_in_field_order_with_nulls() {
    let mut builders = OrderedColumns::new(2);
    builders.label.append_value("alpha");
    builders.label.append_null();
    builders.identifier.append_value([7; 8]).unwrap();
    builders.identifier.append_null();
    builders.count.append_value(31);
    builders.count.append_value(-47);
    let columns = builders.finish();
    assert2::assert!(columns.len() == 3);
    let labels = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
    assert2::assert!(labels.iter().collect::<Vec<_>>() == vec![Some("alpha"), None]);
    let identifiers = columns[1]
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert2::assert!(identifiers.value_length() == 8);
    assert2::assert!(identifiers.value(0) == [7; 8]);
    assert2::assert!(identifiers.is_null(1));
    let counts = columns[2].as_any().downcast_ref::<Int64Array>().unwrap();
    assert2::assert!(counts.iter().collect::<Vec<_>>() == vec![Some(31), Some(-47)]);
}

#[test]
fn named_arrays_keep_their_names_and_distinct_values() {
    let mut builders = NamedColumns::new();
    builders.second.append_value(23);
    builders.first.append_value(11);
    let columns = builders.finish();
    assert2::assert!(
        columns.iter().map(|(name, _)| *name).collect::<Vec<_>>() == vec!["second", "first"]
    );
    for ((_, array), expected) in columns.into_iter().zip([23, 11]) {
        let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert2::assert!(array.value(0) == expected);
    }
}

#[test]
fn empty_builders_keep_column_types_and_binary_width() {
    let columns = OrderedColumns::new(0).finish();
    assert2::assert!(columns.len() == 3);
    assert2::assert!(columns.iter().all(Array::is_empty));
    assert2::assert!(columns[0].data_type() == &arrow::datatypes::DataType::Utf8);
    assert2::assert!(columns[1].data_type() == &arrow::datatypes::DataType::FixedSizeBinary(8));
    assert2::assert!(columns[2].data_type() == &arrow::datatypes::DataType::Int64);
}
