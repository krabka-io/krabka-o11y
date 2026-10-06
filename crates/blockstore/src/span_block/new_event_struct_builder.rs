use super::{Int64Builder, StringBuilder, StructBuilder, new_str_list, new_str_list_list};
use crate::span_schema::event_fields;

pub(crate) fn new_event_struct_builder() -> StructBuilder {
    StructBuilder::new(
        event_fields(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(new_str_list()),
            Box::new(new_str_list_list()),
        ],
    )
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;

    use super::*;

    #[test]
    fn empty_builder_matches_the_nested_block_schema() {
        let schema = crate::span_schema::span_block_schema();
        let data_type = schema
            .field_with_name(crate::span_schema::SCOL_EVENTS)
            .unwrap()
            .data_type();
        let arrow::datatypes::DataType::List(item) = data_type else {
            panic!("expected a list of structs");
        };
        let array = new_event_struct_builder().finish();
        assert2::assert!(array.data_type() == item.data_type());
        assert2::assert!(array.is_empty());
    }
}
