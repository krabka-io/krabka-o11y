use super::{FixedSizeBinaryBuilder, StructBuilder, new_str_list, new_str_list_list};
use crate::span_schema::link_fields;

pub(crate) fn new_link_struct_builder() -> StructBuilder {
    StructBuilder::new(
        link_fields(),
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(8)),
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
            .field_with_name(crate::span_schema::SCOL_LINKS)
            .unwrap()
            .data_type();
        let arrow::datatypes::DataType::List(item) = data_type else {
            panic!("expected a list of structs");
        };
        let array = new_link_struct_builder().finish();
        assert2::assert!(array.data_type() == item.data_type());
        assert2::assert!(array.is_empty());
    }
}
