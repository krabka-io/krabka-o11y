use super::{Int32Builder, ListBuilder, StructBuilder, UInt32Builder, span_list_field, span_struct_fields};

pub(crate) fn new_span_list_builder() -> ListBuilder<StructBuilder> {
    let struct_builder = StructBuilder::new(
        span_struct_fields(),
        vec![
            Box::new(Int32Builder::new()),
            Box::new(UInt32Builder::new()),
        ],
    );
    ListBuilder::new(struct_builder).with_field(span_list_field())
}
