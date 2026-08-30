use super::{Float64Builder, ListBuilder, f64_list_field};

pub(crate) fn new_f64_list_builder() -> ListBuilder<Float64Builder> {
    ListBuilder::new(Float64Builder::new()).with_field(f64_list_field())
}
