use super::*;

pub(crate) fn new_f64_list_builder() -> ListBuilder<Float64Builder> {
    ListBuilder::new(Float64Builder::new()).with_field(f64_list_field())
}
