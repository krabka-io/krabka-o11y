use super::*;

pub(crate) fn struct_column<'a, T: Array + 'static>(histograms: &'a StructArray, name: &str) -> Option<&'a T> {
    histograms
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<T>()
}
