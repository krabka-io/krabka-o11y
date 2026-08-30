use super::{
    CompareFieldClass, CompareRow, Field, compare_field_class, compare_intrinsic_present,
    compare_row_attr_values,
};

pub(crate) fn compare_field_present(field: &Field, row: &CompareRow) -> bool {
    match compare_field_class(field) {
        Ok(CompareFieldClass::Attr { scope, key }) => {
            !compare_row_attr_values(row, &scope, &key).is_empty()
        }
        Ok(CompareFieldClass::Intrinsic(intrinsic)) => compare_intrinsic_present(row, &intrinsic),
        Err(_) => false,
    }
}
