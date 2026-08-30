use super::table_ref_checked;

pub(crate) fn table_ref(index: i32, len: usize) -> u64 {
    table_ref_checked(index, len, "").unwrap_or(0)
}
