use super::*;

pub(crate) fn filter_references_only_pushdown_columns(filter: &Expr) -> bool {
    let columns = filter.column_refs();
    !columns.is_empty()
        && columns.iter().all(|column| {
            matches!(
                column.name.as_str(),
                "series_fingerprint" | "timestamp_ns" | "line"
            )
        })
}
