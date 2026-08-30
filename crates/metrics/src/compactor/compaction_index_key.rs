// cargo-mutants: suffix conversion is covered by object-plan and manifest tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn compaction_index_key(block_key: &str) -> String {
    block_key.strip_suffix(".parquet").map_or_else(
        || format!("{block_key}.index"),
        |prefix| format!("{prefix}.index"),
    )
}
