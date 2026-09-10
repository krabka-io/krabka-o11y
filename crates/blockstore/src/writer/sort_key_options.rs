use super::SortOptions;

/// How the declared sort key orders rows: ascending, nulls first.
///
/// One definition, because the order the writer checks, the order it sorts by
/// when a caller got it wrong, and the order it records in the Parquet
/// `sorting_columns` all have to be the same order or the recorded metadata
/// lies about the file.
pub(crate) const SORT_KEY_OPTIONS: SortOptions = SortOptions {
    descending: false,
    nulls_first: true,
};
