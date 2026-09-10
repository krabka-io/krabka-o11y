use super::RequiredColumn;

/// A signal's declared block schema, physical sort key and bloom-filtered
/// columns.
///
/// The declaration is the single input to how a block is physically written:
/// [`crate::validate_against`] checks `required` against the Arrow schema,
/// `sort_key` becomes both the order the writer enforces and the Parquet
/// `sorting_columns` it records, and `bloom_columns` names the identity
/// columns whose row groups get a bloom filter so a point lookup can skip
/// them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSchema {
    /// Columns the Arrow schema must carry, with their type and nullability.
    pub required: Vec<RequiredColumn>,
    /// Top-level columns the block's rows are ordered by, most significant
    /// first, ascending.
    pub sort_key: Vec<String>,
    /// Top-level columns to write a per-row-group bloom filter for.
    ///
    /// A bloom filter is not free: parquet writes one per row group per
    /// column, sized to the distinct values that row group holds. Measured
    /// over a million-row metrics block -- a fingerprint, a timestamp and a
    /// float -- a filter on the fingerprint column alone added about 7% to
    /// the file.
    ///
    /// So it earns its bytes only on an identity column that nothing cheaper
    /// already prunes. A column in `sort_key` is not one: the block leaves in
    /// that order, so each row group's min/max covers a contiguous slice of
    /// it and prunes an equality lookup exactly. Neither is a column an index
    /// filters on before the block is opened at all -- `BlockMeta`'s
    /// fingerprint set and the traces index's own trace-id bloom both do
    /// that. What is left is an identity column buried inside the sort order,
    /// where the values are scattered across every row group and no statistic
    /// can say which one holds a given value.
    pub bloom_columns: Vec<String>,
}
