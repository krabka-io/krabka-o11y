use super::ResourceAttrs;

/// One `attr.<key>` column a metric projection needs: its name, the attribute
/// key it reads, whether that key is a resource attribute, and its type
/// (`T` is `Option<DataType>` while the type is still being inferred).
pub(crate) struct ProjectedAttrColumn<T> {
    pub(crate) column_name: String,
    pub(crate) lookup_key: String,
    pub(crate) resource: ResourceAttrs,
    pub(crate) data_type: T,
}
