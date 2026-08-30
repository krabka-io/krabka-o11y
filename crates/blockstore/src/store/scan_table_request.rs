use super::*;

/// One named `DataFusion` table registration request over indexed blocks.
pub struct ScanTableRequest<'a> {
    pub table_name: &'a str,
    pub tenant: &'a str,
    pub matchers: &'a [LabelMatcher],
    pub min_ts: i64,
    pub max_ts: i64,
    pub schema: SchemaRef,
}
