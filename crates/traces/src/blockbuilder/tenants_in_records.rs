use super::{BTreeSet, SpanRecord};

pub(crate) fn tenants_in_records(records: &[SpanRecord]) -> BTreeSet<String> {
    records.iter().map(|record| record.tenant.clone()).collect()
}
