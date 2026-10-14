#[derive(Clone, Debug, PartialEq, Eq, derive_more::Deref)]
pub struct BuiltSample {
    pub timestamp_ns: i64,
    #[deref]
    pub fields: krabka_blockstore::ProfileSampleFields,
}
