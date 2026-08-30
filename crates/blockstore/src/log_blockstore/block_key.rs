use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockKey {
    pub tenant: String,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub time_range: TimeRange,
}

impl BlockKey {
    #[must_use]
    pub fn new(
        tenant: impl Into<String>,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        time_range: TimeRange,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            partition,
            first_offset,
            last_offset,
            time_range,
        }
    }

    #[must_use]
    pub fn object_key(&self) -> String {
        format!(
            "tenant={}/partition={}/offsets={}-{}/time={}-{}.parquet",
            self.tenant,
            self.partition,
            self.first_offset,
            self.last_offset,
            self.time_range.start_ns,
            self.time_range.end_ns
        )
    }
}
