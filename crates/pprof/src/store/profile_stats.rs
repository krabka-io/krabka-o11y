#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileStats {
    pub data_ingested: bool,
    pub oldest_profile_time: Option<i64>,
    pub newest_profile_time: Option<i64>,
}
