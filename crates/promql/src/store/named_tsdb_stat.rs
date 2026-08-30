use super::*;

/// One named TSDB status statistic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedTsdbStat {
    pub name: String,
    pub value: usize,
}
