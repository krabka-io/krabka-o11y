/// Selects the observable query behavior of the pinned Pyroscope storage APIs.
/// V1 retains stack-filtered series and global ingestion stats. V2 follows the
/// segment-writer frontend, including its unavailable span-series exemplars.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PyroscopeQueryArchitecture {
    #[default]
    V1,
    V2,
}
impl std::str::FromStr for PyroscopeQueryArchitecture {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "v1" => Ok(Self::V1),
            "v2" => Ok(Self::V2),
            _ => Err("query architecture must be v1 or v2".into()),
        }
    }
}
