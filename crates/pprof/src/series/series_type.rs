use super::{ProfileLabel, SamplePoint};

/// One time series returned by `select_series` in the next slice.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    pub labels: Vec<ProfileLabel>,
    pub points: Vec<SamplePoint>,
}
