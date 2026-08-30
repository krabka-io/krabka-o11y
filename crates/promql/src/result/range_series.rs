use super::{Labels, SampleValue};

/// One labeled series of points in a range matrix.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RangeSeries {
    pub labels: Labels,
    pub samples: Vec<(i64, SampleValue)>,
}
