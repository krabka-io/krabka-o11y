use super::Heatmap;
use crate::ProfileLabel;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledHeatmap {
    pub labels: Vec<ProfileLabel>,
    pub heatmap: Heatmap,
}

/// Label group and unbinned profile totals `(timestamp_ms, value)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledHeatmapPoints {
    pub labels: Vec<ProfileLabel>,
    pub points: Vec<crate::TimestampTotalPoint>,
}
