use super::Heatmap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledHeatmap {
    pub labels: Vec<(String, String)>,
    pub heatmap: Heatmap,
}

/// Label group and unbinned profile totals `(timestamp_ms, value)`.
pub type LabeledHeatmapPoints = (Vec<(String, String)>, Vec<(i64, i64)>);
