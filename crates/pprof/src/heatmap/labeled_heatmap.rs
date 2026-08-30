use super::Heatmap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabeledHeatmap {
    pub labels: Vec<(String, String)>,
    pub heatmap: Heatmap,
}
