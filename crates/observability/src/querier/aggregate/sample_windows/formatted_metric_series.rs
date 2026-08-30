use super::Labels;

pub(crate) type FormattedMetricSeries = Vec<(Labels, Vec<[String; 2]>)>;
