use super::Labels;

pub(crate) struct FormattedMetricSeriesEntry {
    pub(crate) labels: Labels,
    pub(crate) values: Vec<[String; 2]>,
}

pub(crate) type FormattedMetricSeries = Vec<FormattedMetricSeriesEntry>;
