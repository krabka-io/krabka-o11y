use super::{BTreeMap, ClockReadingRow, ExemplarRow, FloatRow, MetadataRow, NativeHistogramRow};

/// Compacted rows for a single tenant.
#[derive(Clone, Debug, PartialEq)]
pub struct TenantCompactionRows {
    pub tenant: String,
    pub series_labels: BTreeMap<u64, crate::MetricLabels>,
    pub float_rows: Vec<FloatRow>,
    pub histogram_rows: Vec<NativeHistogramRow>,
    pub exemplar_rows: Vec<ExemplarRow>,
    pub metadata_rows: Vec<MetadataRow>,
    pub clock_rows: Vec<ClockReadingRow>,
}

impl TenantCompactionRows {
    pub(crate) fn empty(tenant: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            series_labels: BTreeMap::new(),
            float_rows: Vec::new(),
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        }
    }
}
