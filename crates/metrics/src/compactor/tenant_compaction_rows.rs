use super::*;

/// Compacted rows for a single tenant.
#[derive(Clone, Debug, PartialEq)]
pub struct TenantCompactionRows {
    pub tenant: String,
    pub series_labels: BTreeMap<u64, krabka_blockstore::Labels>,
    pub float_rows: Vec<FloatRow>,
    pub histogram_rows: Vec<NativeHistogramRow>,
    pub exemplar_rows: Vec<ExemplarRow>,
    pub metadata_rows: Vec<MetadataRow>,
    pub clock_rows: Vec<ClockReadingRow>,
}
