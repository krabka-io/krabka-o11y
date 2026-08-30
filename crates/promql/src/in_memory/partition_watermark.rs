use super::*;

/// WAL offset watermarks materialized in the head for one partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionWatermark {
    /// First WAL offset the store ingested into the head for this partition.
    pub low_water_offset: Offset,
    /// Most recent WAL offset the store ingested into the head for this partition.
    pub high_water_offset: Offset,
}
