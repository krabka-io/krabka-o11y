use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct EngineOpts {
    pub default_limit: usize,
    pub default_spss: usize,
    pub max_traces: usize,
    pub max_exemplars: usize,
    pub compare_max_values_per_attr: usize,
    pub histogram_buckets: Vec<Time>,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            default_limit: 20,
            default_spss: 3,
            max_traces: 1000,
            max_exemplars: 0,
            compare_max_values_per_attr: 256,
            histogram_buckets: [
                2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384,
            ]
            .map(millis)
            .into(),
        }
    }
}
