/// Engine configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineOpts {
    pub default_max_nodes: i64,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            default_max_nodes: 2048,
        }
    }
}
