use super::*;

impl From<krabka_pprof::FlameGraph> for pb::querier::v1::FlameGraph {
    fn from(value: krabka_pprof::FlameGraph) -> Self {
        Self {
            names: value.names,
            levels: value
                .levels
                .into_iter()
                .map(|level| pb::querier::v1::Level {
                    values: level.values,
                })
                .collect(),
            total: value.total,
            max_self: value.max_self,
        }
    }
}
