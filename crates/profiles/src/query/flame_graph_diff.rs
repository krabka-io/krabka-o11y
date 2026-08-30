use super::*;

impl From<krabka_pprof::FlameGraphDiff> for pb::querier::v1::FlameGraphDiff {
    fn from(value: krabka_pprof::FlameGraphDiff) -> Self {
        let max_self = value
            .levels
            .iter()
            .flat_map(|level| level.values.chunks_exact(7))
            .fold(0, |max_self, bar| max_self.max(bar[2]).max(bar[5]));
        let total = value.left_ticks + value.right_ticks;
        Self {
            names: value.names,
            levels: value
                .levels
                .into_iter()
                .map(|level| pb::querier::v1::Level {
                    values: level.values,
                })
                .collect(),
            total,
            max_self,
            left_ticks: value.left_ticks,
            right_ticks: value.right_ticks,
        }
    }
}
