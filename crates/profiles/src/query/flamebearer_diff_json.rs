use super::*;

pub(crate) fn flamebearer_diff_json(
    diff: krabka_pprof::FlameGraphDiff,
    profile_type: &str,
) -> serde_json::Value {
    let max_self = diff
        .levels
        .iter()
        .flat_map(|level| level.values.chunks_exact(7))
        .fold(0_i64, |max_self, bar| max_self.max(bar[2]).max(bar[5]));
    json!({
        "flamebearer": {
            "names": diff.names,
            "levels": diff.levels.into_iter().map(|level| level.values).collect::<Vec<_>>(),
            "numTicks": diff.left_ticks + diff.right_ticks,
            "maxSelf": max_self,
            "leftTicks": diff.left_ticks,
            "rightTicks": diff.right_ticks,
        },
        "metadata": flamebearer_metadata("double", profile_type)
    })
}
