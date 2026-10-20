use super::json;

pub(crate) fn flamebearer_json(
    flamegraph: krabka_pprof::FlameGraph,
    profile_type: &str,
) -> serde_json::Value {
    json!({
        "flamebearer": {
            "names": flamegraph.names,
            "levels": flamegraph.levels.into_iter().map(|level| level.values).collect::<Vec<_>>(),
            "numTicks": flamegraph.total,
            "maxSelf": flamegraph.max_self,
        },
        "metadata": flamebearer_metadata("single", profile_type)
    })
}

pub(crate) fn flamebearer_diff_json(
    diff: krabka_pprof::FlameGraphDiff,
    profile_type: &str,
) -> serde_json::Value {
    let max_self = diff
        .levels
        .iter()
        .flat_map(|level| level.values.as_chunks::<7>().0.iter())
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

pub(crate) fn flamebearer_metadata(format: &str, profile_type: &str) -> serde_json::Value {
    match super::ProfileType::parse(profile_type) {
        Ok(parsed) => json!({
            "format": format,
            "spyName": parsed.name,
            "sampleRate": 100,
            "units": parsed.sample_unit,
            "name": profile_type,
        }),
        Err(_) => json!({
            "format": format,
            "spyName": "",
            "sampleRate": 100,
            "units": "",
            "name": profile_type,
        }),
    }
}
