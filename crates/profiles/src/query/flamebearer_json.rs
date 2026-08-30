use super::{flamebearer_metadata, json};

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
