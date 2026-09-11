use super::{Target, ValueEnum};

/// What `--target` says about a role this binary used to have, and no longer.
///
/// `querier`, `query-frontend` and `ruler` were once targets of
/// `krabka-metrics`. Each bound the data port, logged that it was listening,
/// answered `/ready` and served a build-info route -- enough for a Grafana
/// datasource to read as healthy -- and then 404'd every query. They are gone,
/// and the roles under those names live in `krabka-metrics-service`.
///
/// Clap's own rejection would say only that the value is not one of the
/// variants. An operator holding a deployment that names `querier` would learn
/// from that which binary does *not* serve it and nothing about which one
/// does, so this message names the binary instead.
///
/// The roles it offers instead are read off [`Target`], not written out here,
/// so a renamed role cannot leave this message pointing at a name no longer
/// accepted -- which is the same failure one layer down from the one the
/// retirement fixed.
///
/// Returns `None` for any other value, which is the parser's cue to go on and
/// match it against [`Target`] as usual.
///
/// [`Target`]: super::Target
pub(crate) fn retired_role_message(value: &str) -> Option<String> {
    if !matches!(value, "querier" | "query-frontend" | "ruler") {
        return None;
    }
    let live: Vec<String> = Target::value_variants()
        .iter()
        .filter_map(ValueEnum::to_possible_value)
        .map(|role| format!("--target={}", role.get_name()))
        .collect();
    Some(format!(
        "`{value}` is not a role of krabka-metrics\n\n  \
         krabka-metrics is the metrics write path: {}.\n  \
         The read path is a second binary. Run `krabka-metrics-service --target={value}` for it.",
        live.join(" and ")
    ))
}
