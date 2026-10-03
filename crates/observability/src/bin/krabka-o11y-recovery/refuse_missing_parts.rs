use super::BTreeSet;

/// The parts that every deployment has.
const SIGNAL_PARTS: [&str; 4] = ["metrics", "logs", "traces", "profiles"];
/// The one `data-root` part of an all-in-one logs role.
const ALL_IN_ONE_DATA_ROOT: &str = "logs-data-root";
/// The `data-root` parts of split logs roles.
const SPLIT_DATA_ROOTS: [&str; 2] = ["logs-block-builder-data-root", "logs-querier-data-root"];

/// Refuses a part list that leaves out a part of the deployment without
/// saying so.
///
/// A deployment has the four signal parts and the logs `data-root` parts.
/// The logs `data-root` is `logs-data-root` for an all-in-one logs role, or
/// both split names for split roles. Each of these must be in `parts` or in
/// `omitted`. An omitted name must be one of these and not in `parts`.
pub(crate) fn refuse_missing_parts(parts: &[&str], omitted: &[String]) -> Result<(), String> {
    let known = SIGNAL_PARTS
        .iter()
        .chain(&SPLIT_DATA_ROOTS)
        .chain([&ALL_IN_ONE_DATA_ROOT])
        .copied()
        .collect::<BTreeSet<_>>();
    let given = parts.iter().copied().collect::<BTreeSet<_>>();
    for name in omitted {
        if !known.contains(name.as_str()) {
            return Err(format!(
                "--omit-part `{name}` is not a deployment part; the parts are {known:?}"
            ));
        }
        if given.contains(name.as_str()) {
            return Err(format!("part `{name}` is given and omitted"));
        }
    }
    let covered = |name: &str| given.contains(name) || omitted.iter().any(|omit| omit == name);
    let mut missing = SIGNAL_PARTS
        .iter()
        .copied()
        .filter(|name| !covered(name))
        .collect::<Vec<_>>();
    if !covered(ALL_IN_ONE_DATA_ROOT) {
        missing.extend(
            SPLIT_DATA_ROOTS
                .iter()
                .copied()
                .filter(|name| !covered(name)),
        );
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the parts {missing:?} are missing; give each one with --part, or pass --omit-part \
             for a part that this deployment does not have"
        ))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A scenario name, the given parts, the omitted parts, and the result.
    type Case = (
        &'static str,
        &'static [&'static str],
        &'static [&'static str],
        Expected,
    );
    type Expected = Result<(), &'static str>;

    #[test]
    fn every_deployment_part_is_given_or_explicitly_omitted() {
        const SPLIT: [&str; 6] = [
            "metrics",
            "logs",
            "traces",
            "profiles",
            "logs-block-builder-data-root",
            "logs-querier-data-root",
        ];
        const ALL_IN_ONE: [&str; 5] = ["metrics", "logs", "traces", "profiles", "logs-data-root"];
        let cases: [Case; 8] = [
            ("split logs roles", &SPLIT, &[], Ok(())),
            ("an all-in-one logs role", &ALL_IN_ONE, &[], Ok(())),
            (
                "an extra part, such as traces-overrides",
                &[
                    "metrics",
                    "logs",
                    "traces",
                    "profiles",
                    "logs-data-root",
                    "traces-overrides",
                ],
                &[],
                Ok(()),
            ),
            (
                "a signal bucket left out",
                &["metrics", "logs", "traces", "logs-data-root"],
                &[],
                Err(
                    "the parts [\"profiles\"] are missing; give each one with --part, or pass \
                     --omit-part for a part that this deployment does not have",
                ),
            ),
            (
                "a data-root volume left out",
                &[
                    "metrics",
                    "logs",
                    "traces",
                    "profiles",
                    "logs-querier-data-root",
                ],
                &[],
                Err(
                    "the parts [\"logs-block-builder-data-root\"] are missing; give each one \
                     with --part, or pass --omit-part for a part that this deployment does not \
                     have",
                ),
            ),
            (
                "a deployment with no profiles role",
                &["metrics", "logs", "traces", "logs-data-root"],
                &["profiles"],
                Ok(()),
            ),
            (
                "a part given and omitted",
                &ALL_IN_ONE,
                &["profiles"],
                Err("part `profiles` is given and omitted"),
            ),
            (
                "an omitted name that is not a part",
                &ALL_IN_ONE,
                &["profile"],
                Err(
                    "--omit-part `profile` is not a deployment part; the parts are \
                     {\"logs\", \"logs-block-builder-data-root\", \"logs-data-root\", \
                     \"logs-querier-data-root\", \"metrics\", \"profiles\", \"traces\"}",
                ),
            ),
        ];
        for (name, parts, omitted, expected) in cases {
            let omitted = omitted.iter().map(ToString::to_string).collect::<Vec<_>>();
            check!(
                refuse_missing_parts(parts, &omitted) == expected.map_err(String::from),
                "{name}"
            );
        }
    }
}
