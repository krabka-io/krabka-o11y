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

    type Expected = Result<(), &'static str>;

    /// A scenario name, the given parts, the omitted parts, and the result.
    struct Case {
        name: &'static str,
        parts: &'static [&'static str],
        omitted: &'static [&'static str],
        expected: Expected,
    }

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
            Case {
                name: "split logs roles",
                parts: &SPLIT,
                omitted: &[],
                expected: Ok(()),
            },
            Case {
                name: "an all-in-one logs role",
                parts: &ALL_IN_ONE,
                omitted: &[],
                expected: Ok(()),
            },
            Case {
                name: "an extra part, such as traces-overrides",
                parts: &[
                    "metrics",
                    "logs",
                    "traces",
                    "profiles",
                    "logs-data-root",
                    "traces-overrides",
                ],
                omitted: &[],
                expected: Ok(()),
            },
            Case {
                name: "a signal bucket left out",
                parts: &["metrics", "logs", "traces", "logs-data-root"],
                omitted: &[],
                expected: Err(
                    "the parts [\"profiles\"] are missing; give each one with --part, or pass \
                     --omit-part for a part that this deployment does not have",
                ),
            },
            Case {
                name: "a data-root volume left out",
                parts: &[
                    "metrics",
                    "logs",
                    "traces",
                    "profiles",
                    "logs-querier-data-root",
                ],
                omitted: &[],
                expected: Err(
                    "the parts [\"logs-block-builder-data-root\"] are missing; give each one \
                     with --part, or pass --omit-part for a part that this deployment does not \
                     have",
                ),
            },
            Case {
                name: "a deployment with no profiles role",
                parts: &["metrics", "logs", "traces", "logs-data-root"],
                omitted: &["profiles"],
                expected: Ok(()),
            },
            Case {
                name: "a part given and omitted",
                parts: &ALL_IN_ONE,
                omitted: &["profiles"],
                expected: Err("part `profiles` is given and omitted"),
            },
            Case {
                name: "an omitted name that is not a part",
                parts: &ALL_IN_ONE,
                omitted: &["profile"],
                expected: Err(
                    "--omit-part `profile` is not a deployment part; the parts are \
                     {\"logs\", \"logs-block-builder-data-root\", \"logs-data-root\", \
                     \"logs-querier-data-root\", \"metrics\", \"profiles\", \"traces\"}",
                ),
            },
        ];
        for case in cases {
            let omitted = case
                .omitted
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            check!(
                refuse_missing_parts(case.parts, &omitted) == case.expected.map_err(String::from),
                "{}",
                case.name
            );
        }
    }
}
