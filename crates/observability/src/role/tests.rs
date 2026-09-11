use assert2::{assert, check};

use super::RoleKind;

/// Every stage this stack has, so the checks below cover the vocabulary
/// rather than a sample of it. A new variant that is not added here fails the
/// count assertion rather than slipping through untested.
const EVERY_ROLE: [RoleKind; 10] = [
    RoleKind::Distributor,
    RoleKind::BlockBuilder,
    RoleKind::LiveStore,
    RoleKind::Querier,
    RoleKind::QueryFrontend,
    RoleKind::Compactor,
    RoleKind::Ruler,
    RoleKind::MetricsGenerator,
    RoleKind::Symbolizer,
    RoleKind::All,
];

/// Two stages that answered to one name would be indistinguishable in a
/// manifest, in a `--target`, and in a readiness gate -- which is the hazard
/// this vocabulary was written to remove, reintroduced inside it.
#[test]
fn no_two_roles_answer_to_the_same_name() {
    let names: std::collections::BTreeSet<&str> =
        EVERY_ROLE.iter().map(|role| role.as_str()).collect();

    assert!(names.len() == EVERY_ROLE.len());
}

/// The names are typed on a command line and written into YAML, and clap's
/// `ValueEnum` derive renders a variant in kebab-case. A name that was not
/// kebab-case could not be the string a `--target` accepts, so the per-signal
/// suites that compare the two would have nothing to agree on.
#[test]
fn every_name_is_the_kebab_case_an_operator_types() {
    for role in EVERY_ROLE {
        let name = role.as_str();
        check!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "{name}"
        );
        check!(!name.starts_with('-') && !name.ends_with('-'), "{name}");
        check!(role.to_string() == name, "{name}");
    }
}

/// `all` is the only name that stands for a composition. A stage wrongly
/// marked composite would be left out of the very list it belongs in, and one
/// wrongly marked a stage would be staged against itself.
#[test]
fn only_all_is_a_composition_of_the_others() {
    let composite: Vec<&str> = EVERY_ROLE
        .iter()
        .filter(|role| role.is_composite())
        .map(|role| role.as_str())
        .collect();

    assert!(composite == ["all"]);
}
