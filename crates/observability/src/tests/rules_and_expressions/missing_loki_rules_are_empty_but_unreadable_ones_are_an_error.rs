use super::*;

/// `read_loki_rule_tenants` treats a MISSING rules file as no rules, and
/// every other I/O failure as an error. That distinction is the point: a
/// store that has never had a rule written to it has no file, and starting
/// up must not fail because of it -- while a file that exists and cannot
/// be read is a real problem the operator needs told about.
///
/// Malformed JSON is likewise an error rather than an empty result:
/// silently discarding every rule in a corrupt file would stop alerting
/// without saying so.
#[test]
pub(crate) fn missing_loki_rules_are_empty_but_unreadable_ones_are_an_error() {
    let dir = tempfile::tempdir().expect("a temp dir");

    // Absent: no rules, no error.
    let absent = dir.path().join("absent.json");
    let tenants =
        super::super::prelude::read_loki_rule_tenants(&absent).expect("an absent file is not an error");
    check!(tenants.is_empty());

    // Present and valid: the rules come back.
    let valid = dir.path().join("valid.json");
    std::fs::write(
        &valid,
        r#"{"tenant-a":{"namespace":{"group":{"rules":[]}}}}"#,
    )
    .expect("the fixture writes");
    let tenants = super::super::prelude::read_loki_rule_tenants(&valid).expect("valid json parses");
    check!(tenants.len() == 1);
    check!(tenants.contains_key("tenant-a"));

    // Present and empty-but-valid.
    let empty = dir.path().join("empty.json");
    std::fs::write(&empty, "{}").expect("the fixture writes");
    check!(
        super::super::prelude::read_loki_rule_tenants(&empty)
            .expect("an empty object parses")
            .is_empty()
    );

    // Present and malformed: an error, NOT an empty set. Returning empty
    // here would silently stop alerting on every rule in the file.
    let malformed = dir.path().join("malformed.json");
    std::fs::write(&malformed, "{not json").expect("the fixture writes");
    check!(matches!(
        super::super::prelude::read_loki_rule_tenants(&malformed),
        Err(super::super::prelude::LokiRuleStoreError::Json { .. })
    ));

    // A directory where a file was expected is an I/O error, which is how
    // a non-NotFound failure is reached without special privileges.
    check!(matches!(
        super::super::prelude::read_loki_rule_tenants(dir.path()),
        Err(super::super::prelude::LokiRuleStoreError::Io { .. })
    ));
}
