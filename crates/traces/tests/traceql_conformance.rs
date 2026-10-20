use std::path::Path;

struct UnsupportedCase {
    suffix: &'static str,
    reason: &'static str,
}

const KNOWN_UNSUPPORTED: &[UnsupportedCase] = &[];

fn traceql_case_file(path: &Path) -> datatest_stable::Result<()> {
    let report = krabka_traceql::testkit::run_and_print_corpus_file(path)?;

    let failing = report
        .cases
        .iter()
        .filter(|case| {
            !case.passed
                && !KNOWN_UNSUPPORTED.iter().any(|unsupported| {
                    case.name.ends_with(unsupported.suffix) && !unsupported.reason.is_empty()
                })
        })
        .collect::<Vec<_>>();

    assert2::assert!(failing.is_empty());
    Ok(())
}

datatest_stable::harness! {
    { test = traceql_case_file, root = "../traceql/tests/testdata/traceql", pattern = r".*\.case$" },
}
