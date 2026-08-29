use std::path::Path;

use assert2::assert;

fn golden_case_file(path: &Path) -> datatest_stable::Result<()> {
    std::fs::metadata(path)?;
    let report = krabka_traceql::testkit::run_corpus_file(path);
    println!("{}", report.to_text());

    let failing = report
        .cases
        .iter()
        .filter(|case| !case.passed)
        .collect::<Vec<_>>();

    assert!(
        failing.is_empty(),
        "traceql golden corpus failures: {failing:?}"
    );

    // A corpus that ran nothing satisfies the assertion above trivially: no
    // case failed because no case ran. Every one of these files carries at
    // least one case, and every case at least one assertion, so a testkit that
    // silently stopped parsing -- or that returned a case with nothing checked
    // -- would otherwise report a clean run over an engine it never exercised.
    assert!(
        !report.cases.is_empty(),
        "{} parsed no cases",
        path.display()
    );
    let assertions: usize = report.cases.iter().map(|case| case.total_assertions).sum();
    assert!(
        assertions > 0,
        "{} ran {} case(s) with no assertions between them",
        path.display(),
        report.cases.len()
    );
    Ok(())
}

datatest_stable::harness! {
    { test = golden_case_file, root = "tests/testdata/traceql", pattern = r".*\.case$" },
}
