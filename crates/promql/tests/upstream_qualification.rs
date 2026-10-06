//! Execute the complete pinned corpus and preserve every compatibility gap.

use krabka_promql::testkit::{corpus_dir, run_corpus_dir};

#[tokio::test]
async fn complete_upstream_corpus_writes_all_case_verdicts() {
    let report = run_corpus_dir(corpus_dir().join("upstream-3.14.0")).await;
    let directory = std::env::var("TEST_UNDECLARED_OUTPUTS_DIR")
        .map_or_else(|_| std::path::PathBuf::from("../../target"), Into::into);
    report
        .write_to_with_upstream(
            directory.join("promql-3.14.0-qualification.txt"),
            "3.14.0",
            "d7598b7141418fa35be2b5ec5d0fefb634199610",
        )
        .expect("write complete qualification report");
    assert2::assert!(report.files.len() == 21);
    assert2::assert!(
        report.files.iter().all(|file| {
            file.total_cases > 0
                && file.total_cases == file.cases.len()
                && file.cases.iter().all(|case| case.status != "uncovered")
        }),
        "every upstream evaluation must be parsed and attempted"
    );
    // Compatibility is assessed by the qualification report, not this execution
    // completeness check. --strict on tools/query-language-report.py requires
    // zero mismatches and zero uncovered inventory features.
}
