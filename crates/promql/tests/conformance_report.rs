//! Checked-in `PromQL` corpus gate that also emits the CI artifact.

use krabka_promql::testkit::{corpus_dir, run_corpus_dir};

#[tokio::test]
async fn checked_in_corpus_is_green_and_writes_report() {
    let report = run_corpus_dir(corpus_dir()).await;
    let report_path = match std::env::var("TEST_UNDECLARED_OUTPUTS_DIR") {
        Ok(dir) => std::path::PathBuf::from(dir).join("promql-conformance-report.txt"),
        Err(_) => std::path::PathBuf::from("../../target/promql-conformance-report.txt"),
    };
    report
        .write_to(&report_path)
        .expect("write PromQL conformance report");
    assert!(!report.files.is_empty(), "the PromQL corpus did not run");
    assert!(
        report.files.iter().all(|file| file.passed),
        "PromQL conformance failures; see ../../target/promql-conformance-report.txt"
    );
}
