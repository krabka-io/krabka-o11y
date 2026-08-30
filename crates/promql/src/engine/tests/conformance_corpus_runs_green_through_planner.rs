
/// Corpus green-through-the-public-entry-points guard.
///
/// This test runs the full conformance corpus through the public
/// `query_instant` and `query_range` entry points, the same way the conformance
/// harness does, and asserts that every file passes. The tree-walking
/// interpreter is deleted, so the operator planner is the only evaluation
/// engine reached from these entry points. A green corpus here is then a green
/// corpus through the planner. The direct totality proof lives in
/// [`plan_instant_expr_is_total_over_construct_sweep`]: every valid query plans
/// to `Ok(Some)`, every invalid one plans to `Err`, and none plans to
/// `Ok(None)`.
#[tokio::test]
pub(crate) async fn conformance_corpus_runs_green_through_planner() {
    use crate::conformance::testkit::{corpus_dir, run_corpus_dir};

    let report = run_corpus_dir(corpus_dir()).await;
    // Sanity: the corpus actually ran (no path/setup error swallowed the run).
    assert2::assert!(!report.files.is_empty());
    assert2::assert!(report.files.iter().all(|file| file.passed));
}
