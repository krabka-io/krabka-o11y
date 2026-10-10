//! The query-transition evidence that the metrics and traces deployment
//! suites leave behind for review.

use serde_json::{Value, json};

/// Appends `case` to `evidence` and, under Bazel, rewrites `filename` in the
/// test's undeclared outputs with every case recorded so far.
///
/// The file is rewritten before the case's own outcome is checked, so a
/// failing run still leaves the case that failed it.
pub fn record_evidence(
    evidence: &mut Vec<Value>,
    case: Value,
    filename: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    evidence.push(case);
    if let Some(output) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        let output = std::path::PathBuf::from(output);
        std::fs::create_dir_all(&output)?;
        std::fs::write(
            output.join(filename),
            serde_json::to_vec_pretty(&json!({"cases":evidence}))?,
        )?;
    }
    Ok(())
}
