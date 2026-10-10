//! The query-transition evidence that the metrics and traces deployment
//! suites leave behind for review.

use serde_json::{Value, json};

/// One query a deployment suite waited on, and how the wait ended.
pub struct QueryOutcome<'a> {
    /// What was asked, as a JSON object: the phase, the tenant, the query, and
    /// any signal-specific field such as the API path.
    pub request: Value,
    pub expected: &'a Value,
    /// The answer that matched `expected`, or why none did.
    pub outcome: &'a Result<Value, Box<dyn std::error::Error + Send + Sync>>,
}

impl QueryOutcome<'_> {
    /// The evidence case: the fields of `request`, then the expected and
    /// actual answers, whether they matched, and the error if they did not.
    pub fn evidence_case(&self) -> Value {
        let mut case = self.request.clone();
        let fields = case
            .as_object_mut()
            .expect("the request of a query outcome is a JSON object");
        fields.insert("expected".to_owned(), self.expected.clone());
        fields.insert("actual".to_owned(), json!(self.outcome.as_ref().ok()));
        fields.insert(
            "status".to_owned(),
            json!(if self.outcome.is_ok() {
                "matched"
            } else {
                "mismatch"
            }),
        );
        fields.insert(
            "error".to_owned(),
            json!(self.outcome.as_ref().err().map(ToString::to_string)),
        );
        case
    }
}

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
