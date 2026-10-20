//! How each implementation answered a query that both should reject, as the
//! rejection differentials record it.
//!
//! `loki_differential` and //crates/traces' `tempo_differential` reach this
//! file with `#[path]`, so it names only external crates.

use serde_json::{Value, json};

/// One implementation's answer to a query that it should reject.
pub struct ImplementationAnswer<'a> {
    /// `upstream` or `krabka`.
    pub implementation: &'a str,
    pub response: reqwest::Response,
}

/// The kind of rejection an HTTP status and body show, or `None` when the
/// answer is not a rejection.
pub type RejectionClassifier = fn(u16, &str) -> Option<&'static str>;

/// Reads `answer`, appends its status, its `classify` reading and its body to
/// `responses`, and returns whether it is a rejection.
pub async fn record_rejection_response(
    responses: &mut Vec<Value>,
    answer: ImplementationAnswer<'_>,
    classify: RejectionClassifier,
) -> reqwest::Result<bool> {
    let ImplementationAnswer {
        implementation,
        response,
    } = answer;
    let status = response.status().as_u16();
    let body = response.text().await?;
    let classification = classify(status, &body);
    let rejected = classification.is_some();
    responses.push(
        json!({"implementation": implementation, "http_status": status,
        "classification": classification, "body": body}),
    );
    Ok(rejected)
}
