use serde_json::json;

use super::{Path, RecoveryError, write_report};

/// The message of a failed backup or restore.
///
/// A broker mismatch also writes its findings to `report`, when the operator
/// names one, as JSON with the same `kind` tags that a cut uses.
pub(crate) fn failure(error: &RecoveryError, report: Option<&Path>) -> String {
    let message = error.to_string();
    if let (
        RecoveryError::BrokerMismatch {
            operation,
            findings,
        },
        Some(path),
    ) = (error, report)
    {
        let failure = json!({
            "error": message,
            "operation": operation,
            "findings": findings,
        });
        if let Err(write_error) = write_report(&failure, path) {
            return format!("{message}; the failure report was not written: {write_error}");
        }
    }
    message
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_blockstore::BrokerFinding;

    use super::*;

    #[test]
    fn a_broker_mismatch_writes_its_findings_to_the_report() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("restore-report.json");
        let finding = BrokerFinding::WalOffset {
            topic: "__krabka_metrics_wal".into(),
            partition: 0,
            expected: Some(42),
            actual: Some(0),
        };
        let error = RecoveryError::BrokerMismatch {
            operation: "restore",
            findings: vec![finding],
        };
        let message = failure(&error, Some(&path));
        check!(
            message
                == "restore refused because the broker state differs from the cut: wal_offset \
                    __krabka_metrics_wal:0 expected 42 actual 0"
        );
        let written: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("report")).expect("JSON");
        check!(
            written
                == json!({
                    "error": message,
                    "operation": "restore",
                    "findings": [{
                        "kind": "wal_offset",
                        "topic": "__krabka_metrics_wal",
                        "partition": 0,
                        "expected": 42,
                        "actual": 0,
                    }],
                })
        );
    }

    #[test]
    fn another_failure_writes_no_report() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("restore-report.json");
        let message = failure(&RecoveryError::Broker("connect".into()), Some(&path));
        check!(message == "broker state could not be read: connect");
        assert!(!path.exists());
    }
}
