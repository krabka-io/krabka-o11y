use super::*;

pub(crate) fn should_omit_expected_logql_token(message: &str, unexpected: &str) -> bool {
    message == "expected '{'" && unexpected == "IDENTIFIER"
}
