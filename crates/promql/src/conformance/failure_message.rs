use super::ExpectedFailure;

pub(crate) fn failure_message(
    header_fail: bool,
    expect_fail_message: Option<ExpectedFailure>,
) -> Option<ExpectedFailure> {
    match (header_fail, expect_fail_message) {
        (_, Some(message)) => Some(message),
        (true, None) => Some(ExpectedFailure::Message(String::new())),
        (false, None) => None,
    }
}
