use super::ExpectedFailure;

pub(crate) fn failure_message(
    header_fail: bool,
    expect_fail_message: Option<ExpectedFailure>,
) -> Option<ExpectedFailure> {
    if let Some(message) = expect_fail_message {
        Some(message)
    } else if header_fail {
        Some(ExpectedFailure::Message(String::new()))
    } else {
        None
    }
}
