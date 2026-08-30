
pub(crate) fn failure_message(
    header_fail: bool,
    expect_fail_message: Option<String>,
) -> Option<String> {
    match (header_fail, expect_fail_message) {
        (_, Some(message)) => Some(message),
        (true, None) => Some(String::new()),
        (false, None) => None,
    }
}
