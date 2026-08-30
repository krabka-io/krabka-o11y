use super::{Result, TestFile, TestParser};

/// Parses the legacy Prometheus `.test` DSL subset that the conformance harness uses.
///
/// # Errors
///
/// Returns [`PromqlError::Parse`] when the input is not valid legacy `.test` DSL.
pub fn parse_test_file(src: &str) -> Result<TestFile> {
    let mut parser = TestParser::new(src);
    parser.parse_file()
}
