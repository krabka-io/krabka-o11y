use super::*;

#[test]
pub(crate) fn non_json_lines_still_use_logfmt_mining() {
    assert_eq!(
        log_line_pattern("status=500 user=100 route=/checkout"),
        "status=<_> user=<_> route=/checkout"
    );
    // A line that merely starts with `{` but is not valid JSON falls back.
    assert_eq!(log_line_pattern("{not json ts=1"), "{not json ts=<_>");
}
