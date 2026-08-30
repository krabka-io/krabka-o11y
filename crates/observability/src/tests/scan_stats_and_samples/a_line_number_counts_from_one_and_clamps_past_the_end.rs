use super::*;

/// `line_number` counts the newlines before a position, one-based, and
/// clamps a position past the end rather than panicking on it -- a parse
/// error can report a position at the very end of the input.
#[test]
pub(crate) fn a_line_number_counts_from_one_and_clamps_past_the_end() {
    let line = super::super::prelude::line_number;

    check!(line("abc", 0) == 1, "the first line is one, not zero");
    check!(line("abc", 3) == 1);
    check!(line("a\nb", 0) == 1);
    check!(line("a\nb", 2) == 2, "past the newline");
    check!(line("a\nb", 1) == 1, "the newline itself is still line one");
    check!(line("a\n\nb", 3) == 3, "a blank line counts");
    check!(line("a\nb", 99) == 2, "a position past the end clamps");
    check!(line("", 0) == 1);
    check!(line("", 99) == 1);
}
