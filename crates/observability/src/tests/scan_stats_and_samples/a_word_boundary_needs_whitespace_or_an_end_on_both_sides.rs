use super::*;

/// `has_word_boundary` asks whether a match at `index` stands alone rather
/// than sitting inside a longer word. Both sides have to hold, so each is
/// broken on its own -- and the ends of the string count as boundaries,
/// which is what `is_none_or` is doing there.
#[test]
pub(crate) fn a_word_boundary_needs_whitespace_or_an_end_on_both_sides() {
    let boundary = super::super::prelude::has_word_boundary;

    check!(boundary("a and b", 2, 3), "space either side");
    check!(boundary("and", 0, 3), "both ends of the string");
    check!(boundary("and b", 0, 3), "the start, and a space after");
    check!(boundary("a and", 2, 3), "a space before, and the end");

    // Each side broken on its own.
    check!(!boundary("aand b", 1, 3), "no boundary before");
    check!(!boundary("a andb", 2, 3), "no boundary after");
    check!(!boundary("aandb", 1, 3), "neither side");
}
