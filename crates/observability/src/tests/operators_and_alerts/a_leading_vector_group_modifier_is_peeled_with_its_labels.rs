use super::*;

/// `split_leading_vector_group_modifier` peels a `group_left`/`group_right`
/// off the front of a vector-match clause, with or without a label list.
/// Four routes leave the function and each returns a different shape, so
/// each is pinned: no modifier, a bare one, one with labels, one with an
/// empty list, and an unclosed list -- which returns the query untouched
/// rather than a half-parsed modifier.
#[test]
pub(crate) fn a_leading_vector_group_modifier_is_peeled_with_its_labels() {
    let split = super::super::prelude::split_leading_vector_group_modifier;

    // No modifier: the query comes back whole.
    check!(split("foo") == (None, "foo"));
    check!(split("  foo") == (None, "foo"), "but trimmed at the front");
    check!(split("") == (None, ""));

    // A bare modifier, with the remainder handed back trimmed.
    check!(split("group_left foo") == (Some("group_left".to_string()), "foo"));
    check!(split("group_right foo") == (Some("group_right".to_string()), "foo"));

    // With labels, which are folded into the modifier's own text.
    check!(
        split("group_left(instance) foo") == (Some("group_left (instance)".to_string()), " foo")
    );
    check!(split("group_right(a,b) foo") == (Some("group_right (a,b)".to_string()), " foo"));

    // An empty label list is the bare modifier again, not "group_left ()".
    check!(split("group_left() foo") == (Some("group_left".to_string()), " foo"));

    // An unclosed label list is not a modifier at all: the query is
    // returned untouched rather than half-consumed.
    check!(split("group_left(instance foo") == (None, "group_left(instance foo"));

    // The match is a bare prefix test, not a word match, so a longer
    // identifier starting with a modifier name is split mid-word. That is
    // current behaviour rather than obviously desirable, and it is pinned
    // so a change to it is deliberate.
    //
    // The order the two modifiers are tried in cannot matter: neither is
    // a prefix of the other, so at most one can ever strip. Swapping them
    // is an equivalent mutation, not an untested one.
    check!(split("group_rightish") == (Some("group_right".to_string()), "ish"));
}
