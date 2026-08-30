use super::*;

/// `scalar_literal_len` reports how many bytes at the front of `input`
/// form a number, so the caller can resume after it. It is a scanner, not
/// a parser: it must stop at the first byte that cannot extend the
/// literal, and refuse anything that is not one.
#[test]
pub(crate) fn a_scalar_literal_ends_where_the_number_does() {
    let len = super::super::prelude::scalar_literal_len;

    check!(len("1") == Some(1));
    check!(len("1234") == Some(4));
    check!(len("+1") == Some(2), "a leading sign counts");
    check!(len("-1") == Some(2));

    // A fraction may sit on either side of the point.
    check!(len("1.5") == Some(3));
    check!(len(".5") == Some(2), "no whole part is still a number");
    check!(len("1.") == Some(2), "a trailing point ends the literal");
    check!(len("+.5") == Some(3));

    // An exponent takes an optional sign and needs at least one digit.
    check!(len("1e5") == Some(3));
    check!(len("1e+5") == Some(4));
    check!(len("1e-5") == Some(4));
    check!(len("1.5e10") == Some(6));
    check!(len("1E5") == Some(3), "an exponent may be upper case");
    check!(len("1E-5") == Some(4));
    check!(
        len("1e") == None,
        "an exponent with no digits is not a number"
    );
    check!(len("1e+") == None);

    // Nothing that is not a number.
    check!(len("") == None);
    check!(len(".") == None, "a bare point has no digits either side");
    check!(len("+") == None);
    check!(len("abc") == None);

    // The scan stops at the first byte it cannot use, rather than
    // rejecting the whole input.
    check!(len("1abc") == Some(1));
    check!(len("1.5]") == Some(3));
    check!(len("1e5x") == Some(3));
}
