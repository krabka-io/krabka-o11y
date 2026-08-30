use super::*;

/// `signed_vector_function_literal_error` catches `vector(+1)`, which `LogQL`
/// does not accept -- the argument must be a bare number. It skips any
/// whitespace after the parenthesis before looking, so the reported column
/// is the SIGN's, not the parenthesis's, and the message names which sign
/// was found.
///
/// As with the unspaced-operator detector, the column counts characters:
/// one case puts multi-byte text ahead of the call so a byte offset gives
/// a different number.
#[test]
pub(crate) fn a_signed_vector_literal_is_reported_at_the_sign() {
    let error = super::super::prelude::signed_vector_function_literal_error;
    let column = |query: &str| {
        error(query).map(|message| {
            message
                .split("col ")
                .nth(1)
                .and_then(|rest| rest.split(':').next())
                .expect("the message names a column")
                .parse::<usize>()
                .expect("the column is a number")
        })
    };

    check!(column("vector(+1)") == Some(8));
    check!(column("vector(-1)") == Some(8));

    // Whitespace after the parenthesis is skipped, so the column follows
    // the sign rather than sitting on the bracket.
    check!(column("vector( +1)") == Some(9));
    check!(column("vector(   -1)") == Some(11));

    // The message names the sign it found, not a fixed one.
    check!(
        error("vector(+1)")
            .expect("a signed literal is an error")
            .contains("unexpected +, expecting NUMBER")
    );
    check!(
        error("vector(-1)")
            .expect("a signed literal is an error")
            .contains("unexpected -, expecting NUMBER")
    );

    // An unsigned argument is fine, and so is anything that is not a sign.
    check!(error("vector(1)").is_none());
    check!(error("vector( 1)").is_none());
    check!(error("vector(x)").is_none());
    check!(error("vector()").is_none());

    // Characters, not bytes: fourteen characters precede the sign here
    // but sixteen bytes do.
    check!(column("(\"\u{e9}\u{e9}\")+vector(-1)") == Some(15));

    // A `vector(` inside a string is text.
    check!(error("(\"vector(+1)\")").is_none());

    // The parenthesis is part of the match, not assumed to follow. Without
    // it, "vector -1" would land the offset straight on the minus and
    // report a signed literal for a call that was never made.
    check!(error("vector -1").is_none());
    check!(error("vector_total -1").is_none());
}
