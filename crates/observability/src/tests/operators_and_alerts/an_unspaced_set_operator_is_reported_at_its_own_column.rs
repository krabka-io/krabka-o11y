use super::*;

/// `unspaced_vector_set_operator_error` catches `)and` written without a
/// space -- a `LogQL` typo that would otherwise fail somewhere unhelpful --
/// and reports the column the operator starts at.
///
/// That column is a CHARACTER count, not a byte offset, so one case puts
/// multi-byte text before the parenthesis: with ASCII alone the two are
/// the same number and a byte count passes.
#[test]
pub(crate) fn an_unspaced_set_operator_is_reported_at_its_own_column() {
    let error = super::super::prelude::unspaced_vector_set_operator_error;
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

    // All three operators, each glued to the closing parenthesis.
    check!(column("vector(1)and vector(2)") == Some(10));
    check!(column("vector(1)or vector(2)") == Some(10));
    check!(column("vector(1)unless vector(2)") == Some(10));

    // Properly spaced is not an error.
    check!(error("vector(1) and vector(2)").is_none());
    check!(error("vector(1)").is_none());

    // A closing parenthesis followed by anything else is fine.
    check!(error("vector(1)+vector(2)").is_none());

    // Unlike the set-operator SPLITTER, this detector has no word-boundary
    // test, so `)android` is reported as an unspaced `and`. That is a
    // false positive, but it fires only on a query that is already a
    // syntax error, so it turns one bad message into a better-placed one.
    // Pinned because it is behaviour, not because it is desirable.
    check!(column("vector(1)android") == Some(10));

    // The column counts characters. Six characters precede the operator
    // here but eight bytes do, because each accented letter takes two.
    check!(column("(\"\u{e9}\u{e9}\")and 1") == Some(7));

    // A `)and` inside a string is text, not an operator.
    check!(error("vector(1)").is_none());
    check!(error("(\")and\")").is_none());

    // The check only applies to scalar-vector expressions: an aggregation
    // is parsed elsewhere and must not be second-guessed here.
    check!(error("sum(rate(x[5m]))and y").is_none());
}
