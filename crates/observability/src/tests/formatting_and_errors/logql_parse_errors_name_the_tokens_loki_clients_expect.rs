use super::*;

/// The two `LogQL` token namers turn a parser's own wording into the token
/// names `Loki`'s clients expect. Each named arm falls through to a generic
/// rewrite when deleted, so every one is pinned to its own answer.
#[test]
pub(crate) fn logql_parse_errors_name_the_tokens_loki_clients_expect() {
    let expected = super::super::prelude::expected_logql_token;
    let unexpected = super::super::prelude::unexpected_logql_token;

    check!(expected("expected '\"'") == "STRING");
    check!(expected("expected closing quote") == "STRING");
    check!(expected("expected label matcher operator") == "ASSIGN, EQ, NEQ, RE, NRE");
    check!(expected("expected label name") == "IDENTIFIER");
    check!(expected("expected end of query") == "$end");

    // Anything else keeps its wording with the lead-in stripped, which is
    // what a deleted arm above would fall through to.
    check!(expected("expected a pipeline stage") == "a pipeline stage");
    check!(expected("something else entirely") == "something else entirely");

    // An underscore starts an identifier just as a letter does, which is
    // the case separating `==` from `!=` in that test.
    check!(unexpected("_foo", 0) == "IDENTIFIER");
    check!(unexpected("foo", 0) == "IDENTIFIER");
    check!(
        unexpected("{app=\"a\"}", 0) == "{",
        "punctuation names itself"
    );
    check!(
        unexpected("1", 0) == "1",
        "and a digit is not an identifier"
    );
    check!(unexpected("", 0) == "$end");
    check!(
        unexpected("abc", 99) == "$end",
        "a position past the end is the end"
    );
}
