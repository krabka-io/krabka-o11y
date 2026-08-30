use super::*;

/// `parse_logfmt_pairs` walks a logfmt line byte by byte: whitespace
/// separates pairs, `=` separates a key from its value, and a quoted value
/// may contain both. Every case below fixes one decision that boundary
/// takes, since a parser that is off by one still returns pairs -- just
/// the wrong ones.
#[test]
pub(crate) fn logfmt_pairs_split_on_unquoted_whitespace() {
    let parse = super::super::prelude::parse_logfmt_pairs;
    let pair = |k: &str, v: &str| (k.to_string(), v.to_string());

    check!(parse("a=1") == vec![pair("a", "1")]);
    // An unquoted value carrying letters, so a transformation of the slice
    // is visible: digits alone survive most of them unchanged.
    check!(parse("level=warn") == vec![pair("level", "warn")]);
    check!(parse("msg=hello level=warn") == vec![pair("msg", "hello"), pair("level", "warn")]);
    check!(parse("a=1 b=2") == vec![pair("a", "1"), pair("b", "2")]);
    check!(
        parse("  a=1   b=2  ") == vec![pair("a", "1"), pair("b", "2")],
        "runs of whitespace are separators, not content"
    );
    check!(parse("") == vec![], "an empty line has no pairs");
    check!(parse("   ") == vec![], "nor does whitespace alone");

    // A key with nothing after the `=` is a pair with an empty value,
    // which is not the same as the key being absent.
    check!(parse("a=") == vec![pair("a", "")]);
    check!(parse("a= b=2") == vec![pair("a", ""), pair("b", "2")]);

    // A bare token is not a pair and must not swallow the next one.
    check!(parse("bare a=1") == vec![pair("a", "1")]);
    check!(parse("a=1 bare") == vec![pair("a", "1")]);
    check!(parse("bare") == vec![]);

    // A leading `=` has an empty key, which is skipped rather than
    // recorded under an empty name.
    check!(parse("=1 a=2") == vec![pair("a", "2")]);

    // Quoted values hold what unquoted ones cannot.
    check!(
        parse(r#"a="x y""#) == vec![pair("a", "x y")],
        "whitespace inside quotes"
    );
    check!(parse(r#"a="x y" b=2"#) == vec![pair("a", "x y"), pair("b", "2")]);
    // An escape inside a quoted value. Every other quoted case here is
    // escape-free, so the two steps the escape branch takes -- over the
    // backslash and over what it protects -- were never taken at all.
    check!(
        parse(r#"a="x \"y\" z""#) == vec![pair("a", r#"x "y" z"#)],
        "an escaped quote is content, not the end of the value"
    );
    check!(
        parse(r#"a="x\\y""#) == vec![pair("a", r"x\y")],
        "an escaped backslash is one backslash"
    );
    // A backslash with nothing after it is not an escape: there is no
    // second byte to step over.
    check!(parse("a=\"x\\") == vec![pair("a", "x\\")]);

    check!(
        parse(r#"a="""#) == vec![pair("a", "")],
        "an empty quoted value"
    );
    check!(
        parse(r#"a="x\"y" b=2"#) == vec![pair("a", "x\"y"), pair("b", "2")],
        "an escaped quote does not end the value"
    );
    check!(
        parse(r#"a="x\\y""#) == vec![pair("a", "x\\y")],
        "an escaped backslash is one backslash"
    );

    // An unterminated quote runs to the end of the line rather than
    // dropping the pair.
    check!(parse(r#"a="x y"#) == vec![pair("a", "x y")]);
    // A trailing backslash has nothing to escape and is taken literally.
    check!(parse(r#"a="x\"#) == vec![pair("a", "x\\")]);
}
