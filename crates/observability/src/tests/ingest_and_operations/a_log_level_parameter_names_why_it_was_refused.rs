use super::*;

/// `parse_log_level_param` accepts the four levels and refuses everything
/// else BY NAME, so the caller can tell "you sent a level I do not know"
/// from "you sent no level". It returns on the first `log_level` it finds,
/// which is what decides precedence when the handler merges two sources.
#[test]
pub(crate) fn a_log_level_parameter_names_why_it_was_refused() {
    let parse = |query: &str| super::super::prelude::parse_log_level_param(Some(query));

    for level in ["debug", "info", "warn", "error"] {
        check!(parse(&format!("log_level={level}")).ok().as_deref() == Some(level));
    }

    // The first occurrence wins, which the handler relies on.
    check!(parse("log_level=info&log_level=warn").ok().as_deref() == Some("info"));
    // And other parameters are skipped rather than ending the search.
    check!(parse("other=1&log_level=warn").ok().as_deref() == Some("warn"));
    check!(parse("log_level=warn&other=1").ok().as_deref() == Some("warn"));

    // Percent and plus escapes are decoded before matching, in the key as
    // well as the value.
    check!(parse("log%5Flevel=warn").ok().as_deref() == Some("warn"));

    // The two refusals are distinct: an unrecognised level names what was
    // sent, a missing one says the parameter was absent.
    check!(matches!(
        parse("log_level=verbose"),
        Err(HttpQueryError::InvalidQueryParameter {
            name: "log_level",
            ..
        })
    ));
    check!(
        matches!(
            parse("log_level="),
            Err(HttpQueryError::InvalidQueryParameter { .. }),
        ),
        "an empty value is an unrecognised level, not an absent parameter"
    );
    check!(matches!(
        parse("other=1"),
        Err(HttpQueryError::MissingQueryParameter("log_level"))
    ));
    check!(matches!(
        parse(""),
        Err(HttpQueryError::MissingQueryParameter("log_level"))
    ));
    check!(matches!(
        super::super::prelude::parse_log_level_param(None),
        Err(HttpQueryError::MissingQueryParameter("log_level"))
    ));

    // Case matters: the levels are lower-case spellings.
    check!(parse("log_level=DEBUG").is_err());
}
