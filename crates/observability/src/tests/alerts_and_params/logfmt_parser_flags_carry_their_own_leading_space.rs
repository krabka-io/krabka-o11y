use super::*;

/// `format_logfmt_parser_flags` renders a parser's options back into the
/// query text. The leading space belongs to the FLAGS, not to the caller:
/// with no flags the string is empty rather than a lone space, which would
/// otherwise leave a trailing space in every query without options.
#[test]
pub(crate) fn logfmt_parser_flags_carry_their_own_leading_space() {
    use krabka_logql::{LogfmtExtraction, LogfmtParserConfig};

    // The flags are only accepted alongside an extraction, so every
    // config here names one.
    let flags = |strict, keep_empty| {
        let extraction = LogfmtExtraction::same("level").expect("a valid extraction");
        let config = LogfmtParserConfig::with_options(vec![extraction], strict, keep_empty)
            .expect("the options are valid");
        super::super::prelude::format_logfmt_parser_flags(&config)
    };

    check!(flags(false, false) == "", "no flags, no space");
    check!(flags(true, false) == " --strict");
    check!(flags(false, true) == " --keep-empty");
    check!(
        flags(true, true) == " --keep-empty --strict",
        "both, in a fixed order, sharing one leading space"
    );
}
