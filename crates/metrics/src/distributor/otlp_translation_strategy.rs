use std::str::FromStr as _;

use super::{HeaderMap, OtlpError, TranslationStrategy};

pub(crate) fn otlp_translation_strategy(
    headers: &HeaderMap,
) -> Result<TranslationStrategy, OtlpError> {
    if let Some(strategy) = header(headers, "x-mimir-otlp-translationstrategy")? {
        return TranslationStrategy::from_str(strategy);
    }

    let Some(add_suffixes) = header(headers, "x-mimir-otlp-addsuffixes")? else {
        return Ok(TranslationStrategy::default());
    };
    let add_suffixes = parse_bool(add_suffixes).ok_or_else(|| {
        OtlpError::Invalid(
            "X-Mimir-OTLP-AddSuffixes".into(),
            format!("invalid boolean `{add_suffixes}`"),
        )
    })?;
    Ok(if add_suffixes {
        TranslationStrategy::UnderscoreEscapingWithSuffixes
    } else {
        TranslationStrategy::UnderscoreEscapingWithoutSuffixes
    })
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, OtlpError> {
    headers
        .get(name)
        .map(|value| {
            value.to_str().map_err(|_| {
                OtlpError::Invalid(name.into(), "the header is not valid UTF-8".into())
            })
        })
        .transpose()
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}
