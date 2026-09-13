use std::str::FromStr;

use super::OtlpError;

/// Prometheus translation strategy for OTLP metric and label names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TranslationStrategy {
    /// Replaces unsupported Prometheus metric and label characters with
    /// underscores, and applies the conventional suffixes such as `_total` for
    /// monotonic sums.
    #[default]
    UnderscoreEscapingWithSuffixes,
    /// Replaces unsupported Prometheus name characters with underscores.
    UnderscoreEscapingWithoutSuffixes,
    /// Keeps UTF-8 names and applies conventional metric suffixes.
    NoUtf8EscapingWithSuffixes,
    /// Keeps names and does not add metric suffixes.
    NoTranslation,
}

impl TranslationStrategy {
    #[must_use]
    pub(crate) fn escapes_names(self) -> bool {
        matches!(
            self,
            Self::UnderscoreEscapingWithSuffixes | Self::UnderscoreEscapingWithoutSuffixes
        )
    }

    #[must_use]
    pub(crate) fn adds_suffixes(self) -> bool {
        matches!(
            self,
            Self::UnderscoreEscapingWithSuffixes | Self::NoUtf8EscapingWithSuffixes
        )
    }
}

impl FromStr for TranslationStrategy {
    type Err = OtlpError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "UnderscoreEscapingWithSuffixes" => Ok(Self::UnderscoreEscapingWithSuffixes),
            "UnderscoreEscapingWithoutSuffixes" => Ok(Self::UnderscoreEscapingWithoutSuffixes),
            "NoUTF8EscapingWithSuffixes" => Ok(Self::NoUtf8EscapingWithSuffixes),
            "NoTranslation" => Ok(Self::NoTranslation),
            _ => Err(OtlpError::Invalid(
                "X-Mimir-OTLP-TranslationStrategy".into(),
                format!("invalid translation strategy `{value}`"),
            )),
        }
    }
}
