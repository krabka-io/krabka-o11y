use krabka_units::{prelude::*, serde_units};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod enforce;
mod overrides;
pub use enforce::{DEFAULT_MAX_RATE_BUCKETS, IngestEnforcer, QueryEnforcer};
pub use overrides::{OverridesError, OverridesProvider};

/// A configured extent that must not be negative.
///
/// `human::time` accepts a signed magnitude, and `QueryEnforcer::check_range`
/// applies only a cap greater than zero. A runtime override of `"-1s"` would
/// therefore load cleanly and mean *unlimited*, but zero is the documented way
/// to turn a cap off. A rejection at parse time keeps one sentinel.
pub mod non_negative_time {
    use serde::{Deserializer, Serializer, de::Error as _};

    use crate::limits::{Time, serde_units};

    /// Writes the extent in its human form.
    ///
    /// # Errors
    ///
    /// Whatever the serializer reports for a string.
    pub fn serialize<S: Serializer>(value: &Time, serializer: S) -> Result<S::Ok, S::Error> {
        serde_units::human::time::serialize(value, serializer)
    }

    /// Reads the extent and rejects a negative one.
    ///
    /// # Errors
    ///
    /// If the value is not a human time string, or names a negative extent.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Time, D::Error> {
        let value = serde_units::human::time::deserialize(deserializer)?;
        if value < Time::default() {
            return Err(D::Error::custom(
                "query span caps cannot be negative; use 0 to disable the cap",
            ));
        }
        Ok(value)
    }
}

/// The `Option` form of [`non_negative_time`], for the sparse override struct.
///
/// The override path deserializes through `PartialLimits` and not `Limits`, so
/// the guard must exist on both or a per-tenant override slips past it. This
/// module is deserialize-only, because nothing serializes `PartialLimits`.
pub(crate) mod option_non_negative_time {
    use serde::{Deserializer, de::Error as _};

    use crate::limits::{Time, serde_units};

    /// Reads the optional extent and rejects a negative one.
    ///
    /// # Errors
    ///
    /// If the value is not a human time string, or names a negative extent.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Time>, D::Error> {
        let value = serde_units::human::option_time::deserialize(deserializer)?;
        if value.is_some_and(|value| value < Time::default()) {
            return Err(D::Error::custom(
                "query span caps cannot be negative; use 0 to disable the cap",
            ));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {

    /// A limit error's message is what a rejected client is told, so every
    /// variant has to name its own limit and what was observed. Four of the
    /// eight share the same field names and integer types, which is exactly
    /// where one variant's text would be reused for another -- so each is
    /// given a distinct pair and the numbers are looked for in the message.
    #[test]
    fn every_limit_error_names_its_own_limit_and_observation() {
        use super::LimitError;

        // Each case names the phrase, then the exact substrings that bind a
        // number to its role. Checking only that both numbers appear cannot
        // tell a message that has swapped them, and every variant here puts
        // the observation and the limit in the same sentence.
        let cases: &[(LimitError, &str, &[&str])] = &[
            (
                LimitError::IngestionRateExceeded {
                    rate: 11.0,
                    observed: 22.0,
                },
                "ingestion rate",
                &["observed 22", "limit 11"],
            ),
            (
                LimitError::MaxSeriesPerUser {
                    limit: 33,
                    observed: 44,
                },
                "active series",
                &["observed 44", "limit 33"],
            ),
            (
                LimitError::LabelNameTooLong {
                    limit: 55,
                    observed: 66,
                },
                "label name",
                &["observed 66", "limit 55"],
            ),
            (
                LimitError::LabelValueTooLong {
                    limit: 77,
                    observed: 88,
                },
                "label value",
                &["observed 88", "limit 77"],
            ),
            (
                LimitError::SamplesPerQueryExceeded {
                    limit: 99,
                    observed: 111,
                },
                "samples per query",
                &["observed 111", "limit 99"],
            ),
            (
                LimitError::SeriesPerQueryExceeded {
                    limit: 122,
                    observed: 133,
                },
                "series per query",
                &["observed 133", "limit 122"],
            ),
            (
                LimitError::QueryLookbackExceeded {
                    limit_secs: 144,
                    observed_secs: 155,
                },
                "lookback",
                &["observed 155s", "limit 144s"],
            ),
            (
                LimitError::QueryRangeTooLong {
                    limit_secs: 166,
                    observed_secs: 177,
                },
                "range too long",
                &["observed 177s", "limit 166s"],
            ),
        ];

        for (error, phrase, numbers) in cases {
            let message = error.message();
            check!(!message.is_empty(), "{error:?} said nothing");
            check!(
                message.contains(phrase),
                "{message:?} does not mention {phrase:?}"
            );
            for fragment in *numbers {
                check!(message.contains(fragment), "{message:?} omits {fragment:?}");
            }
        }

        // The message is the display text, not a fixed string: two variants
        // must not say the same thing.
        let first = LimitError::LabelNameTooLong {
            limit: 1,
            observed: 2,
        }
        .message();
        let second = LimitError::LabelValueTooLong {
            limit: 1,
            observed: 2,
        }
        .message();
        check!(
            first != second,
            "name and value limits read alike: {first:?}"
        );
    }
    use assert2::{assert, check};

    use super::*;

    /// A query span cap is a duration, and a negative one has no meaning:
    /// zero is how a cap is turned off. The deserializer rejects negatives
    /// rather than accepting one and comparing against it, which would
    /// refuse every query.
    #[test]
    fn a_query_span_cap_may_be_zero_but_not_negative() {
        // A one-field wrapper so the adapter is exercised on its own rather
        // than through every other field Limits requires.
        #[derive(serde::Deserialize)]
        struct OnlyCap {
            #[serde(with = "super::non_negative_time")]
            cap: Time,
        }

        let parse = |value: &str| {
            let json = String::from("{\"cap\":\"") + value + "\"}";
            serde_json::from_str::<OnlyCap>(&json).map(|parsed| parsed.cap)
        };

        check!(
            parse("0s").unwrap() == Time::default(),
            "zero turns the cap off"
        );
        check!(parse("1h").unwrap() == krabka_units::hours(1));
        check!(
            parse("1ms").unwrap() == krabka_units::millis(1),
            "the smallest positive cap"
        );

        for rejected in ["-1s", "-1ms", "-1h"] {
            let err = parse(rejected).unwrap_err().to_string();
            check!(
                err.contains("cannot be negative"),
                "{rejected} should be rejected, got: {err}"
            );
        }
    }

    /// The `Option` form of the same adapter, used by the sparse per-tenant
    /// override struct. It has to make the same three-way distinction as its
    /// twin above and one more: an absent field is None, which means "inherit
    /// the default", and is not the same as a present zero, which means
    /// "turn the cap off for this tenant".
    #[test]
    fn an_optional_query_span_cap_tells_absent_from_zero() {
        #[derive(serde::Deserialize)]
        struct MaybeCap {
            #[serde(default, with = "super::option_non_negative_time")]
            cap: Option<Time>,
        }

        let parse = |body: &str| serde_json::from_str::<MaybeCap>(body).map(|parsed| parsed.cap);

        // Absent and zero are different answers, and conflating them would
        // silently turn off a cap the tenant never mentioned.
        check!(parse("{}").unwrap().is_none(), "an absent cap inherits");
        check!(
            parse("{\"cap\":\"0s\"}").unwrap() == Some(Time::default()),
            "a zero cap is off"
        );
        check!(parse("{\"cap\":\"1h\"}").unwrap() == Some(krabka_units::hours(1)));
        check!(parse("{\"cap\":\"1ms\"}").unwrap() == Some(krabka_units::millis(1)));

        // Negatives are refused here too, with the same message.
        for rejected in ["-1s", "-1ms", "-1h"] {
            let body = String::from("{\"cap\":\"") + rejected + "\"}";
            let err = parse(&body).unwrap_err().to_string();
            check!(
                err.contains("cannot be negative"),
                "{rejected} should be rejected, got: {err}"
            );
        }
    }

    #[test]
    fn default_limits_are_generous_and_finite() {
        let l = Limits::default();
        check!(l.ingestion_rate > Frequency::ZERO);
        check!(l.max_global_series_per_user >= 100_000);
        check!(l.max_label_name_length == kibibytes(1));
    }

    #[test]
    fn query_span_caps_are_extents() {
        let l = Limits {
            max_query_lookback: days(1),
            max_query_length: hours(1),
            ..Limits::default()
        };
        check!(l.max_query_lookback == secs(86_400));
        check!(l.max_query_length == secs(3600));
        check!(Limits::default().max_query_lookback == Time::ZERO);
    }

    #[test]
    fn limit_errors_carry_prometheus_status_and_type() {
        let rate = LimitError::IngestionRateExceeded {
            rate: 10_000.0,
            observed: 12_000.0,
        };
        assert!(rate.http_status() == 429);

        let series = LimitError::SeriesPerQueryExceeded {
            limit: 100,
            observed: 101,
        };
        assert!(series.http_status() == 422);
        assert!(series.error_type() == "execution");

        let label = LimitError::LabelValueTooLong {
            limit: 2048,
            observed: 5000,
        };
        assert!(label.http_status() == 400);
        assert!(label.error_type() == "bad_data");
    }
}

// === split-modules: generated submodules ===
mod limit_error;
mod limits;

pub use limit_error::LimitError;
pub use limits::Limits;
