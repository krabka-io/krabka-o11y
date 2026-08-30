//! Prometheus label-selector helper.
//!
//! This is not a profiles query language. It only parses the matcher subset
//! carried in Pyroscope/Grafana requests.

use krabka_blockstore::{LabelMatcher, MatchOp};
use regex::Regex;

use crate::error::ProfileError;

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_blockstore::MatchOp;

    use super::*;

    #[test]
    fn parses_braced_matchers() {
        let ms =
            parse_label_selector(r#"{service_name="checkout", env=~"prod|stage", region!="eu"}"#)
                .unwrap();
        assert!(
            ms == vec![
                LabelMatcher::new("service_name", MatchOp::Eq, "checkout"),
                LabelMatcher::new("env", MatchOp::Re, "prod|stage"),
                LabelMatcher::new("region", MatchOp::Neq, "eu"),
            ]
        );
    }

    #[test]
    fn empty_selector_is_empty() {
        assert!(parse_label_selector("{}").unwrap().is_empty());
        assert!(parse_label_selector("").unwrap().is_empty());
    }

    // Grafana's pyroscope drilldown app concatenates an often-empty base filter
    // with the service name, producing leading/trailing/double commas. Real
    // Pyroscope tolerates these; we skip the empty parts rather than rejecting
    // the whole query with "empty label matcher" (which blanked every tile).
    #[test]
    fn tolerates_stray_commas() {
        for sel in [
            r#"{,service_name="broker"}"#,
            r#"{service_name="broker",}"#,
            r#"{ , service_name="broker" , }"#,
        ] {
            let ms = parse_label_selector(sel).unwrap();
            assert!(ms.len() == 1, "{sel}");
            assert!(ms[0].name == "service_name" && ms[0].value == "broker");
        }
        // Real matchers on either side of a double comma are both kept.
        assert!(
            parse_label_selector(r#"{service_name="a",,instance="i"}"#)
                .unwrap()
                .len()
                == 2
        );
        // A selector that is only commas/whitespace means match-all.
        assert!(parse_label_selector("{,}").unwrap().is_empty());
    }

    #[test]
    fn keeps_commas_inside_quoted_matcher_values() {
        let ms = parse_label_selector(r#"{service_name="api,primary",instance="pod-1"}"#).unwrap();

        assert!(
            ms == vec![
                LabelMatcher::new("service_name", MatchOp::Eq, "api,primary"),
                LabelMatcher::new("instance", MatchOp::Eq, "pod-1"),
            ]
        );
    }

    #[test]
    fn escaped_quotes_do_not_toggle_comma_splitting() {
        let ms = parse_label_selector(r#"{note="say \"hi, there\"",service_name="api"}"#).unwrap();

        assert!(
            ms == vec![
                LabelMatcher::new("note", MatchOp::Eq, "say \"hi, there\""),
                LabelMatcher::new("service_name", MatchOp::Eq, "api"),
            ]
        );
    }

    #[test]
    fn splitter_does_not_treat_backslash_outside_quotes_as_escape() {
        assert!(
            split_top_level_commas("left\\,right,tail").unwrap() == vec!["left\\", "right", "tail"]
        );
    }

    #[test]
    fn rejects_malformed() {
        assert!(parse_label_selector(r"{service_name=}").is_err());
        assert!(parse_label_selector(r#"{=~"x"}"#).is_err());
    }
}

mod parse_label_selector;
mod split_top_level_commas;
mod trim_selector;
mod unescape_quoted;

pub use parse_label_selector::parse_label_selector;
use split_top_level_commas::split_top_level_commas;
use trim_selector::trim_selector;
use unescape_quoted::unescape_quoted;
