use krabka_logql::LogfmtParser;

use super::{Labels, Limits};

mod detect_entry_level;

use detect_entry_level::detect_entry_level;

// Loki 3.7.7 distributor/field_detection.go adds discovery to entry metadata,
// after validating user metadata. The immutable stream labels never change.
pub(crate) fn discover_detected_level_label(
    labels: &Labels,
    metadata: &mut Labels,
    line: &str,
    limits: &Limits,
) {
    if !limits.discover_log_levels {
        return;
    }
    if let Some(existing) = metadata.get_mut("detected_level") {
        *existing = normalize_level(existing).to_string();
        return;
    }
    let value = level_fields(limits)
        .find_map(|name| labels.get(name))
        .or_else(|| level_fields(limits).find_map(|name| metadata.get(name)))
        .map_or_else(
            || detect_entry_level(line, metadata, limits),
            |value| normalize_level(value).to_string(),
        );
    if !value.is_empty() {
        metadata.insert("detected_level".to_string(), value);
    }
}

#[cfg(test)]
mod tests;

const DEFAULT_FIELDS: &[&str] = &[
    "level",
    "LEVEL",
    "Level",
    "log.level",
    "severity",
    "SEVERITY",
    "Severity",
    "SeverityText",
    "lvl",
    "LVL",
    "Lvl",
    "severity_text",
    "Severity_Text",
    "SEVERITY_TEXT",
];

pub(super) fn level_fields(limits: &Limits) -> impl Iterator<Item = &str> {
    limits.log_level_fields.iter().map(String::as_str).chain(
        DEFAULT_FIELDS
            .iter()
            .copied()
            .filter(|_| limits.log_level_fields.is_empty()),
    )
}

pub(super) fn normalize_level(value: &str) -> &str {
    struct LevelAliases {
        canonical: &'static str,
        aliases: &'static [&'static str],
    }
    for level in [
        LevelAliases {
            canonical: "trace",
            aliases: &["trace", "trc"],
        },
        LevelAliases {
            canonical: "debug",
            aliases: &["debug", "dbg"],
        },
        LevelAliases {
            canonical: "info",
            aliases: &["info", "inf", "information"],
        },
        LevelAliases {
            canonical: "warn",
            aliases: &["warn", "wrn", "warning"],
        },
        LevelAliases {
            canonical: "error",
            aliases: &["error", "err"],
        },
        LevelAliases {
            canonical: "critical",
            aliases: &["critical"],
        },
        LevelAliases {
            canonical: "fatal",
            aliases: &["fatal"],
        },
    ] {
        if level
            .aliases
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            return level.canonical;
        }
    }
    value
}
