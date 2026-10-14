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
