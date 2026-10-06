pub(super) fn normalize_level(value: &str) -> &str {
    for (canonical, aliases) in [
        ("trace", &["trace", "trc"][..]),
        ("debug", &["debug", "dbg"][..]),
        ("info", &["info", "inf", "information"][..]),
        ("warn", &["warn", "wrn", "warning"][..]),
        ("error", &["error", "err"][..]),
        ("critical", &["critical"][..]),
        ("fatal", &["fatal"][..]),
    ] {
        if aliases
            .iter()
            .any(|alias| value.eq_ignore_ascii_case(alias))
        {
            return canonical;
        }
    }
    value
}
