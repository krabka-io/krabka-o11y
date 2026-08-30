use super::*;

pub(crate) fn detect_log_level(line: &str) -> Option<&'static str> {
    let line = line.to_ascii_lowercase();
    for level in [
        "critical", "crit", "fatal", "error", "warn", "warning", "info", "debug", "trace",
    ] {
        if contains_log_level_token(&line, level) {
            return Some(match level {
                "crit" => "critical",
                "warning" => "warn",
                level => level,
            });
        }
    }
    None
}
