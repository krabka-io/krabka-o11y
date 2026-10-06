use super::{Labels, Limits, LogfmtParser, level_fields, normalize_level};

mod ordered_json_level;

use ordered_json_level::find_json_level;

pub(super) fn detect_entry_level(line: &str, metadata: &Labels, limits: &Limits) -> String {
    if let Some(number) = metadata
        .get("severity_number")
        .filter(|number| !number.is_empty())
    {
        return match number.parse::<i64>() {
            Err(_) => "info",
            Ok(0) => "unknown",
            Ok(value) if value <= 4 => "trace",
            Ok(value) if value <= 8 => "debug",
            Ok(value) if value <= 12 => "info",
            Ok(value) if value <= 16 => "warn",
            Ok(value) if value <= 20 => "error",
            Ok(value) if value <= 24 => "fatal",
            Ok(_) => "unknown",
        }
        .to_string();
    }
    let trimmed = line.trim();
    let field = if trimmed.starts_with('{') && trimmed.ends_with('}') {
        find_json_level(trimmed, limits)
    } else if line.contains('=') {
        find_logfmt_level(line, limits)
    } else {
        None
    };
    if let Some(field) = field {
        let normalized = normalize_level(&field);
        if matches!(
            normalized,
            "trace" | "debug" | "info" | "warn" | "error" | "critical" | "fatal"
        ) {
            return normalized.to_string();
        }
    }
    detect_bounded_level(line).to_string()
}

fn find_logfmt_level(line: &str, limits: &Limits) -> Option<String> {
    let mut parser = LogfmtParser::new(line);
    let mut best: Option<(usize, String)> = None;
    while let Ok(Some((name, value))) = parser.next_pair_with_options(true, true) {
        if let Some(position) =
            level_fields(limits).position(|field| krabka_logql::go_string_equal_fold(field, &name))
            && best
                .as_ref()
                .is_none_or(|(previous, _)| position < *previous)
        {
            if position == 0 {
                return Some(value);
            }
            best = Some((position, value));
        }
    }
    best.map(|(_, value)| value)
}

fn detect_bounded_level(line: &str) -> &'static str {
    let line = line.to_lowercase();
    let mut first = line.len();
    let mut level = "unknown";
    for (word, candidate) in [
        ("trace", "trace"),
        ("debug", "debug"),
        ("fatal", "fatal"),
        ("critical", "critical"),
        ("error", "error"),
        ("err", "error"),
        ("warning", "warn"),
        ("warn", "warn"),
        ("info", "info"),
    ] {
        for (start, _) in line.match_indices(word) {
            let left = start
                .checked_sub(1)
                .and_then(|index| line.as_bytes().get(index));
            let right = line.as_bytes().get(start + word.len());
            if start < first
                && left.is_none_or(|byte| b" \t\n[({\"=".contains(byte))
                && right.is_none_or(|byte| b" \t\n[](){}:,!\"=".contains(byte))
            {
                first = start;
                level = candidate;
            }
        }
    }
    level
}
