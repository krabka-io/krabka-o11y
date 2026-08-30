use super::*;

pub(crate) fn format_go_time_layout(layout: &str, timestamp: OffsetDateTime) -> String {
    let mut formatted = String::new();
    let mut rest = layout;
    while !rest.is_empty() {
        if let Some(next) = rest.strip_prefix("2006") {
            let _ = write!(formatted, "{:04}", timestamp.year());
            rest = next;
        } else if let Some(next) = rest.strip_prefix("06") {
            let _ = write!(formatted, "{:02}", timestamp.year().rem_euclid(100));
            rest = next;
        } else if let Some(next) = rest.strip_prefix("15") {
            let _ = write!(formatted, "{:02}", timestamp.hour());
            rest = next;
        } else if let Some(next) = rest.strip_prefix("04") {
            let _ = write!(formatted, "{:02}", timestamp.minute());
            rest = next;
        } else if let Some(next) = rest.strip_prefix("05") {
            let _ = write!(formatted, "{:02}", timestamp.second());
            rest = next;
        } else if let Some(next) = rest.strip_prefix("01") {
            let _ = write!(formatted, "{:02}", u8::from(timestamp.month()));
            rest = next;
        } else if let Some(next) = rest.strip_prefix('1') {
            formatted.push_str(&u8::from(timestamp.month()).to_string());
            rest = next;
        } else if let Some(next) = rest.strip_prefix("02") {
            let _ = write!(formatted, "{:02}", timestamp.day());
            rest = next;
        } else if let Some(next) = rest.strip_prefix('2') {
            formatted.push_str(&timestamp.day().to_string());
            rest = next;
        } else if let Some(next) = rest.strip_prefix("Z07:00") {
            formatted.push('Z');
            rest = next;
        } else if let Some(next) = rest.strip_prefix("-07:00") {
            formatted.push_str("+00:00");
            rest = next;
        } else if let Some(fraction_rest) = rest.strip_prefix('.') {
            let digits = fraction_rest
                .chars()
                .take_while(|ch| *ch == '0' || *ch == '9')
                .count();
            if digits == 0 {
                formatted.push('.');
                rest = fraction_rest;
                continue;
            }
            let fraction = format!("{:09}", timestamp.nanosecond());
            formatted.push('.');
            formatted.push_str(&fraction[..digits.min(fraction.len())]);
            rest = &fraction_rest[digits..];
        } else {
            let ch = rest.chars().next().expect("layout rest is not empty");
            formatted.push(ch);
            rest = &rest[ch.len_utf8()..];
        }
    }
    formatted
}
