use super::Limits;

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
