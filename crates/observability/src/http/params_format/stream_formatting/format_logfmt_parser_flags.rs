use super::*;

pub(crate) fn format_logfmt_parser_flags(config: &LogfmtParserConfig) -> String {
    let mut flags = Vec::new();
    if config.keep_empty() {
        flags.push("--keep-empty");
    }
    if config.strict() {
        flags.push("--strict");
    }
    if flags.is_empty() {
        String::new()
    } else {
        format!(" {}", flags.join(" "))
    }
}
