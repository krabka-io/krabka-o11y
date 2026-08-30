use super::*;

pub(crate) fn log_line_pattern(line: &str) -> String {
    // Krabka services (and every JSON-emitting collector) log compact objects
    // like `{"timestamp":"…","severity":"INFO","message":"connection opened"}`.
    // Whitespace tokenization mangles those — the quoted values contain spaces
    // and the `:` separator is invisible to the logfmt `key=value` splitter — so
    // every distinct timestamp became its own pattern. Templatize JSON lines
    // structurally instead, keeping keys and constant values while collapsing
    // variable values (timestamps, ids, numbers) to `<_>`.
    if let Some(pattern) = json_log_pattern(line) {
        return pattern;
    }
    line.split_whitespace()
        .map(log_pattern_token)
        .collect::<Vec<_>>()
        .join(" ")
}
