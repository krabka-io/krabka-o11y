/// Whether a `|~` pattern can be handed to `regexp_like` without risking a
/// wrong answer.
///
/// `DataFusion` does not always run a literal regex as a regex. When the
/// pattern parses to literal text it simplifies `regexp_like(col, '...')` into
/// `col LIKE '%...%'`, and builds that `LIKE` pattern by copying the regex's
/// literal characters across untouched -- `is_safe_for_like` in
/// `simplify_expressions::regex` rejects `%` and `_` and nothing else. A
/// backslash among them arrives at arrow's `LIKE` as arrow's own escape
/// character and swallows the character after it, so the regex `C:\\Users`
/// becomes a `LIKE` that matches `C:Users` and keeps none of the lines it
/// should. The scan may never drop a row the pipeline would have kept, so a
/// pattern that can put a literal backslash into the match stays in Rust.
///
/// Two escapes in the `regex` crate's syntax can produce one: `\\`, and a hex
/// escape such as `\x5c` or `\x{5c}`. Both are refused by looking for the
/// escape rather than for what it denotes, which over-refuses -- `\x41` is an
/// `A` and would have been fine -- and never under-refuses. `\d`, `\s`, `\w`,
/// `\.` and the rest put no backslash into the match and push down unchanged.
pub(crate) fn regex_line_filter_is_pushdown_safe(pattern: &str) -> bool {
    !pattern.contains("\\\\") && !pattern.contains("\\x")
}
