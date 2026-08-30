use super::*;

pub(crate) fn contains_log_level_token(line: &str, level: &str) -> bool {
    line.match_indices(level).any(|(start, _)| {
        let end = start + level.len();
        let before = start
            .checked_sub(1)
            .and_then(|index| line.as_bytes().get(index))
            .copied();
        let after = line.as_bytes().get(end).copied();
        !before.is_some_and(is_log_level_word_byte) && !after.is_some_and(is_log_level_word_byte)
    })
}
