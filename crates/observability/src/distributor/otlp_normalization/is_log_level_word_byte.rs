use super::*;

pub(crate) fn is_log_level_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}
