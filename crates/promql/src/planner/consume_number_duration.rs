
pub(crate) fn consume_number_duration(chars: &[char], mut index: usize) -> usize {
    while index < chars.len() && (chars[index].is_ascii_digit() || chars[index] == '.') {
        index += 1;
    }
    while index < chars.len() && chars[index].is_ascii_alphabetic() {
        index += 1;
        while index < chars.len() && (chars[index].is_ascii_digit() || chars[index] == '.') {
            index += 1;
        }
    }
    index
}
