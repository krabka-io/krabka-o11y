//! Source titleCaser.`Transform`: first cased scalar after a Break (or two Mids)
//! receives the full title mapping; `NoLower` copies subsequent scalars unchanged.
#[path = "title_data.rs"]
mod data;

pub(super) fn format(bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    let mut mid_word = false;
    let mut previous_mid = false;
    for (ch, span) in super::super::format_template_printf::raw::runes(bytes) {
        let code = u32::from(ch);
        let index = data::RANGES.partition_point(|(_, end, _)| *end < code);
        let flags = data::RANGES[index].2;
        let is_mid = flags & 4 != 0;
        if previous_mid && is_mid {
            mid_word = false;
        }
        if flags & 1 != 0 {
            if mid_word {
                output.extend(&bytes[span]);
            } else {
                if let Ok(index) = data::MAPPINGS.binary_search_by_key(&code, |(code, _)| *code) {
                    output.extend(data::MAPPINGS[index].1.as_bytes());
                } else {
                    output.extend(&bytes[span]);
                }
                mid_word = true;
            }
        } else {
            output.extend(&bytes[span]);
            if flags & 2 != 0 {
                mid_word = false;
            }
        }
        previous_mid = is_mid;
    }
    output
}
