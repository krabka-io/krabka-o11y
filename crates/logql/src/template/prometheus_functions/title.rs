//! Source titleCaser.`Transform`: first cased scalar after a Break (or two Mids)
//! receives the full title mapping; `NoLower` copies subsequent scalars unchanged.
#[path = "title_data.rs"]
mod data;

pub(super) fn format(bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    let mut mid_word = false;
    let mut previous_mid = false;
    for rune in super::super::format_template_printf::raw::runes(bytes) {
        let code = u32::from(rune.character);
        let index = data::RANGES.partition_point(|range| range.end < code);
        let flags = data::RANGES[index].flags;
        let is_mid = flags & 4 != 0;
        if previous_mid && is_mid {
            mid_word = false;
        }
        if flags & 1 != 0 {
            if mid_word {
                output.extend(&bytes[rune.range]);
            } else {
                if let Ok(index) =
                    data::MAPPINGS.binary_search_by_key(&code, |mapping| mapping.code)
                {
                    output.extend(data::MAPPINGS[index].replacement.as_bytes());
                } else {
                    output.extend(&bytes[rune.range]);
                }
                mid_word = true;
            }
        } else {
            output.extend(&bytes[rune.range]);
            if flags & 2 != 0 {
                mid_word = false;
            }
        }
        previous_mid = is_mid;
    }
    output
}
