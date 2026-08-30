/// Parses a comma-separated id list. Empty entries are allowed so a trailing
/// comma is harmless, but an entry that is not a number is a mistake in the
/// case file and stops the run: dropping it would silently shorten the list
/// the case is asserting against.
pub(crate) fn parse_u8_list(value: Option<&str>) -> Vec<u8> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse()
                .unwrap_or_else(|_| panic!("`{item}` is not a valid id in list {value:?}"))
        })
        .collect()
}
