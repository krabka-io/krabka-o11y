pub fn sorted_trace_block_keys<'a>(keys: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut keys = keys.into_iter().map(str::to_owned).collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}
