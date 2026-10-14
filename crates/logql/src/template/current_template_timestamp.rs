pub(crate) fn current_template_timestamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map_or_else(Default::default, |duration| duration.as_nanos().to_string())
}
