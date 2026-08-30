/// Parse an untrusted ELF/DWARF blob with `object::File::parse`.
///
/// This function catches any panic that the parser can trigger on a crafted
/// artifact. It returns `Ok(())` only when the bytes parse cleanly and the
/// parser does not panic.
pub(crate) fn parse_object_guarded(bytes: &[u8]) -> Result<(), String> {
    std::panic::catch_unwind(|| {
        object::File::parse(bytes)
            .map(|_| ())
            .map_err(|err| err.to_string())
    })
    .unwrap_or_else(|_| Err("panic while parsing object file".to_string()))
}
