pub(crate) fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch == ':' || ch == '.' || ch.is_ascii_alphabetic()
}
