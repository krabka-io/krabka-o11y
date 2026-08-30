pub(crate) fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch == '-' || ch.is_alphanumeric()
}
