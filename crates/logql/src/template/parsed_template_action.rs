pub(crate) struct ParsedTemplateAction<'a> {
    pub(crate) expression: &'a str,
    pub(crate) next_pos: usize,
    pub(crate) trim_left: bool,
}
