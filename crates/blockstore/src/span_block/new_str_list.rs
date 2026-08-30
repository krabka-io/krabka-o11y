use super::*;

pub(crate) fn new_str_list() -> ListBuilder<StringBuilder> {
    ListBuilder::new(StringBuilder::new())
}
