use super::*;

pub(crate) fn new_str_list_list() -> ListBuilder<ListBuilder<StringBuilder>> {
    ListBuilder::new(new_str_list())
}
