use super::{AttrValue, Attribute};

pub(crate) struct GroupedAttributes<'a> {
    pub(crate) key: &'a str,
    pub(crate) values: Vec<&'a AttrValue>,
}

pub(crate) fn group_attrs(attrs: &[Attribute]) -> Vec<GroupedAttributes<'_>> {
    let mut grouped: Vec<GroupedAttributes<'_>> = Vec::new();
    for attribute in attrs {
        if let Some(group) = grouped.iter_mut().find(|group| group.key == attribute.key) {
            group.values.push(&attribute.value);
        } else {
            grouped.push(GroupedAttributes {
                key: &attribute.key,
                values: vec![&attribute.value],
            });
        }
    }
    grouped
}
