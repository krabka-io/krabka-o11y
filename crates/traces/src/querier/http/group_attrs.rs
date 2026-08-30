use super::AttrValue;

pub(crate) fn group_attrs(attrs: &[(String, AttrValue)]) -> Vec<(&str, Vec<&AttrValue>)> {
    let mut grouped: Vec<(&str, Vec<&AttrValue>)> = Vec::new();
    for (key, value) in attrs {
        if let Some((_, values)) = grouped
            .iter_mut()
            .find(|(existing_key, _)| existing_key == key)
        {
            values.push(value);
        } else {
            grouped.push((key.as_str(), vec![value]));
        }
    }
    grouped
}
