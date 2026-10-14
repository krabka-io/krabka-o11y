use super::{Labels, yaml_string_entries};

pub(crate) fn yaml_string_labels_field(fields: &serde_yaml::Mapping, name: &'static str) -> Labels {
    yaml_string_entries(fields, name).into_iter().collect()
}
