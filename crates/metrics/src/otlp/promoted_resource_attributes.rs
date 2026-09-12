use super::{KeyValue, attribute_value, string_attribute};

pub(crate) fn promoted_resource_attributes(
    resource_attributes: &[KeyValue],
    additional_attributes: &[String],
) -> Vec<KeyValue> {
    let value = |name: &str| {
        resource_attributes
            .iter()
            .find(|attribute| attribute.key == name)
            .and_then(|attribute| attribute_value(attribute.value.as_ref()))
    };

    let mut promoted = Vec::new();
    if let Some(service_name) = value("service.name") {
        let job = value("service.namespace").map_or_else(
            || service_name.clone(),
            |namespace| format!("{namespace}/{service_name}"),
        );
        promoted.push(string_attribute("job", &job));
    }
    if let Some(instance) = value("service.instance.id") {
        promoted.push(string_attribute("instance", &instance));
    }
    promoted.extend(
        additional_attributes
            .iter()
            .filter_map(|name| {
                resource_attributes
                    .iter()
                    .find(|attribute| attribute.key == *name)
            })
            .cloned(),
    );
    promoted
}
