use super::pb;

pub(crate) fn resolve_service_name(rp: &pb::otlp_profiles::ResourceProfiles) -> String {
    use pb::opentelemetry::proto::common::v1::any_value::Value;

    let Some(resource) = &rp.resource else {
        return "unknown_service".to_string();
    };
    for attr in &resource.attributes {
        if attr.key == "service.name"
            && let Some(value) = &attr.value
            && let Some(Value::StringValue(service)) = &value.value
            && !service.is_empty()
        {
            return service.clone();
        }
    }
    "unknown_service".to_string()
}
