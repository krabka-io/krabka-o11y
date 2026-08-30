use super::*;

pub(crate) fn prometheus_rule_response(rule: &serde_yaml::Value) -> Option<Value> {
    let fields = loki_yaml_mapping(rule)?;
    let query = yaml_string_field(fields, "expr")?;
    if let Some(name) = yaml_string_field(fields, "alert") {
        let mut rule = json!({
            "type": "alerting",
            "name": name,
            "query": query,
            "duration": yaml_duration_seconds_field(fields, "for").unwrap_or(0),
            "labels": yaml_string_map_field(fields, "labels"),
            "annotations": yaml_string_map_field(fields, "annotations"),
            "alerts": [],
            "health": "ok",
        });
        remove_empty_object_field(&mut rule, "labels");
        remove_empty_object_field(&mut rule, "annotations");
        return Some(rule);
    }
    yaml_string_field(fields, "record").map(|name| {
        let mut rule = json!({
            "type": "recording",
            "name": name,
            "query": query,
            "labels": yaml_string_map_field(fields, "labels"),
            "health": "ok",
        });
        remove_empty_object_field(&mut rule, "labels");
        rule
    })
}
