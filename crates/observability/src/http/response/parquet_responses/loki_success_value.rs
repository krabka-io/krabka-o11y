use super::*;

pub(crate) fn loki_success_value(data: impl serde::Serialize) -> Value {
    json!({
        "status": "success",
        "data": data,
    })
}
