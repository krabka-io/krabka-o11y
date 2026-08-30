use super::*;

pub(crate) fn value_type(value: pb::otlp_profiles::ValueType) -> krabka_pprof::proto::ValueType {
    krabka_pprof::proto::ValueType {
        r#type: i64::from(value.type_strindex),
        unit: i64::from(value.unit_strindex),
    }
}
