use serde::Serializer;

pub(crate) fn serialize_proto<S: Serializer>(
    value: impl std::borrow::Borrow<f64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serialize(*value.borrow(), serializer, false)
}

pub(crate) fn serialize_attr<S: Serializer>(
    value: impl std::borrow::Borrow<f64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serialize(*value.borrow(), serializer, true)
}

fn serialize<S: Serializer>(
    value: f64,
    serializer: S,
    only_when_human_readable: bool,
) -> Result<S::Ok, S::Error> {
    if (!only_when_human_readable || serializer.is_human_readable()) && !value.is_finite() {
        serializer.serialize_str(if value.is_nan() {
            "NaN"
        } else if value.is_sign_positive() {
            "Infinity"
        } else {
            "-Infinity"
        })
    } else {
        serializer.serialize_f64(value)
    }
}
