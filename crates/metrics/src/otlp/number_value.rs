use super::{NumberDataPoint, number_data_point, ToPrimitive};

pub(crate) fn number_value(point: &NumberDataPoint) -> Option<f64> {
    match point.value {
        Some(number_data_point::Value::AsDouble(value)) => Some(value),
        Some(number_data_point::Value::AsInt(value)) => value.to_f64(),
        None => None,
    }
}
