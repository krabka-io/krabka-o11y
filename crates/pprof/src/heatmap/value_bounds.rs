use crate::TimestampTotalPoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValueBounds {
    pub min: i64,
    pub max: i64,
}

pub(crate) fn value_bounds(points: &[TimestampTotalPoint]) -> ValueBounds {
    let Some(first) = points.first() else {
        return ValueBounds { min: 0, max: 0 };
    };
    points.iter().fold(
        ValueBounds {
            min: first.total,
            max: first.total,
        },
        |bounds, point| ValueBounds {
            min: bounds.min.min(point.total),
            max: bounds.max.max(point.total),
        },
    )
}
