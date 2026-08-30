pub(crate) fn value_bounds(points: &[(i64, i64)]) -> (i64, i64) {
    let Some((_, first)) = points.first() else {
        return (0, 0);
    };
    points
        .iter()
        .fold((*first, *first), |(min, max), (_, value)| {
            (min.min(*value), max.max(*value))
        })
}
