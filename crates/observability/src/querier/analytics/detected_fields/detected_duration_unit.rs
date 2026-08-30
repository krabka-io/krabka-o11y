pub(crate) fn detected_duration_unit(unit: &str) -> Option<(u8, u16)> {
    match unit {
        "y" => Some((0, 1 << 0)),
        "w" => Some((1, 1 << 1)),
        "d" => Some((2, 1 << 2)),
        "h" => Some((3, 1 << 3)),
        "m" => Some((4, 1 << 4)),
        "s" => Some((5, 1 << 5)),
        "ms" => Some((6, 1 << 6)),
        "us" => Some((7, 1 << 7)),
        "ns" => Some((8, 1 << 8)),
        _ => None,
    }
}
