pub(crate) fn standard_histogram_bound(index: i32, schema: i8) -> f64 {
    crate::engine::standard_histogram_bound(index, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_bounds_keep_rounding_subnormals_and_last_finite_bucket() {
        // Go 1.26.5 source results. Negative indices exercise table wrapping.
        for (index, schema, expected) in [
            (-1, 1, f64::from_bits(0x3fe6_a09e_667f_3bcc)),
            (-1, 8, f64::from_bits(0x3fef_e9d9_6b2a_23d6)),
            (1, 8, 1.002_711_275_050_202_5),
            (1024, 0, f64::MAX),
            (262_144, 8, f64::MAX),
            (1025, 0, f64::INFINITY),
            (-1074, 0, f64::from_bits(1)),
            (-1075, 0, 0.0),
        ] {
            assert2::assert!(
                standard_histogram_bound(index, schema).to_bits() == expected.to_bits()
            );
        }
    }
}
