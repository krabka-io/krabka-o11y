use super::*;

#[test]
pub(crate) fn float_formatting_matches_go() {
    // Matches Go's strconv.AppendFloat(f, fmt, -1, 64) selection used by
    // Prometheus jsonutil.MarshalFloat.
    assert2::assert!(format_sample_value(1.0) == "1");
    assert2::assert!(format_sample_value(1.5) == "1.5");
    assert2::assert!(format_sample_value(0.0) == "0");
    assert2::assert!(format_sample_value(-0.0) == "-0");
    assert2::assert!(format_sample_value(3.0) == "3");
    assert2::assert!(format_sample_value(0.5) == "0.5");
    // 1e20 stays in 'f' form (abs < 1e21).
    assert2::assert!(format_sample_value(1e20) == "100000000000000000000");
    // 1e21 is the boundary where 'e' form kicks in.
    assert2::assert!(format_sample_value(1e21) == "1e+21");
    // 1e-6 is NOT < 1e-6, so it stays in 'f' form.
    assert2::assert!(format_sample_value(1e-6) == "0.000001");
    // Just below 1e-6 switches to 'e' form.
    assert2::assert!(format_sample_value(9.999e-7) == "9.999e-07");
    assert2::assert!(format_sample_value(1.5e-7) == "1.5e-07");
    assert2::assert!(format_sample_value(f64::NAN) == "NaN");
    assert2::assert!(format_sample_value(f64::INFINITY) == "+Inf");
    assert2::assert!(format_sample_value(f64::NEG_INFINITY) == "-Inf");
    // Very long decimal: shortest round-trip representation.
    assert2::assert!(format_sample_value(0.1 + 0.2) == "0.30000000000000004");
    assert2::assert!(format_sample_value(-1234.5678) == "-1234.5678");
    // Negative exponent boundary and large magnitudes.
    assert2::assert!(format_sample_value(-1e21) == "-1e+21");
    assert2::assert!(format_sample_value(1.234e30) == "1.234e+30");
}
