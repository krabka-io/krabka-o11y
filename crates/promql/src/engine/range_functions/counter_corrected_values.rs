use super::*;

pub(crate) fn counter_corrected_values(values: &[f64]) -> Option<Vec<f64>> {
    let mut out = Vec::with_capacity(values.len());
    let mut correction = 0.0;
    let mut previous = *values.first()?;
    out.push(previous);
    for &value in &values[1..] {
        if value < previous {
            correction += previous;
        }
        out.push(value + correction);
        previous = value;
    }
    Some(out)
}
