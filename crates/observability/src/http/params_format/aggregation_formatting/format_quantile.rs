use super::*;

pub(crate) fn format_quantile(quantile: Quantile) -> String {
    ScalarSample::new(
        i128::from(quantile.numerator.0),
        u128::from(quantile.denominator.0),
    )
    .format()
}
