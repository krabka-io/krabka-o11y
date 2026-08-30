use super::*;

pub(crate) fn keep_infinite_mean(mean: f64, value: f64) -> bool {
    mean.is_infinite()
        && ((value.is_infinite() && value.is_sign_positive() == mean.is_sign_positive())
            || (!value.is_infinite() && !value.is_nan()))
}
