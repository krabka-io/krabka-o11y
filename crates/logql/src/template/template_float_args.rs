
pub(crate) fn template_float_args(args: &[String]) -> Option<Vec<f64>> {
    args.iter()
        .map(|value| value.parse::<f64>().ok().filter(|value| value.is_finite()))
        .collect()
}
