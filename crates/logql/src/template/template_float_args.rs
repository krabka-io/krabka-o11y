pub(crate) fn template_float_args(args: &[String]) -> Vec<f64> {
    args.iter()
        .map(|value| value.parse::<f64>().unwrap_or_default())
        .collect()
}
