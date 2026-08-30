pub(crate) fn parse_positive_usize(value: &str) -> Result<usize, String> {
    use refined_type::rule::GreaterUsize;

    GreaterUsize::<0>::new(value.parse::<usize>().map_err(|error| error.to_string())?)
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}
