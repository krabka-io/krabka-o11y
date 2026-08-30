use super::*;

pub(crate) fn parse_min_two_usize(value: &str) -> Result<usize, String> {
    use refined_type::rule::GreaterUsize;

    GreaterUsize::<1>::new(value.parse::<usize>().map_err(|error| error.to_string())?)
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}
