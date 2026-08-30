use super::*;

pub(crate) fn parse_positive_usize(value: &str) -> Result<usize, String> {
    use refined_type::rule::GreaterUsize;

    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    GreaterUsize::<0>::new(value)
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}
