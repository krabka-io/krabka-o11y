pub(crate) fn parse_non_empty_string(value: &str) -> Result<String, String> {
    refined_type::rule::NonEmptyString::new(value.to_owned())
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}
