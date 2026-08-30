#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IngestRateBucketCap(pub(crate) usize);

impl IngestRateBucketCap {
    pub(crate) fn new(value: usize) -> Result<Self, String> {
        refined_type::rule::GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("ingest rate bucket cap: {error}"))
    }

    #[must_use]
    pub(crate) const fn get(self) -> usize {
        self.0
    }
}
