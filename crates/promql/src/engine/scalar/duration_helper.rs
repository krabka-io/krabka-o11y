#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(crate) enum DurationHelper {
    Range,
    Step,
    Start,
    End,
}

#[cfg(feature = "experimental-functions")]
impl DurationHelper {
    pub(crate) fn value_ms(self) -> i64 {
        QUERY_RANGE_CONTEXT
            .try_with(|context| match self {
                Self::Range => context.end_ms.saturating_sub(context.start_ms),
                Self::Step => context.step.millis_i64(),
                Self::Start => context.start_ms,
                Self::End => context.end_ms,
            })
            .unwrap_or(0)
    }
}
