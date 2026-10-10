use super::{ExtendedSelectorModifier, RangeSeries, Time, annotations::TypeAndUnitLabels};

/// An evaluated range vector: its series and the window they were read over.
pub(crate) struct RangeEval {
    pub(crate) series: Vec<RangeSeries>,
    pub(crate) window: RangeWindow,
}

/// The window a range vector covers, and how range functions over it annotate.
#[derive(Clone, Copy)]
pub(crate) struct RangeWindow {
    /// Where the window ends, after any `@` modifier and offset, in milliseconds.
    pub(crate) end_ms: i64,
    /// The window's width.
    pub(crate) range: Time,
    /// The selector's `anchored` or `smoothed` modifier, if any.
    pub(crate) modifier: Option<ExtendedSelectorModifier>,
    /// Whether the non-counter info reads the `__type__` label.
    pub(crate) type_and_unit_labels: TypeAndUnitLabels,
}
