use super::*;

/// How a planner-path output batch carries its result value and labels.
///
/// The shared assembler `PromqlEngine::assemble_planned_instant` uses the shape
/// to read each variant's columns into an `InstantVector`.
pub(crate) enum InstantShape {
    /// `SeriesDivide -> SeriesNormalize -> InstantManipulate`. The output carries
    /// label columns plus `timestamp`/`value`/`sample_timestamp`. The selected
    /// sample's true timestamp stays in `sample_timestamp`. The assembler
    /// recovers the result labels from `labels_by_fp`, keyed by the row's
    /// reconstructed fingerprint.
    Selector,
    /// `... -> RangeManipulate -> Projection(labels..., prom_<fn>(...) AS value)`.
    /// The output carries label columns plus a single `value` column. The
    /// assembler reattaches the eval timestamp and drops the metric name. It also
    /// suppresses NaN rows, which are the UDF's "no value" sentinel.
    RateProjection,
    /// `... -> RangeManipulate -> Projection(labels..., prom_<fn>_over_time(...)
    /// AS value)`. The output carries label columns plus a single `value` column.
    /// The assembler reattaches the eval timestamp and suppresses NaN rows, which
    /// are the UDF's "no value" sentinel. `preserve_metric_name` keeps `__name__`
    /// only for `last_over_time`. Every other family drops it, which matches the
    /// interpreter's `eval_over_time_call`.
    OverTimeProjection { preserve_metric_name: bool },
    /// `<inner> -> Aggregate -> Projection(group_labels..., agg AS value)`. The
    /// output carries exactly the grouping label columns plus `value`. The
    /// result label set is the grouping labels, which the assembler reads
    /// straight from the batch without a fingerprint lookup. The assembler also
    /// reattaches the eval timestamp.
    Aggregate,
    /// `<leaf over already-evaluated inner vector> -> Projection(labels...,
    /// prom_<fn>([bounds...,] value) AS value)`. The output carries the
    /// metadata-free label columns plus a single `value` column, because the leaf
    /// already dropped the metric name. The assembler reads the label set
    /// straight from the batch and reattaches the eval timestamp. This shape
    /// keeps every row and does not suppress NaN rows, unlike the rate and
    /// `*_over_time` shapes. `f(NaN)` and `sqrt(-1)` render as `NaN`, which
    /// matches the interpreter, because the interpreter keeps every float sample.
    ScalarMath,
}
