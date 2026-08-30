use super::*;

/// Builds the NaN-ignoring `prom_min` or `prom_max` aggregate UDAF.
#[must_use]
pub(crate) fn extremum_udaf(extremum: Extremum) -> AggregateUDF {
    let name = match extremum {
        Extremum::Min => PROM_MIN_UDAF_NAME,
        Extremum::Max => PROM_MAX_UDAF_NAME,
    };
    create_udaf(
        name,
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(move |_args: AccumulatorArgs| {
            Ok(Box::new(PromExtremumAccumulator::new(extremum)) as Box<dyn Accumulator>)
        }),
        // (running extremum, seen flag) intermediate state.
        Arc::new(vec![DataType::Float64, DataType::Boolean]),
    )
}
