use std::time::SystemTime;

use krabka_blockstore::MatchOp;
use krabka_units::prelude::*;
use num_traits::ToPrimitive;
use promql_parser::{
    label as prom_label,
    parser::{AtModifier, Offset, VectorSelector},
};
use regex::Regex;

use crate::{PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher, error::Result};
mod at_modifier_bounds;
mod build_label_matchers;
mod compile_label_matchers;
mod compiled_label_matcher;
mod compiled_label_matchers;
mod label_value_matches;
pub(super) use at_modifier_bounds::AtModifierBounds;
use build_label_matchers::build_label_matchers;
pub(super) use compile_label_matchers::compile_label_matchers;
use compiled_label_matcher::CompiledLabelMatcher;
pub(super) use compiled_label_matchers::CompiledLabelMatchers;
use label_value_matches::{LabelValueMatcher, label_value_matches};

pub(crate) fn apply_offset_delta(time_ms: i64, offset: Time) -> Result<i64> {
    time_ms
        .checked_add(offset.millis_i64())
        .ok_or_else(|| PromqlError::Plan("offset evaluation time overflow".to_string()))
}

pub(crate) fn duration_to_i64_ms(duration: std::time::Duration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .map_err(|_| PromqlError::Plan("@ modifier timestamp is too large".to_string()))
}

pub(crate) fn info_data_label_matchers(selector: &VectorSelector) -> Result<Vec<LabelMatcher>> {
    let matcher_sets = label_matcher_sets(selector);
    let [matchers] = matcher_sets.as_slice() else {
        return Err(PromqlError::Plan(
            "info data label selector does not support or matchers".to_string(),
        ));
    };
    Ok(matchers.clone())
}

pub(crate) fn system_time_ms(time: SystemTime) -> Result<i64> {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration_to_i64_ms(duration),
        Err(error) => duration_to_i64_ms(error.duration()).and_then(|duration_ms| {
            duration_ms
                .checked_neg()
                .ok_or_else(|| PromqlError::Plan("@ modifier timestamp is too small".to_string()))
        }),
    }
}

pub(crate) fn timestamp_seconds(timestamp_ms: i64) -> f64 {
    timestamp_ms.to_f64().unwrap_or(f64::MAX) / 1000.0
}

pub(crate) fn apply_selector_time_modifier(
    time_ms: i64,
    at: Option<&AtModifier>,
    offset: Option<&Offset>,
    bounds: Option<AtModifierBounds>,
) -> Result<i64> {
    let base_time_ms = selector_at_ms(time_ms, at, bounds)?;
    apply_offset_delta(base_time_ms, selector_offset(offset)?)
}

pub(crate) fn label_matcher_sets(selector: &VectorSelector) -> Vec<Vec<LabelMatcher>> {
    if selector.matchers.or_matchers.is_empty() {
        return vec![build_label_matchers(
            selector.name.as_deref(),
            &selector.matchers.matchers,
        )];
    }

    let mut out = Vec::new();
    for matchers in &selector.matchers.or_matchers {
        out.push(build_label_matchers(selector.name.as_deref(), matchers));
    }
    out
}

pub(crate) fn labels_match(labels: &Labels, matchers: &[LabelMatcher]) -> Result<bool> {
    for matcher in matchers {
        let is_match = label_value_matches::<PromqlError>(
            labels,
            LabelValueMatcher {
                name: &matcher.name,
                op: matcher.op,
                expected: matcher.value.as_bytes(),
            },
            |label_value| {
                let regex = Regex::new(&format!("^(?s:{})$", matcher.value)).map_err(|error| {
                    PromqlError::Plan(format!(
                        "invalid label matcher regex for {}: {error}",
                        matcher.name
                    ))
                })?;
                Ok(regex.is_match(label_value))
            },
        )?;
        if !is_match {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn selector_at_ms(
    time_ms: i64,
    at: Option<&AtModifier>,
    bounds: Option<AtModifierBounds>,
) -> Result<i64> {
    let Some(at) = at else {
        return Ok(time_ms);
    };
    match at {
        AtModifier::At(time) => system_time_ms(*time),
        AtModifier::Start => Ok(bounds.map_or(time_ms, |bounds| bounds.start_ms)),
        AtModifier::End => Ok(bounds.map_or(time_ms, |bounds| bounds.end_ms)),
    }
}

/// A `PromQL` duration literal as a time extent.
///
/// The literal is a form such as `5m`, `1h`, or the `[…]` of a matrix selector.
/// The `i64`-millisecond round trip is the range check, not a unit conversion.
/// This function rejects a literal wider than [`i64::MAX`] milliseconds here,
/// instead of a silent loss of precision downstream, where the caller applies
/// the extent to millisecond instants.
pub(crate) fn selector_duration(duration: std::time::Duration) -> Result<Time> {
    i64::try_from(duration.as_millis())
        .map(Time::from_millis)
        .map_err(|_| PromqlError::Plan("range selector duration is too large".to_string()))
}

/// The signed extent by which an `offset` modifier shifts a selector's
/// evaluation instant. `offset 5m` looks 5 minutes further back, so it is a
/// negative extent.
pub(crate) fn selector_offset(offset: Option<&Offset>) -> Result<Time> {
    let Some(offset) = offset else {
        return Ok(Time::ZERO);
    };
    match offset {
        Offset::Pos(duration) => Ok(selector_duration(*duration)? * -1.0),
        Offset::Neg(duration) => Ok(selector_duration(*duration)?),
    }
}
