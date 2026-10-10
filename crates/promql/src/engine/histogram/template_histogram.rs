use std::collections::BTreeMap;

use krabka_logql::{
    TemplateBucket, TemplateBucketIterator, TemplateData as V, TemplateFloatHistogram as Dto,
    TemplateHistogram, TemplateHistogramBucket, TemplateHistogramError, TemplateHistogramSpan,
    TemplateHistogramView as View,
};

use super::{
    NativeHistogram, ResetHint, native_histogram_detect_reset, spanned_histogram_counts,
    trim_native_histogram,
};

pub(crate) fn template_histogram_value(histogram: NativeHistogram) -> V {
    V::FloatHistogram(TemplateHistogram::new(dto(histogram), call))
}
fn dto(histogram: NativeHistogram) -> Dto {
    let display_buckets = crate::http_api::native_histogram_buckets(&histogram)
        .into_iter()
        .map(|bucket| TemplateHistogramBucket {
            lower: bucket.lower,
            upper: bucket.upper,
            count: bucket.count,
            boundary_rule: bucket.boundary_rule,
        })
        .collect();
    let spans = |spans: Vec<krabka_metrics::BucketSpan>| {
        spans
            .into_iter()
            .map(|span| TemplateHistogramSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect()
    };
    let nil_slices = u8::from(histogram.positive_spans.is_empty())
        | (u8::from(histogram.negative_spans.is_empty()) << 1)
        | (u8::from(histogram.positive_counts.is_empty()) << 2)
        | (u8::from(histogram.negative_counts.is_empty()) << 3)
        | (u8::from(histogram.custom_values.is_none()) << 4);
    Dto {
        counter_reset_hint: u8::try_from(histogram.reset_hint.as_i8())
            .expect("reset hints are in 0..=3"),
        schema: i32::from(histogram.schema),
        zero_threshold: histogram.zero_threshold,
        zero_count: histogram.zero_count,
        count: histogram.count,
        sum: histogram.sum,
        positive_spans: spans(histogram.positive_spans),
        negative_spans: spans(histogram.negative_spans),
        positive_buckets: histogram.positive_counts,
        negative_buckets: histogram.negative_counts,
        custom_values: histogram.custom_values.unwrap_or_default(),
        display_buckets,
        nil_slices,
    }
}
fn native(dto: Dto) -> NativeHistogram {
    let spans = |spans: Vec<TemplateHistogramSpan>| {
        spans
            .into_iter()
            .map(|span| krabka_metrics::BucketSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect()
    };
    NativeHistogram {
        schema: i8::try_from(dto.schema).expect("histograms have native schemas"),
        is_float: true,
        reset_hint: ResetHint::from_i8(
            i8::try_from(dto.counter_reset_hint).expect("reset hints are in 0..=3"),
        ),
        zero_threshold: dto.zero_threshold,
        zero_count: dto.zero_count,
        count: dto.count,
        sum: dto.sum,
        positive_spans: spans(dto.positive_spans),
        negative_spans: spans(dto.negative_spans),
        positive_counts: dto.positive_buckets,
        negative_counts: dto.negative_buckets,
        custom_values: (dto.schema == -53).then_some(dto.custom_values),
        start_timestamp_ms: None,
    }
}
fn call(receiver: &TemplateHistogram, name: &str, args: &[V]) -> Result<V, String> {
    if matches!(name, "CopyTo" | "Add" | "KahanAdd" | "Sub") {
        return Err(format!(
            "function {name} has {} return values; should be 1 or 2",
            if name == "CopyTo" { 0 } else { 4 }
        ));
    }
    let count = match name {
        "Mul" | "Div" | "CopyToSchema" | "ReduceResolution" | "Compact" | "Equals"
        | "DetectReset" => 1,
        "TrimBuckets" => 2,
        _ => 0,
    };
    if args.len() != count {
        return Err(format!("wrong number of args for {name}"));
    }
    let mut raw = receiver.snapshot();
    if name == "Copy" {
        return Ok(V::FloatHistogram(receiver.copy()));
    }
    if matches!(name, "CopyToSchema" | "ReduceResolution") {
        return change_schema(receiver, name, args, raw);
    }
    if matches!(name, "Mul" | "Div") {
        let V::Float(factor) = args[0] else {
            return Err("expected float64".into());
        };
        let scale = |value: &mut f64| {
            if name == "Mul" {
                *value *= factor;
            } else {
                *value /= factor;
            }
        };
        scale(&mut raw.count);
        scale(&mut raw.sum);
        scale(&mut raw.zero_count);
        if name == "Div" && factor == 0.0 {
            raw.positive_spans.clear();
            raw.negative_spans.clear();
            raw.positive_buckets.clear();
            raw.negative_buckets.clear();
            raw.nil_slices |= 15;
        } else {
            for count in raw
                .positive_buckets
                .iter_mut()
                .chain(&mut raw.negative_buckets)
            {
                scale(count);
            }
            if factor < 0.0 {
                raw.counter_reset_hint = 3;
            }
        }
        refresh_display(&mut raw);
        receiver.replace(raw);
        return Ok(V::FloatHistogram(receiver.clone()));
    }
    let mut hist = raw;
    let boolean = |value| V::Json(serde_json::Value::Bool(value));
    match name {
        "ZeroBucket" => {
            if hist.schema == -53 {
                return Err("histograms with custom buckets have no zero bucket".into());
            }
            Ok(V::HistogramBucket(TemplateBucket {
                lower: -hist.zero_threshold,
                upper: hist.zero_threshold,
                lower_inclusive: true,
                upper_inclusive: true,
                count: hist.zero_count,
                index: 0,
            }))
        }
        "HasOverflow" => Ok(boolean(
            [hist.zero_count, hist.count, hist.sum]
                .into_iter()
                .chain(hist.positive_buckets.iter().copied())
                .chain(hist.negative_buckets.iter().copied())
                .chain(hist.custom_values.iter().copied())
                .any(f64::is_infinite),
        )),
        "Size" => Ok(V::Integer(
            i64::try_from(
                168 + 8
                    * (hist.positive_spans.len()
                        + hist.negative_spans.len()
                        + hist.positive_buckets.len()
                        + hist.negative_buckets.len()
                        + hist.custom_values.len()),
            )
            .expect("histogram fits in memory"),
        )),
        "Equals" => {
            if matches!(&args[0], V::Json(serde_json::Value::Null)) {
                return Ok(boolean(false));
            }
            let V::FloatHistogram(other) = &args[0] else {
                return Err("expected *histogram.FloatHistogram".into());
            };
            Ok(boolean(equals(&hist, &other.snapshot())))
        }
        "DetectReset" => {
            let V::FloatHistogram(other) = &args[0] else {
                return Err("expected *histogram.FloatHistogram".into());
            };
            Ok(boolean(detect_reset(&other.snapshot(), &hist)))
        }
        "TrimBuckets" => {
            let (V::Float(bound), V::Json(serde_json::Value::Bool(upper))) = (&args[0], &args[1])
            else {
                return Err("expected float64 and bool".into());
            };
            receiver.replace(trim_template_histogram(&hist, *bound, *upper));
            Ok(V::FloatHistogram(receiver.clone()))
        }
        "Compact" => {
            let V::Integer(max) = args[0] else {
                return Err("expected int".into());
            };
            compact(&mut hist, max);
            refresh_display(&mut hist);
            receiver.replace(hist);
            Ok(V::FloatHistogram(receiver.clone()))
        }
        "TestExpression" => {
            compact(&mut hist, i64::MAX);
            Ok(V::String(test_expression(&hist)))
        }
        "Validate" => Ok(validate(&hist).map_or(V::NilError, |error| {
            V::HistogramError(validation_error(error))
        })),
        "PositiveBucketIterator"
        | "NegativeBucketIterator"
        | "PositiveReverseBucketIterator"
        | "NegativeReverseBucketIterator"
        | "AllBucketIterator"
        | "AllReverseBucketIterator" => Ok(V::HistogramIterator(source_iterator(receiver, name))),
        _ => Err(format!("cannot evaluate field or method {name}")),
    }
}
fn change_schema(
    receiver: &TemplateHistogram,
    name: &str,
    args: &[V],
    mut raw: Dto,
) -> Result<V, String> {
    let V::Integer32(target) = args[0] else {
        return Err("expected int32".into());
    };
    if name == "CopyToSchema" && target == raw.schema {
        return Ok(V::FloatHistogram(receiver.copy()));
    }
    let error = if raw.schema == -53 {
        Some(if name == "ReduceResolution" {
            "cannot reduce resolution when there are custom buckets".to_owned()
        } else {
            format!("cannot reduce resolution to {target} when there are custom buckets")
        })
    } else if target == -53 {
        Some("cannot reduce resolution to custom buckets schema".into())
    } else if target > raw.schema || (name == "ReduceResolution" && target == raw.schema) {
        Some(if name == "ReduceResolution" {
            format!(
                "cannot reduce resolution from schema {} to {target}",
                raw.schema
            )
        } else {
            format!("cannot copy from schema {} to {target}", raw.schema)
        })
    } else {
        None
    };
    if let Some(error) = error {
        return if name == "ReduceResolution" {
            Ok(V::HistogramError(TemplateHistogramError::plain(error)))
        } else {
            Err(error)
        };
    }
    let original_schema = raw.schema;
    if target != original_schema {
        for positive in [true, false] {
            let (spans, counts) = if positive {
                (&mut raw.positive_spans, &mut raw.positive_buckets)
            } else {
                (&mut raw.negative_spans, &mut raw.negative_buckets)
            };
            let native_spans = spans
                .iter()
                .map(|span| krabka_metrics::BucketSpan {
                    offset: span.offset,
                    length: span.length,
                })
                .collect::<Vec<_>>();
            let shift =
                u32::try_from(i64::from(original_schema) - i64::from(target)).unwrap_or(u32::MAX);
            let mut reduced = BTreeMap::new();
            for (index, count) in spanned_histogram_counts(&native_spans, counts) {
                let base = index.wrapping_sub(1);
                let target_index = if shift >= 32 {
                    i32::from(base >= 0)
                } else {
                    (base >> shift).wrapping_add(1)
                };
                *reduced.entry(target_index).or_insert(0.0) += count;
            }
            let (new_spans, new_counts) = spans_from_buckets(reduced);
            *spans = new_spans
                .into_iter()
                .map(|span| TemplateHistogramSpan {
                    offset: span.offset,
                    length: span.length,
                })
                .collect();
            *counts = new_counts;
        }
        raw.schema = target;
        refresh_display(&mut raw);
    }
    if name == "CopyToSchema" {
        if original_schema != target {
            raw.counter_reset_hint = 0;
        }
        raw.nil_slices = 16
            | u8::from(raw.positive_spans.is_empty())
            | (u8::from(raw.negative_spans.is_empty()) << 1)
            | (u8::from(raw.positive_buckets.is_empty()) << 2)
            | (u8::from(raw.negative_buckets.is_empty()) << 3);
        return Ok(V::FloatHistogram(TemplateHistogram::new(raw, call)));
    }
    receiver.replace(raw);
    Ok(V::NilError)
}

// Pinned floatBucketIterator fast path and reverseFloatBucketIterator keep a
// copied slice header but read element backing live. Field templates instead
// hold reflected headers, so Compact changes their length without resizing an
// iterator or an explicit slice view created before the mutation.
struct SideCursor {
    schema: i32,
    positive: bool,
    reverse: bool,
    spans: krabka_logql::TemplateQueryResult,
    buckets: krabka_logql::TemplateQueryResult,
    custom: krabka_logql::TemplateQueryResult,
    nil_slices: u8,
    span_index: i64,
    bucket_index: i64,
    in_span: i64,
    index: i32,
    count: f64,
}
impl SideCursor {
    fn new(receiver: &TemplateHistogram, positive: bool, reverse: bool) -> Self {
        let capture = |field| match receiver.field(field).expect("typed histogram field") {
            V::FloatSlice(values) | V::HistogramSpans(values) => values.capture(),
            _ => unreachable!("histogram field is a slice"),
        };
        let spans = capture(if positive {
            "PositiveSpans"
        } else {
            "NegativeSpans"
        });
        let buckets = capture(if positive {
            "PositiveBuckets"
        } else {
            "NegativeBuckets"
        });
        let mut cursor = Self {
            schema: receiver.snapshot().schema,
            nil_slices: receiver.snapshot().nil_slices,
            positive,
            reverse,
            custom: capture("CustomValues"),
            spans,
            buckets,
            span_index: 0,
            bucket_index: 0,
            in_span: 0,
            index: 0,
            count: 0.0,
        };
        if reverse {
            cursor.span_index = i64::try_from(cursor.spans.len()).expect("span length") - 1;
            cursor.bucket_index = i64::try_from(cursor.buckets.len()).expect("bucket length") - 1;
            if let Some(span) = cursor.span() {
                cursor.in_span = i64::from(span.length) - 1;
            }
            for span in cursor.spans.snapshot() {
                if let V::HistogramSpan(span) = span {
                    cursor.index = cursor
                        .index
                        .wrapping_add(span.offset)
                        .wrapping_add(i32::from_ne_bytes(span.length.to_ne_bytes()));
                }
            }
        }
        cursor
    }
    fn reflection(&self) -> View {
        let span_bit = if self.positive { 1 } else { 2 };
        let bucket_bit = if self.positive { 4 } else { 8 };
        let base = View::structure(
            "histogram.baseBucketIterator[float64,float64]",
            vec![
                ("schema", View::scalar(V::Integer32(self.schema))),
                (
                    "spans",
                    View::slice_at(
                        "[]histogram.Span",
                        self.spans.snapshot(),
                        self.nil_slices & span_bit != 0,
                        self.spans.backing_address(),
                    ),
                ),
                (
                    "buckets",
                    View::slice_at(
                        "[]float64",
                        self.buckets.snapshot(),
                        self.nil_slices & bucket_bit != 0,
                        self.buckets.backing_address(),
                    ),
                ),
                ("positive", View::scalar(V::Json(self.positive.into()))),
                ("spansIdx", View::scalar(V::Integer(self.span_index))),
                (
                    "idxInSpan",
                    View::scalar(V::Unsigned32(if self.reverse {
                        0
                    } else {
                        u32::try_from(self.in_span).expect("source uint32 cursor")
                    })),
                ),
                ("bucketsIdx", View::scalar(V::Integer(self.bucket_index))),
                ("currCount", View::scalar(V::Float(self.count))),
                ("currIdx", View::scalar(V::Integer32(self.index))),
                (
                    "customValues",
                    View::slice_at(
                        "[]float64",
                        self.custom.snapshot(),
                        self.nil_slices & 16 != 0,
                        self.custom.backing_address(),
                    ),
                ),
            ],
        );
        if self.reverse {
            View::structure(
                "histogram.reverseFloatBucketIterator",
                vec![
                    ("baseBucketIterator", base),
                    (
                        "idxInSpan",
                        View::scalar(V::Integer32(
                            i32::try_from(self.in_span).expect("source int32 cursor"),
                        )),
                    ),
                ],
            )
        } else {
            View::structure(
                "histogram.floatBucketIterator",
                vec![
                    ("baseBucketIterator", base),
                    ("targetSchema", View::scalar(V::Integer32(self.schema))),
                    ("origIdx", View::scalar(V::Integer32(0))),
                    ("absoluteStartValue", View::scalar(V::Float(0.0))),
                    ("boundReachedStartValue", View::scalar(V::Json(true.into()))),
                ],
            )
        }
    }
    fn span(&self) -> Option<TemplateHistogramSpan> {
        match self.spans.get(usize::try_from(self.span_index).ok()?) {
            Some(V::HistogramSpan(span)) => Some(span),
            _ => None,
        }
    }
    fn next(&mut self) -> bool {
        if self.reverse {
            self.index = self.index.wrapping_sub(1);
            if self.bucket_index < 0 {
                return false;
            }
            while self.in_span < 0 {
                self.span_index -= 1;
                let Some(span) = self.span() else {
                    return false;
                };
                self.in_span = i64::from(span.length) - 1;
                if let Some(V::HistogramSpan(next)) = self
                    .spans
                    .get(usize::try_from(self.span_index + 1).expect("next span"))
                {
                    self.index = self.index.wrapping_sub(next.offset);
                }
            }
            let Some(V::Float(count)) = self
                .buckets
                .get(usize::try_from(self.bucket_index).expect("positive bucket"))
            else {
                return false;
            };
            self.count = count;
            self.bucket_index -= 1;
            self.in_span -= 1;
            true
        } else {
            let Some(mut span) = self.span() else {
                return false;
            };
            self.index = if self.bucket_index == 0 {
                span.offset
            } else {
                self.index.wrapping_add(1)
            };
            if usize::try_from(self.bucket_index).expect("positive bucket") >= self.buckets.len() {
                return false;
            }
            while self.in_span >= i64::from(span.length) {
                self.in_span = 0;
                self.span_index += 1;
                let Some(next) = self.span() else {
                    return false;
                };
                span = next;
                self.index = self.index.wrapping_add(span.offset);
            }
            let Some(V::Float(count)) = self
                .buckets
                .get(usize::try_from(self.bucket_index).expect("positive bucket"))
            else {
                return false;
            };
            self.count = count;
            self.in_span += 1;
            self.bucket_index += 1;
            true
        }
    }
    fn at(&self) -> TemplateBucket {
        let bound = |index: i32| {
            if self.schema == -53 {
                if index < 0 {
                    f64::NEG_INFINITY
                } else {
                    match self
                        .custom
                        .get(usize::try_from(index).expect("nonnegative bound"))
                    {
                        Some(V::Float(value)) => value,
                        _ => f64::INFINITY,
                    }
                }
            } else {
                template_bound(index, self.schema)
            }
        };
        let (lower, upper) = if self.positive {
            (bound(self.index.wrapping_sub(1)), bound(self.index))
        } else {
            (-bound(self.index), -bound(self.index.wrapping_sub(1)))
        };
        TemplateBucket {
            lower,
            upper,
            lower_inclusive: if self.schema == -53 {
                self.index == 0
            } else {
                lower < 0.0
            },
            upper_inclusive: if self.schema == -53 {
                true
            } else {
                upper > 0.0
            },
            count: self.count,
            index: self.index,
        }
    }
}
fn source_iterator(receiver: &TemplateHistogram, name: &str) -> TemplateBucketIterator {
    use std::sync::{Arc, Mutex};
    let all = name.starts_with("All");
    let reverse = name.contains("Reverse");
    if !all {
        let cursor = Arc::new(Mutex::new(SideCursor::new(
            receiver,
            name.starts_with("Positive"),
            reverse,
        )));
        let initial = cursor.lock().expect("source cursor").at();
        let reflection_cursor = Arc::clone(&cursor);
        return TemplateBucketIterator::new(
            initial,
            if reverse {
                "*histogram.reverseFloatBucketIterator"
            } else {
                "*histogram.floatBucketIterator"
            },
            move || {
                let mut cursor = cursor.lock().expect("source cursor");
                let advanced = cursor.next();
                (advanced, cursor.at())
            },
            move || {
                reflection_cursor
                    .lock()
                    .expect("source cursor")
                    .reflection()
            },
        );
    }
    let receiver = receiver.clone();
    let cursor = Arc::new(Mutex::new((
        SideCursor::new(&receiver, reverse, true),
        SideCursor::new(&receiver, !reverse, false),
        -1_i8,
        TemplateBucket::default(),
    )));
    let reflection_cursor = Arc::clone(&cursor);
    let reflection_receiver = receiver.clone();
    TemplateBucketIterator::new(
        TemplateBucket::default(),
        "*histogram.allFloatBucketIterator",
        move || {
            let mut guard = cursor.lock().expect("source cursor");
            let (left, right, state, current) = &mut *guard;
            loop {
                let advanced = match *state {
                    -1 => {
                        if left.next() {
                            *current = left.at();
                            true
                        } else {
                            *state = 0;
                            continue;
                        }
                    }
                    0 => {
                        *state = 1;
                        let hist = receiver.snapshot();
                        if hist.zero_count > 0.0 {
                            *current = TemplateBucket {
                                lower: -hist.zero_threshold,
                                upper: hist.zero_threshold,
                                lower_inclusive: true,
                                upper_inclusive: true,
                                count: hist.zero_count,
                                index: 0,
                            };
                            return (true, current.clone());
                        }
                        continue;
                    }
                    1 => {
                        if right.next() {
                            *current = right.at();
                            true
                        } else {
                            *state = 42;
                            false
                        }
                    }
                    _ => false,
                };
                if advanced {
                    let threshold = receiver.snapshot().zero_threshold;
                    if current.upper < 0.0 && current.upper > -threshold {
                        current.upper = -threshold;
                    } else if current.lower > 0.0 && current.lower < threshold {
                        current.lower = threshold;
                    }
                }
                return (advanced, current.clone());
            }
        },
        move || {
            let guard = reflection_cursor.lock().expect("source cursor");
            let (left, right, state, current) = &*guard;
            View::structure(
                "histogram.allFloatBucketIterator",
                vec![
                    ("h", reflection_receiver.reflection()),
                    ("leftIter", left.reflection()),
                    ("rightIter", right.reflection()),
                    (
                        "state",
                        View::NamedScalar {
                            name: "int8",
                            value: V::Integer(i64::from(*state)),
                        },
                    ),
                    ("currBucket", current.reflection()),
                ],
            )
        },
    )
}

fn compact(hist: &mut Dto, max_empty: i64) {
    fn side(spans: &mut Vec<TemplateHistogramSpan>, counts: &mut Vec<f64>, max_empty: i64) {
        let nonzero = span_counts(spans, counts)
            .into_iter()
            .filter(|(_, count)| *count != 0.0)
            .collect::<Vec<_>>();
        let mut buckets = BTreeMap::new();
        let mut previous = None;
        for (index, count) in nonzero {
            if let Some(previous) = previous
                && i64::from(index) - i64::from(previous) - 1 <= max_empty
            {
                for gap in previous + 1..index {
                    buckets.insert(gap, 0.0);
                }
            }
            buckets.insert(index, count);
            previous = Some(index);
        }
        // Keep the explicit zero gaps required by maxEmptyBuckets.
        let (new_spans, new_counts) = spans_from_buckets(buckets);
        *spans = new_spans
            .into_iter()
            .map(|span| TemplateHistogramSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect();
        *counts = new_counts;
    }
    side(
        &mut hist.positive_spans,
        &mut hist.positive_buckets,
        max_empty,
    );
    side(
        &mut hist.negative_spans,
        &mut hist.negative_buckets,
        max_empty,
    );
}
fn equals(left: &Dto, right: &Dto) -> bool {
    let bits_equal = |left: &[f64], right: &[f64]| {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(a, b)| a.to_bits() == b.to_bits())
    };
    left.schema == right.schema
        && left.count.to_bits() == right.count.to_bits()
        && left.sum.to_bits() == right.sum.to_bits()
        && left.zero_threshold.partial_cmp(&right.zero_threshold) == Some(std::cmp::Ordering::Equal)
        && left.zero_count.to_bits() == right.zero_count.to_bits()
        && normalized_spans(&left.positive_spans) == normalized_spans(&right.positive_spans)
        && normalized_spans(&left.negative_spans) == normalized_spans(&right.negative_spans)
        && bits_equal(&left.positive_buckets, &right.positive_buckets)
        && bits_equal(&left.negative_buckets, &right.negative_buckets)
        && (left.schema != -53
            || (left.custom_values.len() == right.custom_values.len()
                && left
                    .custom_values
                    .iter()
                    .zip(&right.custom_values)
                    .all(|(a, b)| a.partial_cmp(b) == Some(std::cmp::Ordering::Equal))))
}
fn side_buckets(hist: &Dto, positive: bool) -> Vec<TemplateBucket> {
    let (spans, counts) = if positive {
        (&hist.positive_spans, &hist.positive_buckets)
    } else {
        (&hist.negative_spans, &hist.negative_buckets)
    };
    span_counts(spans, counts)
        .into_iter()
        .map(|(index, count)| {
            let (lower, upper, lower_inclusive, upper_inclusive) = if hist.schema == -53 {
                let bounds = hist.custom_values.as_slice();
                let bound = |index: i32| {
                    usize::try_from(index)
                        .ok()
                        .and_then(|index| bounds.get(index))
                        .copied()
                        .unwrap_or(if index < 0 {
                            f64::NEG_INFINITY
                        } else {
                            f64::INFINITY
                        })
                };
                (bound(index - 1), bound(index), index == 0, true)
            } else if positive {
                (
                    template_bound(index - 1, hist.schema),
                    template_bound(index, hist.schema),
                    false,
                    true,
                )
            } else {
                (
                    -template_bound(index, hist.schema),
                    -template_bound(index - 1, hist.schema),
                    true,
                    false,
                )
            };
            TemplateBucket {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
                count,
                index,
            }
        })
        .collect()
}
fn validate(hist: &Dto) -> Option<String> {
    fn spans(spans: &[TemplateHistogramSpan], counts: usize, custom: bool) -> Option<String> {
        for (index, span) in spans.iter().enumerate() {
            if span.offset < 0 && (custom || index > 0) {
                return Some(format!(
                    "span number {} with offset {}: histogram has a span whose offset is negative",
                    index + 1,
                    span.offset
                ));
            }
        }
        let total = spans.iter().map(|span| u64::from(span.length)).sum::<u64>();
        (total != counts as u64).then(|| format!("spans need {total} buckets, have {counts} buckets: histogram spans specify different number of buckets than provided"))
    }
    fn counts(counts: &[f64]) -> Option<String> {
        counts.iter().enumerate().find(|(_, count)| **count < 0.0).map(|(index,count)| format!("bucket number {} has observation count of {count}: histogram has a bucket whose observation count is negative", index + 1))
    }
    if hist.schema == -53 {
        let bounds = hist.custom_values.as_slice();
        let mut previous = f64::NEG_INFINITY;
        for (index, value) in bounds.iter().copied().enumerate() {
            if value.is_nan() {
                return Some("custom buckets: histogram custom bounds must not be NaN".into());
            }
            if index > 0 && value <= previous {
                return Some(format!(
                    "custom buckets: previous bound is {previous:.6} and current is {value:.6}: histogram custom bounds must be in strictly increasing order"
                ));
            }
            previous = value;
        }
        if previous == f64::INFINITY {
            return Some("custom buckets: last +Inf bound must not be explicitly defined: histogram custom bounds must be finite".into());
        }
        if let Some(error) = spans(&hist.positive_spans, hist.positive_buckets.len(), true) {
            return Some(format!("custom buckets: {error}"));
        }
        let length = hist
            .positive_spans
            .iter()
            .map(|span| i64::from(span.offset) + i64::from(span.length))
            .sum::<i64>();
        if i64::try_from(bounds.len() + 1).expect("bounds fit in memory") < length {
            return Some(format!(
                "custom buckets: only {} custom bounds defined which is insufficient to cover total span length of {length}: histogram custom bounds are too few",
                bounds.len()
            ));
        }
        if hist.zero_count != 0.0 {
            return Some("custom buckets: must have zero count of 0".into());
        }
        if hist.zero_threshold != 0.0 {
            return Some("custom buckets: must have zero threshold of 0".into());
        }
        if !hist.negative_spans.is_empty() {
            return Some("custom buckets: must not have negative spans".into());
        }
        if !hist.negative_buckets.is_empty() {
            return Some("custom buckets: must not have negative buckets".into());
        }
    } else if (-4..=8).contains(&hist.schema) {
        if let Some(error) = spans(&hist.positive_spans, hist.positive_buckets.len(), false) {
            return Some(format!("positive side: {error}"));
        }
        if let Some(error) = spans(&hist.negative_spans, hist.negative_buckets.len(), false)
            .or_else(|| counts(&hist.negative_buckets))
        {
            return Some(format!("negative side: {error}"));
        }
        if hist.zero_count < 0.0 {
            return Some(format!(
                "zero bucket has observation count of {}: histogram has a bucket whose observation count is negative",
                hist.zero_count
            ));
        }
        if hist.nil_slices & 16 == 0 {
            return Some("histogram with exponential schema must not have custom bounds".into());
        }
    } else {
        return Some(format!(
            "histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets, got schema {}",
            hist.schema
        ));
    }
    if hist.count < 0.0 {
        return Some(format!(
            "observation count is  {}: histogram's observation count is negative",
            hist.count
        ));
    }
    counts(&hist.positive_buckets).map(|error| format!("positive side: {error}"))
}
fn test_expression(hist: &Dto) -> String {
    let float = Dto::format_float;
    let mut parts = Vec::new();
    if hist.schema != 0 {
        parts.push(format!("schema:{}", hist.schema));
    }
    for (name, value) in [
        ("count", hist.count),
        ("sum", hist.sum),
        ("z_bucket", hist.zero_count),
        ("z_bucket_w", hist.zero_threshold),
    ] {
        if value != 0.0 {
            parts.push(format!("{name}:{}", float(value)));
        }
    }
    if hist.schema == -53 {
        let bounds = &hist.custom_values;
        parts.push(format!(
            "custom_values:[{}]",
            bounds
                .iter()
                .map(|value| float(*value))
                .collect::<Vec<_>>()
                .join(" ")
        ));
    }
    match ResetHint::from_i8(i8::try_from(hist.counter_reset_hint).expect("reset hint")) {
        ResetHint::Unknown => {}
        ResetHint::Yes => parts.push("counter_reset_hint:reset".into()),
        ResetHint::No => parts.push("counter_reset_hint:not_reset".into()),
        ResetHint::Gauge => parts.push("counter_reset_hint:gauge".into()),
    }
    for (bucket_key, offset_key, spans, counts) in [
        (
            "buckets",
            "offset",
            &hist.positive_spans,
            &hist.positive_buckets,
        ),
        (
            "n_buckets",
            "n_offset",
            &hist.negative_spans,
            &hist.negative_buckets,
        ),
    ] {
        if let Some(span) = spans.first()
            && span.offset != 0
        {
            parts.push(format!("{offset_key}:{}", span.offset));
        }
        if !counts.is_empty() {
            parts.push(format!(
                "{bucket_key}:[{}]",
                counts
                    .iter()
                    .map(|value| float(*value))
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
        }
    }
    format!("{{{{{}}}}}", parts.join(" "))
}

fn normalized_spans(spans: &[TemplateHistogramSpan]) -> Vec<(i32, u32)> {
    let mut offset = 0;
    let mut out = Vec::new();
    for span in spans {
        offset += span.offset;
        if span.length > 0 {
            out.push((offset, span.length));
            offset = 0;
        }
    }
    out
}

fn validation_error(message: String) -> std::sync::Arc<TemplateHistogramError> {
    static CONSTANTS: std::sync::OnceLock<
        std::sync::Mutex<BTreeMap<String, std::sync::Arc<TemplateHistogramError>>>,
    > = std::sync::OnceLock::new();
    let node = |message: String, kind, unwrap| {
        std::sync::Arc::new(TemplateHistogramError {
            message,
            kind,
            unwrap,
        })
    };
    if message.starts_with("histogram has an invalid schema") {
        let base = message
            .split(", got schema")
            .next()
            .expect("schema error")
            .to_owned();
        let base = node(
            base.clone(),
            "histogram.Error",
            Some(TemplateHistogramError::plain(base)),
        );
        let wrapped = node(message.clone(), "*fmt.wrapError", Some(base));
        return node(message, "histogram.Error", Some(wrapped));
    }
    if message.starts_with("custom buckets: must ") {
        return node(
            message.clone(),
            "histogram.Error",
            Some(TemplateHistogramError::plain(message)),
        );
    }
    if let Some((_prefix, rest)) = message.split_once(": ") {
        return node(
            message.clone(),
            "*fmt.wrapError",
            Some(validation_error(rest.to_owned())),
        );
    }
    let mut constants = CONSTANTS
        .get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
        .lock()
        .expect("histogram errors poisoned");
    std::sync::Arc::clone(constants.entry(message.clone()).or_insert_with(|| {
        node(
            message.clone(),
            "histogram.Error",
            Some(TemplateHistogramError::plain(message)),
        )
    }))
}

fn span_counts(spans: &[TemplateHistogramSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let spans = spans
        .iter()
        .map(|span| krabka_metrics::BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect::<Vec<_>>();
    spanned_histogram_counts(&spans, counts)
}
fn has_native_schema(hist: &Dto) -> bool {
    hist.schema == -53 || (-4..=8).contains(&hist.schema)
}
fn detect_reset(previous: &Dto, current: &Dto) -> bool {
    if has_native_schema(previous) && has_native_schema(current) {
        return native_histogram_detect_reset(&native(previous.clone()), &native(current.clone()));
    }
    match current.counter_reset_hint {
        1 => return true,
        2 => return false,
        _ => {}
    }
    if current.count < previous.count
        || current.schema > previous.schema
        || current.zero_threshold < previous.zero_threshold
        || (current.schema == -53 && previous.schema != -53)
    {
        return true;
    }
    let mut previous_zero = previous.zero_count;
    if current.zero_threshold > previous.zero_threshold {
        for positive in [true, false] {
            for bucket in side_buckets(previous, positive) {
                let (lower, upper) = if positive {
                    (bucket.lower, bucket.upper)
                } else {
                    (-bucket.upper, -bucket.lower)
                };
                if upper <= current.zero_threshold {
                    previous_zero += bucket.count;
                } else if lower < current.zero_threshold && bucket.count != 0.0 {
                    return true;
                }
            }
        }
    }
    if current.zero_count < previous_zero {
        return true;
    }
    let counts = |hist: &Dto, positive: bool| {
        let (spans, counts) = if positive {
            (&hist.positive_spans, &hist.positive_buckets)
        } else {
            (&hist.negative_spans, &hist.negative_buckets)
        };
        let shift =
            u32::try_from(i64::from(hist.schema) - i64::from(current.schema)).unwrap_or_default();
        let mut out = BTreeMap::new();
        for (index, count) in span_counts(spans, counts) {
            let index = if shift >= 32 {
                i32::from(index.wrapping_sub(1) >= 0)
            } else {
                (index.wrapping_sub(1) >> shift).wrapping_add(1)
            };
            if current.zero_threshold <= 0.0
                || current.schema == -53
                || template_bound(index, current.schema) > current.zero_threshold
            {
                *out.entry(index).or_insert(0.0) += count;
            }
        }
        out
    };
    [true, false].into_iter().any(|positive| {
        let current_counts = counts(current, positive);
        counts(previous, positive).iter().any(|(index, count)| {
            current_counts
                .get(index)
                .map_or(*count != 0.0, |current| current < count)
        })
    })
}
fn trim_template_histogram(hist: &Dto, bound: f64, upper: bool) -> Dto {
    if has_native_schema(hist) {
        let mut out = dto(trim_native_histogram(&native(hist.clone()), bound, upper));
        out.nil_slices = hist.nil_slices;
        return out;
    }
    let mut out = hist.clone();
    let mut total = 0.0;
    let mut sum = 0.0;
    let mut changed = false;
    for positive in [true, false] {
        let original = side_buckets(hist, positive);
        let counts = if positive {
            &mut out.positive_buckets
        } else {
            &mut out.negative_buckets
        };
        for (bucket, observations) in original.into_iter().zip(counts) {
            if *observations == 0.0 {
                continue;
            }
            let (keep, midpoint) =
                if (upper && bucket.upper <= bound) || (!upper && bucket.lower >= bound) {
                    (
                        *observations,
                        super::trim_native_histogram::midpoint(
                            bucket.lower,
                            bucket.upper,
                            positive,
                            false,
                        ),
                    )
                } else if (upper && bucket.lower < bound) || (!upper && bucket.upper > bound) {
                    super::trim_native_histogram::partial_bucket(
                        bucket.lower,
                        bucket.upper,
                        *observations,
                        bound,
                        upper,
                        positive,
                        false,
                    )
                } else {
                    (0.0, 0.0)
                };
            changed |= keep.partial_cmp(observations) != Some(std::cmp::Ordering::Equal);
            *observations = keep;
            total += keep;
            sum += midpoint * keep;
        }
    }
    if hist.zero_count > 0.0 {
        let positive = hist.positive_buckets.iter().any(|count| *count != 0.0);
        let negative = hist.negative_buckets.iter().any(|count| *count != 0.0);
        let lower = if positive && !negative {
            0.0
        } else {
            -hist.zero_threshold
        };
        let higher = if negative && !positive {
            0.0
        } else {
            hist.zero_threshold
        };
        let (keep, midpoint) = if (upper && bound <= lower) || (!upper && bound >= higher) {
            (0.0, 0.0)
        } else if (upper && bound >= higher) || (!upper && bound <= lower) {
            (hist.zero_count, source_linear_midpoint(lower, higher))
        } else if upper {
            (
                hist.zero_count * (bound - lower) / (higher - lower),
                source_linear_midpoint(lower, bound),
            )
        } else {
            (
                hist.zero_count * (higher - bound) / (higher - lower),
                source_linear_midpoint(bound, higher),
            )
        };
        changed |= keep.partial_cmp(&hist.zero_count) != Some(std::cmp::Ordering::Equal);
        out.zero_count = keep;
        total += keep;
        sum += midpoint * keep;
    }
    if !changed {
        return hist.clone();
    }
    out.count = total;
    out.sum = sum;
    compact(&mut out, 0);
    refresh_display(&mut out);
    out
}

// Preserve Go's sum-then-divide rounding and overflow, rather than using
// the overflow-avoiding f64::midpoint.
fn source_linear_midpoint(lower: f64, upper: f64) -> f64 {
    let sum = lower + upper;
    sum / 2.0
}

fn spans_from_buckets(buckets: BTreeMap<i32, f64>) -> (Vec<krabka_metrics::BucketSpan>, Vec<f64>) {
    let mut spans = Vec::new();
    let mut counts = Vec::new();
    let mut start = None;
    let mut end = 0;
    let mut previous = 0;
    for (index, count) in buckets {
        if start.is_some() && index != previous + 1 {
            let begin = start.take().expect("span started");
            spans.push(krabka_metrics::BucketSpan {
                offset: begin - end,
                length: u32::try_from(previous - begin + 1).expect("span length"),
            });
            end = previous + 1;
        }
        if start.is_none() {
            start = Some(index);
        }
        counts.push(count);
        previous = index;
    }
    if let Some(begin) = start {
        spans.push(krabka_metrics::BucketSpan {
            offset: begin - end,
            length: u32::try_from(previous - begin + 1).expect("span length"),
        });
    }
    (spans, counts)
}
fn template_bound(index: i32, schema: i32) -> f64 {
    super::standard_histogram_bound::bound(index, schema)
}
fn refresh_display(hist: &mut Dto) {
    let mut buckets = Vec::new();
    for positive in [false, true] {
        let (spans, counts) = if positive {
            (&hist.positive_spans, &hist.positive_buckets)
        } else {
            (&hist.negative_spans, &hist.negative_buckets)
        };
        let spans = spans
            .iter()
            .map(|span| krabka_metrics::BucketSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect::<Vec<_>>();
        for (index, count) in spanned_histogram_counts(&spans, counts) {
            if count == 0.0 {
                continue;
            }
            let (lower, upper, boundary_rule) = if hist.schema == -53 {
                let bound = |index: i32| {
                    usize::try_from(index)
                        .ok()
                        .and_then(|index| hist.custom_values.get(index))
                        .copied()
                        .unwrap_or(if index < 0 {
                            f64::NEG_INFINITY
                        } else {
                            f64::INFINITY
                        })
                };
                (
                    bound(index - 1),
                    bound(index),
                    if index == 0 { 3 } else { 0 },
                )
            } else if positive {
                (
                    template_bound(index - 1, hist.schema),
                    template_bound(index, hist.schema),
                    0,
                )
            } else {
                (
                    -template_bound(index, hist.schema),
                    -template_bound(index - 1, hist.schema),
                    1,
                )
            };
            buckets.push(TemplateHistogramBucket {
                lower,
                upper,
                count,
                boundary_rule,
            });
        }
    }
    if hist.schema != -53 && hist.zero_count != 0.0 {
        buckets.push(TemplateHistogramBucket {
            lower: -hist.zero_threshold,
            upper: hist.zero_threshold,
            count: hist.zero_count,
            boundary_rule: 3,
        });
    }
    buckets.sort_by(|a, b| a.lower.total_cmp(&b.lower));
    hist.display_buckets = buckets;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use krabka_logql::LineFormat;

    use super::*;
    fn fixture() -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::Gauge,
            zero_threshold: 0.001,
            zero_count: 2.0,
            count: 8.0,
            sum: 10.0,
            positive_spans: vec![krabka_metrics::BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.0, 2.0],
            negative_spans: vec![krabka_metrics::BucketSpan {
                offset: 0,
                length: 2,
            }],
            negative_counts: vec![1.0, 2.0],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }
    // Independently executed by Go 1.26.5 text/template against Prometheus
    // v0.314.0 model/histogram.FloatHistogram, not calculated by this adapter.
    #[test]
    fn exported_histogram_methods_match_source_composition_goldens() {
        let cases = [
            (
                "{{$n:=$h.Count}}{{$b:=index $h.PositiveBuckets 1}}{{$s:=index $h.PositiveSpans 0}}{{$x:=$h.Mul 2}}{{$n}}/{{$b}}/{{$s.Length}}",
                "16/4/2",
            ),
            (
                "{{$h.ReduceResolution -1000}}/{{$h.Copy.Schema}}/{{$h.ZeroBucket.Count}}/{{$h.HasOverflow}}/{{$h.Size}}/{{$h.Equals $h.Copy}}/{{$h.DetectReset $h.Copy}}/{{$h.TestExpression}}",
                "<nil>/-1000/2/false/216/true/false/{{schema:-1000 count:8 sum:10 z_bucket:2 z_bucket_w:0.001 counter_reset_hint:gauge buckets:[1 2] n_buckets:[1 2]}}",
            ),
            (
                "{{$x:=$h.Mul -2}}{{printf \"%T\" $h.Validate}}/{{$h.Validate.Unwrap}}",
                "*fmt.wrapError/bucket number 1 has observation count of -2: histogram has a bucket whose observation count is negative",
            ),
            (
                "{{$h.ReduceResolution -1000}}/{{printf \"%T\" $h.Validate}}",
                "<nil>/histogram.Error",
            ),
            (
                "{{$i:=$h.PositiveBucketIterator}}{{eq $i $i}}/{{eq $h.ZeroBucket $h.ZeroBucket}}/{{printf \"%T\" $i}}",
                "true/true/*histogram.floatBucketIterator",
            ),
            (
                "{{$b:=$h.PositiveBuckets}}{{$i:=$h.PositiveBucketIterator}}{{$x:=$h.Mul 2}}{{index $b 0}}/{{$i.Next}}/{{$i.At.Count}}",
                "2/true/2",
            ),
            (
                "{{$h.ReduceResolution -1000}}/{{$h.Schema}}/{{$h.Validate}}",
                "<nil>/-1000/histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets, got schema -1000",
            ),
            (
                "{{$x:=$h.Mul -2}}{{$h.Validate}}",
                "negative side: bucket number 1 has observation count of -2: histogram has a bucket whose observation count is negative",
            ),
            ("{{$x:=$h.Compact 0}}{{$h.Count}}/{{eq $h $x}}", "8/true"),
            ("{{$h.Copy.Count}}/{{eq $h $h.Copy}}", "8/false"),
            (
                "{{$h.ZeroBucket.Count}}/{{$h.ZeroBucket.LowerInclusive}}/{{$h.ZeroBucket.Index}}",
                "2/true/0",
            ),
            (
                "{{$h.HasOverflow}}/{{$h.Size}}/{{$h.Validate}}/{{$h.Validate | printf \"%T\"}}",
                "false/216/<nil>/<nil>",
            ),
            (
                "{{$alias:=$h}}{{$copy:=$h.Copy}}{{$x:=$h.Mul 2}}{{$alias.Count}}/{{$copy.Count}}/{{eq $h $x}}",
                "16/8/true",
            ),
            (
                "{{$x:=$h.Div -2}}{{$h.Count}}/{{$h.CounterResetHint}}",
                "-4/3",
            ),
            (
                "{{$x:=$h.Div 0}}{{$h.Count}}/{{$h.ZeroCount}}/{{len $h.PositiveBuckets}}",
                "+Inf/+Inf/0",
            ),
            (
                "{{$c:=$h.CopyToSchema -1}}{{$c.Schema}}/{{$c.CounterResetHint}}/{{$c.PositiveBuckets}}/{{$h.Schema}}",
                "-1/0/[1 2]/0",
            ),
            (
                "{{$h.ReduceResolution -1}}/{{$h.Schema}}/{{$h.PositiveBuckets}}",
                "<nil>/-1/[1 2]",
            ),
            (
                "{{$h.ReduceResolution 1}}/{{$h.Schema}}",
                "cannot reduce resolution from schema 0 to 1/0",
            ),
            (
                "{{$h.TestExpression}}",
                "{{count:8 sum:10 z_bucket:2 z_bucket_w:0.001 counter_reset_hint:gauge buckets:[1 2] n_buckets:[1 2]}}",
            ),
            (
                "{{$h.Equals $h.Copy}}/{{$h.Equals nil}}/{{$h.DetectReset $h.Copy}}",
                "true/false/false",
            ),
            (
                "{{$i:=$h.PositiveBucketIterator}}{{$i.Next}}/{{$i.At.Lower}}/{{$i.At.Upper}}/{{$i.At.Index}}/{{$i.Next}}/{{$i.At.Count}}/{{$i.Next}}",
                "true/0.5/1/0/true/2/false",
            ),
            (
                "{{$i:=$h.NegativeBucketIterator}}{{$i.Next}}/{{$i.At.Lower}}/{{$i.At.Upper}}/{{$i.At.Index}}",
                "true/-1/-0.5/0",
            ),
            (
                "{{$i:=$h.AllBucketIterator}}{{$i.Next}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Count}}",
                "true/2/true/1/true/2",
            ),
            (
                "{{$i:=$h.AllReverseBucketIterator}}{{$i.Next}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Count}}",
                "true/2/true/1/true/2",
            ),
            (
                "{{$i:=$h.PositiveReverseBucketIterator}}{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}",
                "true/1/2",
            ),
            (
                "{{$i:=$h.NegativeReverseBucketIterator}}{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}",
                "true/1/2",
            ),
            (
                "{{$x:=$h.TrimBuckets 1.5 true}}{{$h.Count}}/{{$h.Sum}}/{{eq $h $x}}",
                "7.169925001442312/-1.3955674793169206/true",
            ),
        ];
        for (text, expected) in cases {
            let variables = BTreeMap::from([("h".to_owned(), template_histogram_value(fixture()))]);
            let actual = LineFormat::new_prometheus(text)
                .unwrap()
                .render_prometheus_bytes(&variables, &[], 0);
            assert2::assert!(
                actual == Ok(expected.as_bytes().to_vec()),
                "{text}: {actual:?}"
            );
        }
    }
    #[test]
    fn iterator_headers_and_reflected_fields_follow_source_layout_mutations() {
        let cases = [
            (
                "{{$b:=$h.PositiveBuckets}}{{$s:=$h.PositiveSpans}}{{$i:=$h.PositiveBucketIterator}}{{$x:=$h.Compact 0}}{{len $b}}/{{$b}}/{{$s}}/{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}",
                "2/[1 3]/[{0 1} {1 1}]/true/0/1/false/1/1/false",
            ),
            (
                "{{$b:=$h.PositiveBuckets}}{{$view:=slice $b 0 3}}{{$x:=$h.Compact 0}}{{len $b}}/{{len $view}}/{{$view}}",
                "2/3/[1 3 3]",
            ),
            (
                "{{$i:=$h.PositiveBucketIterator}}{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$x:=$h.Compact 0}}{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}",
                "true/0/1/false/1/1/false",
            ),
            (
                "{{$i:=$h.PositiveReverseBucketIterator}}{{$x:=$h.Compact 0}}{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}",
                "true/2/3/true/1/3/true",
            ),
            (
                "{{$b:=$h.PositiveBuckets}}{{$s:=$h.PositiveSpans}}{{$i:=$h.PositiveBucketIterator}}{{$h.ReduceResolution -1}}/{{$b}}/{{$s}}/{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}",
                "<nil>/[1 3]/[{0 2}]/true/0/1/true/1/3/false",
            ),
            (
                "{{$b:=$h.PositiveBuckets}}{{$i:=$h.PositiveBucketIterator}}{{$x:=$h.TrimBuckets 1.5 true}}{{$b}}/{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Index}}/{{$i.At.Count}}/{{$i.Next}}",
                "[1]/true/0/1/false/1/1/false",
            ),
            (
                "{{$b:=$h.PositiveBuckets}}{{$i:=$h.PositiveBucketIterator}}{{$x:=$h.Div 0}}{{len $b}}/{{$i.Next}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Count}}/{{$i.Next}}/{{$i.At.Count}}",
                "0/true/1/true/0/true/3",
            ),
        ];
        for (text, expected) in cases {
            let mut histogram = fixture();
            histogram.positive_spans[0].length = 3;
            histogram.positive_counts = vec![1.0, 0.0, 3.0];
            let variables = BTreeMap::from([("h".to_owned(), template_histogram_value(histogram))]);
            let actual = LineFormat::new_prometheus(text)
                .unwrap()
                .render_prometheus_bytes(&variables, &[], 0);
            assert2::assert!(
                actual == Ok(expected.as_bytes().to_vec()),
                "{text}: {actual:?}"
            );
        }
    }

    #[test]
    fn custom_bounds_compare_numerically_while_bucket_populations_compare_bits() {
        // Pinned Go FloatHistogram.Equals: signed-zero bounds agree, while
        // signed-zero population bits do not; reset hints do not affect Equals.
        let mut histogram = fixture();
        histogram.schema = -53;
        histogram.zero_count = 0.0;
        histogram.zero_threshold = 0.0;
        histogram.negative_spans.clear();
        histogram.negative_counts.clear();
        histogram.custom_values = Some(vec![0.0, 2.0]);
        histogram.positive_spans[0].length = 3;
        histogram.positive_counts = vec![1.0, 0.0, 7.0];
        let mut other = histogram.clone();
        other.custom_values.as_mut().unwrap()[0] = -0.0;
        other.reset_hint = ResetHint::Unknown;
        let mut changed = histogram.clone();
        changed.positive_counts[1] = -0.0;
        let variables = BTreeMap::from([
            ("h".to_owned(), template_histogram_value(histogram)),
            ("other".to_owned(), template_histogram_value(other)),
            ("changed".to_owned(), template_histogram_value(changed)),
        ]);
        let actual = LineFormat::new_prometheus("{{$h.Equals $other}}/{{$h.Equals $changed}}")
            .unwrap()
            .render_prometheus_bytes(&variables, &[], 0);
        assert2::assert!(actual == Ok(b"true/false".to_vec()));
    }

    // Go1.26.5 / Prometheusv0.314.0 independent captures, SHA256
    // e6389a33185dc4fd309a4e4db8ab3795cd4bc4144eb60ba8fad8c759210e5be5.
    // Only process-local histogram addresses are bound to the separately
    // printed pointer; all fields, numeric types and slice headers are exact.
    const HISTOGRAM_FORMATTING_GOLDENS: &[(&str, &str)] = &[
        (
            "{{printf \"%p\" $h}}\n{{printf \"%s/%q/%d/%p\" $h.Validate $h.Validate $h.Validate $h.Validate}}",
            "%!s(<nil>)/%!q(<nil>)/%!d(<nil>)/%!p(<nil>)",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{$alias:=$i}}{{$copy:=$h.Copy}}{{eq (printf \"%p\" $i) (printf \"%p\" $alias)}}/{{eq (printf \"%p\" $h) (printf \"%p\" $copy)}}/{{eq $h.CustomValues nil}}/{{eq $h.PositiveBuckets nil}}",
            "true/false/true/false",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$b:=$h.PositiveBuckets}}{{$view:=slice $b 0 1}}{{$x:=$h.Div 0}}{{eq $b nil}}/{{eq $view nil}}/{{printf \"%#v\" $b}}/{{printf \"%#v\" $view}}",
            "true/false/[]float64(nil)/[]float64{1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{printf \"%s\" $i}}",
            "&{{%!s(int32=0) [{%!s(int32=0) %!s(uint32=2)}] [%!s(float64=1) %!s(float64=2)] %!s(bool=true) %!s(int=0) %!s(uint32=0) %!s(int=0) %!s(float64=0) %!s(int32=0) []} %!s(int32=0) %!s(int32=0) %!s(float64=0) %!s(bool=true)}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%d\" $h}}/{{printf \"%d\" $h.ZeroBucket}}",
            "&{3 0 %!d(float64=0.001) %!d(float64=2) %!d(float64=8) %!d(float64=10) [{0 2}] [{0 2}] [%!d(float64=1) %!d(float64=2)] [%!d(float64=1) %!d(float64=2)] []}/{%!d(float64=-0.001) %!d(float64=0.001) %!d(bool=true) %!d(bool=true) %!d(float64=2) 0}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%v\" $h}}",
            "{count:8, sum:10, [-2,-1):2, [-1,-0.5):1, [-0.001,0.001]:2, (0.5,1]:1, (1,2]:2}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%+v\" $h}}",
            "{count:8, sum:10, [-2,-1):2, [-1,-0.5):1, [-0.001,0.001]:2, (0.5,1]:1, (1,2]:2}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%#v\" $h}}",
            "&histogram.FloatHistogram{CounterResetHint:0x3, Schema:0, ZeroThreshold:0.001, ZeroCount:2, Count:8, Sum:10, PositiveSpans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, NegativeSpans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, PositiveBuckets:[]float64{1, 2}, NegativeBuckets:[]float64{1, 2}, CustomValues:[]float64(nil)}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%v\" $h.ZeroBucket}}",
            "[-0.001,0.001]:2",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%+v\" $h.ZeroBucket}}",
            "[-0.001,0.001]:2",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%#v\" $h.ZeroBucket}}",
            "histogram.Bucket[float64]{Lower:-0.001, Upper:0.001, LowerInclusive:true, UpperInclusive:true, Count:2, Index:0}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%v\" (index $h.PositiveSpans 0)}}",
            "{0 2}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%+v\" (index $h.PositiveSpans 0)}}",
            "{Offset:0 Length:2}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%#v\" (index $h.PositiveSpans 0)}}",
            "histogram.Span{Offset:0, Length:0x2}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%v\" $h.PositiveSpans}}",
            "[{0 2}]",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%+v\" $h.PositiveSpans}}",
            "[{Offset:0 Length:2}]",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%#v\" $h.PositiveSpans}}",
            "[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%v\" $h.PositiveBuckets}}",
            "[1 2]",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%+v\" $h.PositiveBuckets}}",
            "[1 2]",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%#v\" $h.PositiveBuckets}}",
            "[]float64{1, 2}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%v\" $h.CustomValues}}",
            "[]",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%+v\" $h.CustomValues}}",
            "[]",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%#v\" $h.CustomValues}}",
            "[]float64(nil)",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [1 2] true 0 0 0 0 0 []} 0 0 0 true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[1 2] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:0 currIdx:0 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{printf \"%#v\" $i}}",
            "&histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{1, 2}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:0, currIdx:0, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [2 4] true 0 1 1 1 0 []} 0 0 0 true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:true spansIdx:0 idxInSpan:1 bucketsIdx:1 currCount:1 currIdx:0 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%#v\" $i}}",
            "&histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:true, spansIdx:0, idxInSpan:0x1, bucketsIdx:1, currCount:1, currIdx:0, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [2 4] true 0 2 2 4 2 []} 0 0 0 true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:true spansIdx:0 idxInSpan:2 bucketsIdx:2 currCount:4 currIdx:2 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%#v\" $i}}",
            "&histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:true, spansIdx:0, idxInSpan:0x2, bucketsIdx:2, currCount:4, currIdx:2, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [1 2] false 0 0 0 0 0 []} 0 0 0 true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[1 2] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:0 currIdx:0 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{printf \"%#v\" $i}}",
            "&histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{1, 2}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:0, currIdx:0, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [2 4] false 0 1 1 1 0 []} 0 0 0 true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:false spansIdx:0 idxInSpan:1 bucketsIdx:1 currCount:1 currIdx:0 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%#v\" $i}}",
            "&histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:false, spansIdx:0, idxInSpan:0x1, bucketsIdx:1, currCount:1, currIdx:0, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [2 4] false 0 2 2 4 2 []} 0 0 0 true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:false spansIdx:0 idxInSpan:2 bucketsIdx:2 currCount:4 currIdx:2 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%#v\" $i}}",
            "&histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:false, spansIdx:0, idxInSpan:0x2, bucketsIdx:2, currCount:4, currIdx:2, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [1 2] true 0 0 1 0 2 []} 1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[1 2] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:1 currCount:0 currIdx:2 customValues:[]} idxInSpan:1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{printf \"%#v\" $i}}",
            "&histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{1, 2}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:1, currCount:0, currIdx:2, customValues:[]float64(nil)}, idxInSpan:1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [2 4] true 0 0 0 2 1 []} 0}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:2 currIdx:1 customValues:[]} idxInSpan:0}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%#v\" $i}}",
            "&histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:2, currIdx:1, customValues:[]float64(nil)}, idxInSpan:0}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [2 4] true 0 0 -1 2 -1 []} -1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:-1 currCount:2 currIdx:-1 customValues:[]} idxInSpan:-1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.PositiveReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%#v\" $i}}",
            "&histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:-1, currCount:2, currIdx:-1, customValues:[]float64(nil)}, idxInSpan:-1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [1 2] false 0 0 1 0 2 []} 1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[1 2] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:1 currCount:0 currIdx:2 customValues:[]} idxInSpan:1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{printf \"%#v\" $i}}",
            "&histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{1, 2}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:1, currCount:0, currIdx:2, customValues:[]float64(nil)}, idxInSpan:1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [2 4] false 0 0 0 2 1 []} 0}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:2 currIdx:1 customValues:[]} idxInSpan:0}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%#v\" $i}}",
            "&histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:2, currIdx:1, customValues:[]float64(nil)}, idxInSpan:0}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%v\" $i}}",
            "&{{0 [{0 2}] [2 4] false 0 0 -1 2 -1 []} -1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%+v\" $i}}",
            "&{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:-1 currCount:2 currIdx:-1 customValues:[]} idxInSpan:-1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.NegativeReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%#v\" $i}}",
            "&histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:-1, currCount:2, currIdx:-1, customValues:[]float64(nil)}, idxInSpan:-1}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{printf \"%v\" $i}}",
            "&{$HISTOGRAM_POINTER {{0 [{0 2}] [1 2] false 0 0 1 0 2 []} 1} {{0 [{0 2}] [1 2] true 0 0 0 0 0 []} 0 0 0 true} -1 {0 0 false false 0 0}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{printf \"%+v\" $i}}",
            "&{h:$HISTOGRAM_POINTER leftIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[1 2] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:1 currCount:0 currIdx:2 customValues:[]} idxInSpan:1} rightIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[1 2] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:0 currIdx:0 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true} state:-1 currBucket:{Lower:0 Upper:0 LowerInclusive:false UpperInclusive:false Count:0 Index:0}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{printf \"%#v\" $i}}",
            "&histogram.allFloatBucketIterator{h:(*histogram.FloatHistogram)($HISTOGRAM_POINTER), leftIter:histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{1, 2}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:1, currCount:0, currIdx:2, customValues:[]float64(nil)}, idxInSpan:1}, rightIter:histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{1, 2}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:0, currIdx:0, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}, state:-1, currBucket:histogram.Bucket[float64]{Lower:0, Upper:0, LowerInclusive:false, UpperInclusive:false, Count:0, Index:0}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%v\" $i}}",
            "&{$HISTOGRAM_POINTER {{0 [{0 2}] [2 4] false 0 0 0 2 1 []} 0} {{0 [{0 2}] [2 4] true 0 0 0 0 0 []} 0 0 0 true} -1 {-2 -1 true false 2 1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%+v\" $i}}",
            "&{h:$HISTOGRAM_POINTER leftIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:2 currIdx:1 customValues:[]} idxInSpan:0} rightIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:0 currIdx:0 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true} state:-1 currBucket:{Lower:-2 Upper:-1 LowerInclusive:true UpperInclusive:false Count:2 Index:1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%#v\" $i}}",
            "&histogram.allFloatBucketIterator{h:(*histogram.FloatHistogram)($HISTOGRAM_POINTER), leftIter:histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:2, currIdx:1, customValues:[]float64(nil)}, idxInSpan:0}, rightIter:histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:0, currIdx:0, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}, state:-1, currBucket:histogram.Bucket[float64]{Lower:-2, Upper:-1, LowerInclusive:true, UpperInclusive:false, Count:2, Index:1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%v\" $i}}",
            "&{$HISTOGRAM_POINTER {{0 [{0 2}] [2 4] false 0 0 -1 2 -1 []} -1} {{0 [{0 2}] [2 4] true 0 2 2 4 2 []} 0 0 0 true} 42 {1 2 false true 4 1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%+v\" $i}}",
            "&{h:$HISTOGRAM_POINTER leftIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:-1 currCount:2 currIdx:-1 customValues:[]} idxInSpan:-1} rightIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:true spansIdx:0 idxInSpan:2 bucketsIdx:2 currCount:4 currIdx:2 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true} state:42 currBucket:{Lower:1 Upper:2 LowerInclusive:false UpperInclusive:true Count:4 Index:1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%#v\" $i}}",
            "&histogram.allFloatBucketIterator{h:(*histogram.FloatHistogram)($HISTOGRAM_POINTER), leftIter:histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:-1, currCount:2, currIdx:-1, customValues:[]float64(nil)}, idxInSpan:-1}, rightIter:histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:true, spansIdx:0, idxInSpan:0x2, bucketsIdx:2, currCount:4, currIdx:2, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}, state:42, currBucket:histogram.Bucket[float64]{Lower:1, Upper:2, LowerInclusive:false, UpperInclusive:true, Count:4, Index:1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{printf \"%v\" $i}}",
            "&{$HISTOGRAM_POINTER {{0 [{0 2}] [1 2] true 0 0 1 0 2 []} 1} {{0 [{0 2}] [1 2] false 0 0 0 0 0 []} 0 0 0 true} -1 {0 0 false false 0 0}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{printf \"%+v\" $i}}",
            "&{h:$HISTOGRAM_POINTER leftIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[1 2] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:1 currCount:0 currIdx:2 customValues:[]} idxInSpan:1} rightIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[1 2] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:0 currIdx:0 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true} state:-1 currBucket:{Lower:0 Upper:0 LowerInclusive:false UpperInclusive:false Count:0 Index:0}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{printf \"%#v\" $i}}",
            "&histogram.allFloatBucketIterator{h:(*histogram.FloatHistogram)($HISTOGRAM_POINTER), leftIter:histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{1, 2}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:1, currCount:0, currIdx:2, customValues:[]float64(nil)}, idxInSpan:1}, rightIter:histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{1, 2}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:0, currIdx:0, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}, state:-1, currBucket:histogram.Bucket[float64]{Lower:0, Upper:0, LowerInclusive:false, UpperInclusive:false, Count:0, Index:0}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%v\" $i}}",
            "&{$HISTOGRAM_POINTER {{0 [{0 2}] [2 4] true 0 0 0 2 1 []} 0} {{0 [{0 2}] [2 4] false 0 0 0 0 0 []} 0 0 0 true} -1 {1 2 false true 2 1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%+v\" $i}}",
            "&{h:$HISTOGRAM_POINTER leftIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:2 currIdx:1 customValues:[]} idxInSpan:0} rightIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:false spansIdx:0 idxInSpan:0 bucketsIdx:0 currCount:0 currIdx:0 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true} state:-1 currBucket:{Lower:1 Upper:2 LowerInclusive:false UpperInclusive:true Count:2 Index:1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{printf \"%#v\" $i}}",
            "&histogram.allFloatBucketIterator{h:(*histogram.FloatHistogram)($HISTOGRAM_POINTER), leftIter:histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:2, currIdx:1, customValues:[]float64(nil)}, idxInSpan:0}, rightIter:histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:false, spansIdx:0, idxInSpan:0x0, bucketsIdx:0, currCount:0, currIdx:0, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}, state:-1, currBucket:histogram.Bucket[float64]{Lower:1, Upper:2, LowerInclusive:false, UpperInclusive:true, Count:2, Index:1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%v\" $i}}",
            "&{$HISTOGRAM_POINTER {{0 [{0 2}] [2 4] true 0 0 -1 2 -1 []} -1} {{0 [{0 2}] [2 4] false 0 2 2 4 2 []} 0 0 0 true} 42 {-2 -1 true false 4 1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%+v\" $i}}",
            "&{h:$HISTOGRAM_POINTER leftIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:true spansIdx:0 idxInSpan:0 bucketsIdx:-1 currCount:2 currIdx:-1 customValues:[]} idxInSpan:-1} rightIter:{baseBucketIterator:{schema:0 spans:[{Offset:0 Length:2}] buckets:[2 4] positive:false spansIdx:0 idxInSpan:2 bucketsIdx:2 currCount:4 currIdx:2 customValues:[]} targetSchema:0 origIdx:0 absoluteStartValue:0 boundReachedStartValue:true} state:42 currBucket:{Lower:-2 Upper:-1 LowerInclusive:true UpperInclusive:false Count:4 Index:1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{$i:=$h.AllReverseBucketIterator}}{{$n:=$i.Next}}{{$unused:=$h.Mul 2}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{$n:=$i.Next}}{{printf \"%#v\" $i}}",
            "&histogram.allFloatBucketIterator{h:(*histogram.FloatHistogram)($HISTOGRAM_POINTER), leftIter:histogram.reverseFloatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:true, spansIdx:0, idxInSpan:0x0, bucketsIdx:-1, currCount:2, currIdx:-1, customValues:[]float64(nil)}, idxInSpan:-1}, rightIter:histogram.floatBucketIterator{baseBucketIterator:histogram.baseBucketIterator[float64,float64]{schema:0, spans:[]histogram.Span{histogram.Span{Offset:0, Length:0x2}}, buckets:[]float64{2, 4}, positive:false, spansIdx:0, idxInSpan:0x2, bucketsIdx:2, currCount:4, currIdx:2, customValues:[]float64(nil)}, targetSchema:0, origIdx:0, absoluteStartValue:0, boundReachedStartValue:true}, state:42, currBucket:histogram.Bucket[float64]{Lower:-2, Upper:-1, LowerInclusive:true, UpperInclusive:false, Count:4, Index:1}}",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%v\" $h.Validate}}",
            "<nil>",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%+v\" $h.Validate}}",
            "<nil>",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%#v\" $h.Validate}}",
            "<nil>",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%v\" ($h.ReduceResolution 1)}}",
            "cannot reduce resolution from schema 0 to 1",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%+v\" ($h.ReduceResolution 1)}}",
            "cannot reduce resolution from schema 0 to 1",
        ),
        (
            "{{printf \"%p\" $h}}\n{{printf \"%#v\" ($h.ReduceResolution 1)}}",
            "&errors.errorString{s:\"cannot reduce resolution from schema 0 to 1\"}",
        ),
    ];

    #[test]
    fn histogram_formatting_matches_pinned_go_cursor_and_reflection_goldens() {
        for &(text, expected) in HISTOGRAM_FORMATTING_GOLDENS {
            let variables = BTreeMap::from([("h".to_owned(), template_histogram_value(fixture()))]);
            let actual = LineFormat::new_prometheus(text)
                .unwrap()
                .render_prometheus_bytes(&variables, &[], 0)
                .unwrap();
            let actual = String::from_utf8(actual).unwrap();
            let (pointer, actual) = actual.split_once('\n').unwrap();
            assert2::assert!(
                pointer.starts_with("0x")
                    && usize::from_str_radix(&pointer[2..], 16).is_ok_and(|pointer| pointer != 0)
            );
            let expected = expected.replace("$HISTOGRAM_POINTER", pointer);
            assert2::assert!(actual == expected, "{text}");
        }
    }

    #[test]
    fn custom_copy_shares_interned_bounds_and_deep_copies_bucket_counts() {
        let mut histogram = fixture();
        histogram.schema = -53;
        histogram.custom_values = Some(vec![1.0, 2.0]);
        histogram.negative_spans.clear();
        histogram.negative_counts.clear();
        histogram.zero_count = 0.0;
        histogram.zero_threshold = 0.0;
        let variables = BTreeMap::from([("h".into(), template_histogram_value(histogram))]);
        // Independently executed pinned Go text/template copy_probe: true/false.
        let text = "{{$c:=$h.Copy}}{{eq (printf \"%p\" $h.CustomValues) (printf \"%p\" $c.CustomValues)}}/{{eq (printf \"%p\" $h.PositiveBuckets) (printf \"%p\" $c.PositiveBuckets)}}";
        let actual = LineFormat::new_prometheus(text)
            .unwrap()
            .render_prometheus_bytes(&variables, &[], 0);
        assert2::assert!(actual == Ok(b"true/false".to_vec()));
    }
    #[test]
    fn pointer_flags_reach_histogram_iterator_and_slice_callers() {
        let variables = BTreeMap::from([("h".into(), template_histogram_value(fixture()))]);
        // Go p flags: # removes only the address prefix; + adds a sign.
        // Relations use each actual process-local pointer, never oracle addresses.
        let text = "{{$i:=$h.PositiveBucketIterator}}{{$b:=$h.PositiveBuckets}}{{eq (printf \"%#p\" $h) (slice (printf \"%p\" $h) 2)}}/{{eq (printf \"%+p\" $i) (printf \"+%p\" $i)}}/{{eq (printf \"%#p\" $b) (slice (printf \"%p\" $b) 2)}}";
        let actual = LineFormat::new_prometheus(text)
            .unwrap()
            .render_prometheus_bytes(&variables, &[], 0);
        assert2::assert!(actual == Ok(b"true/true/true".to_vec()));
    }
    #[test]
    fn invalid_source_method_signatures_reject_without_mutation() {
        for (name, args, count) in [
            ("CopyTo", "$h.Copy", 0),
            ("Add", "$h.Copy", 4),
            ("KahanAdd", "$h.Copy $h.Copy", 4),
            ("Sub", "$h.Copy", 4),
        ] {
            let value = template_histogram_value(fixture());
            let V::FloatHistogram(histogram) = &value else {
                panic!("expected histogram");
            };
            let variables = BTreeMap::from([("h".to_owned(), value.clone())]);
            let text = format!("{{{{ $h.{name} {args} }}}}");
            let error = LineFormat::new_prometheus(text)
                .unwrap()
                .render_prometheus_bytes(&variables, &[], 0)
                .unwrap_err();
            assert2::assert!(
                error
                    .to_string()
                    .contains(&format!("{name} has {count} return values"))
            );
            assert2::assert!(histogram.snapshot().count.to_bits() == 8.0_f64.to_bits());
        }
    }
    #[test]
    fn mutable_histogram_fields_and_aliases_are_reset_between_replays() {
        let variables = BTreeMap::from([("h".to_owned(), template_histogram_value(fixture()))]);
        let format = LineFormat::new_prometheus("{{ $b := $h.PositiveBuckets }}{{ $alias := $h }}{{ $x := $h.Mul 2 }}{{ index $b 0 }}/{{ $alias.Count }}{{ query (printf \"vector(%g)\" (index $b 0)) | first | value }}").unwrap();
        let first = format
            .render_prometheus_bytes(&variables, &[], 60_000)
            .unwrap_err();
        assert2::assert!(
            first == krabka_logql::TemplateRenderError::NeedsQuery("vector(2)".into())
        );
        let result = V::QueryResult(
            vec![V::Sample(std::sync::Arc::new(BTreeMap::from([
                ("Labels".into(), V::ByteLabels(BTreeMap::new())),
                ("Value".into(), V::Float(2.0)),
            ])))]
            .into(),
        );
        let second = format
            .render_prometheus_bytes(&variables, &[result], 60_000)
            .unwrap();
        assert2::assert!(second == b"2/162");
        let V::FloatHistogram(histogram) = &variables["h"] else {
            panic!("expected histogram");
        };
        assert2::assert!(histogram.snapshot().count.to_bits() == 8.0_f64.to_bits());
    }
}
