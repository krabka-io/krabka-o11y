//! DTO for source-visible histogram values without a `PromQL` dependency edge.
//! Prometheus v3.14.0 `model/histogram/float_histogram.go::String` and
//! `model/histogram/generic.go::Bucket.String` determine the display ledger.
use std::fmt::Write as _;

#[derive(Clone, Debug, PartialEq)]
pub struct TemplateHistogramSpan {
    pub offset: i32,
    pub length: u32,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TemplateHistogramBucket {
    pub lower: f64,
    pub upper: f64,
    pub count: f64,
    /// 0: `(lower,upper]`, 1: `[lower,upper)`, 3: `[lower,upper]`.
    pub boundary_rule: u8,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TemplateFloatHistogram {
    pub counter_reset_hint: u8,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<TemplateHistogramSpan>,
    pub negative_spans: Vec<TemplateHistogramSpan>,
    pub positive_buckets: Vec<f64>,
    pub negative_buckets: Vec<f64>,
    pub custom_values: Vec<f64>,
    /// Source nil slice headers: spans +/-, buckets +/-, custom values.
    pub nil_slices: u8,
    /// Source iterator order: negative ascending, zero, positive ascending.
    pub display_buckets: Vec<TemplateHistogramBucket>,
}
impl TemplateFloatHistogram {
    /// Prometheus histogram numbers use Go fmt %g.
    #[must_use]
    pub fn format_float(value: f64) -> String {
        float(value)
    }
    pub(crate) fn as_string(&self) -> String {
        let mut output = format!("{{count:{}, sum:{}", float(self.count), float(self.sum));
        for bucket in &self.display_buckets {
            if bucket.count == 0.0 {
                continue;
            }
            write!(
                output,
                ", {}{},{}{}:{}",
                if matches!(bucket.boundary_rule, 1 | 3) {
                    '['
                } else {
                    '('
                },
                float(bucket.lower),
                float(bucket.upper),
                if matches!(bucket.boundary_rule, 0 | 3) {
                    ']'
                } else {
                    ')'
                },
                float(bucket.count)
            )
            .expect("writing a String cannot fail");
        }
        output.push('}');
        output
    }
    pub(crate) fn field(&self, name: &str) -> Option<super::super::TemplateRuntimeValue> {
        use super::super::TemplateRuntimeValue as V;
        let value = match name {
            "Count" => V::Float(self.count),
            "Sum" => V::Float(self.sum),
            "ZeroCount" => V::Float(self.zero_count),
            "ZeroThreshold" => V::Float(self.zero_threshold),
            "Schema" => V::Integer32(self.schema),
            "CounterResetHint" => V::CounterResetHint(self.counter_reset_hint),
            "PositiveBuckets" => V::FloatSlice(
                self.positive_buckets
                    .iter()
                    .copied()
                    .map(V::Float)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            "NegativeBuckets" => V::FloatSlice(
                self.negative_buckets
                    .iter()
                    .copied()
                    .map(V::Float)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            "CustomValues" => V::FloatSlice(
                self.custom_values
                    .iter()
                    .copied()
                    .map(V::Float)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            "PositiveSpans" => V::HistogramSpans(
                self.positive_spans
                    .iter()
                    .cloned()
                    .map(V::HistogramSpan)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            "NegativeSpans" => V::HistogramSpans(
                self.negative_spans
                    .iter()
                    .cloned()
                    .map(V::HistogramSpan)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            "String" => V::String(self.as_string()),
            "UsesCustomBuckets" => V::Json(serde_json::Value::Bool(self.schema == -53)),
            _ => return None,
        };
        let bit = match name {
            "PositiveSpans" => 1,
            "NegativeSpans" => 2,
            "PositiveBuckets" => 4,
            "NegativeBuckets" => 8,
            "CustomValues" => 16,
            _ => 0,
        };
        if let V::FloatSlice(slice) | V::HistogramSpans(slice) = &value {
            slice.set_nil(self.nil_slices & bit != 0);
        }
        Some(value)
    }
}
fn float(value: f64) -> String {
    super::super::format_template_printf::format_template_printf_values(&[
        super::super::TemplateRuntimeValue::String("%g".into()),
        super::super::TemplateRuntimeValue::Float(value),
    ])
}
