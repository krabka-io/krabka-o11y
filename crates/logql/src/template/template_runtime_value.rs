use super::{
    format_template_float, template_json_value_to_string, template_json_value_truthy,
    template_string_truthy,
};

#[derive(Clone, Debug, PartialEq)]
/// Typed values shared by Loki formatting and Prometheus rule templates.
pub enum TemplateRuntimeValue {
    String(String),
    Integer(i64),
    Integer64(i64),
    Byte(u8),
    Float(f64),
    Complex(f64, f64),
    Json(serde_json::Value),
    Labels(super::Labels),
    Bytes(Vec<u8>),
    ByteLabels(std::collections::BTreeMap<String, Vec<u8>>),
    Object(std::collections::BTreeMap<String, TemplateRuntimeValue>),
    Array(Vec<TemplateRuntimeValue>),
    Time(super::TemplateTime),
    TimeLocation(super::template_time::TemplateTimeLocation),
    Duration(i64),
    Month(u8),
    Weekday(u8),
    ByteSlice(Vec<u8>),
    Integer32(i32),
    Unsigned32(u32),
    CounterResetHint(u8),
    FloatSlice(super::TemplateHistogramSlice),
    HistogramSpans(super::TemplateHistogramSlice),
    HistogramSpan(super::prometheus_functions::histogram::TemplateHistogramSpan),
    FloatHistogram(super::TemplateHistogram),
    Reference(super::TemplateHistogramReference),
    HistogramBucket(super::TemplateBucket),
    HistogramIterator(super::TemplateBucketIterator),
    QueryError(String),
    HistogramError(std::sync::Arc<super::TemplateHistogramError>),
    NilError,
    TimePointer(std::sync::Arc<super::TemplateTime>),
    DurationPointer(std::sync::Arc<i64>),
    SafeHtml(Vec<u8>),
    QueryResult(super::prometheus_functions::query_result::TemplateQueryResult),
    Sample(std::sync::Arc<std::collections::BTreeMap<String, TemplateRuntimeValue>>),
}

impl TemplateRuntimeValue {
    pub(crate) fn resolved(&self) -> Self {
        match self {
            Self::Reference(value) => value.resolve(),
            value => value.clone(),
        }
    }

    pub(crate) fn clone_for_query_execution(&self) -> Self {
        match self {
            Self::QueryResult(values) => Self::QueryResult(
                values
                    .snapshot()
                    .iter()
                    .map(Self::clone_for_query_execution)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            Self::Sample(value) => Self::Sample(std::sync::Arc::new(
                value
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone_for_query_execution()))
                    .collect(),
            )),
            Self::FloatHistogram(value) => Self::FloatHistogram(value.copy()),
            value => value.clone(),
        }
    }
    pub(crate) fn as_rendered_string(&self) -> String {
        super::template_bytes_to_string(&self.rendered_bytes())
    }

    pub(crate) fn string_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::String(value) | Self::Json(serde_json::Value::String(value)) => {
                Some(value.as_bytes())
            }
            Self::Bytes(value) | Self::SafeHtml(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn rendered_bytes(&self) -> Vec<u8> {
        match self {
            Self::Reference(value) => value.resolve().rendered_bytes(),
            Self::Bytes(value) | Self::SafeHtml(value) => value.clone(),
            Self::TimePointer(value) => value.as_string().into_bytes(),
            Self::DurationPointer(value) => {
                super::template_time::duration::string(**value).into_bytes()
            }
            Self::Sample(value) => super::format_template_printf_bytes(&[
                Self::String("%v".into()),
                Self::Sample(value.clone()),
            ]),
            Self::String(value) => value.as_bytes().to_vec(),
            Self::Integer(value) | Self::Integer64(value) => value.to_string().into_bytes(),
            Self::Duration(value) => super::template_time::duration::string(*value).into_bytes(),
            Self::Month(value) => super::template_time::layout::MONTHS
                .get(usize::from(value.wrapping_sub(1)))
                .map_or_else(
                    || format!("%!Month({value})").into_bytes(),
                    |name| name.as_bytes().to_vec(),
                ),
            Self::Weekday(value) => super::template_time::layout::DAYS
                .get(usize::from(*value))
                .map_or_else(
                    || format!("%!Weekday({value})").into_bytes(),
                    |name| name.as_bytes().to_vec(),
                ),
            Self::TimeLocation(value) => value.name().into_bytes(),
            Self::ByteSlice(value) => {
                Self::Array(value.iter().copied().map(Self::Byte).collect()).rendered_bytes()
            }
            Self::Byte(value) | Self::CounterResetHint(value) => value.to_string().into_bytes(),
            Self::Integer32(value) => value.to_string().into_bytes(),
            Self::Unsigned32(value) => u64::from(*value).to_string().into_bytes(),
            Self::FloatHistogram(value) => value.as_string().into_bytes(),
            Self::HistogramBucket(value) => value.as_string().into_bytes(),
            Self::HistogramIterator(_) => {
                super::format_template_printf_bytes(&[Self::String("%v".into()), self.clone()])
            }
            Self::FloatSlice(values) | Self::HistogramSpans(values) => {
                Self::Array(values.snapshot()).rendered_bytes()
            }
            Self::HistogramSpan(value) => {
                format!("{{{} {}}}", value.offset, value.length).into_bytes()
            }
            Self::Float(value) => format_template_float(*value).into_bytes(),
            Self::Complex(real, imaginary) => super::format_template_printf_bytes(&[
                Self::String("%v".into()),
                Self::Complex(*real, *imaginary),
            ]),
            Self::Json(value) => template_json_value_to_string(value).into_bytes(),
            Self::Labels(value) => {
                template_json_value_to_string(&serde_json::json!(value)).into_bytes()
            }
            Self::Time(value) => value.as_string().into_bytes(),
            Self::QueryError(_) => Vec::new(),
            Self::HistogramError(error) => error.message.as_bytes().to_vec(),
            Self::NilError => b"<nil>".to_vec(),
            Self::QueryResult(values) => Self::Array(values.snapshot()).rendered_bytes(),
            Self::Array(values) => {
                let mut output = b"[".to_vec();
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(b' ');
                    }
                    output.extend(value.rendered_bytes());
                }
                output.push(b']');
                output
            }
            Self::Object(values) => render_map(
                values
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.rendered_bytes())),
            ),
            Self::ByteLabels(values) => render_map(
                values
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.clone())),
            ),
        }
    }

    pub(crate) fn as_numeric_string(&self, integer: bool) -> String {
        match self {
            Self::Reference(value) => value.resolve().as_numeric_string(integer),
            Self::Json(serde_json::Value::Bool(value)) => {
                if *value { "1" } else { "0" }.to_string()
            }
            Self::Json(serde_json::Value::Null) | Self::Complex(_, _) => "0".to_string(),
            Self::Duration(value) => value.to_string(),
            Self::Month(value) | Self::Weekday(value) => value.to_string(),
            Self::Float(value) if integer => float_to_integer(*value),
            Self::Json(serde_json::Value::Number(value)) if integer => {
                float_to_integer(value.as_f64().unwrap_or_default())
            }
            _ if !integer && self.is_template_string() => {
                let value = self.as_rendered_string();
                super::template_value::number::parse_float(&value)
                    .map_or(value, format_template_float)
            }
            _ => self.as_rendered_string(),
        }
    }

    pub(crate) fn signed_integer(&self) -> Option<i64> {
        match self {
            Self::Reference(value) => value.resolve().signed_integer(),
            Self::Integer(value) | Self::Integer64(value) | Self::Duration(value) => Some(*value),
            Self::Month(value) | Self::Weekday(value) | Self::CounterResetHint(value) => {
                Some(i64::from(*value))
            }
            Self::Integer32(value) => Some(i64::from(*value)),
            Self::Unsigned32(value) => Some(i64::from(*value)),
            _ => None,
        }
    }

    pub(crate) fn is_template_string(&self) -> bool {
        matches!(
            self,
            Self::String(_) | Self::Bytes(_) | Self::Json(serde_json::Value::String(_))
        )
    }

    pub(crate) fn is_truthy(&self) -> bool {
        match self {
            Self::Reference(value) => value.resolve().is_truthy(),
            Self::String(value) => template_string_truthy(value),
            Self::Integer(value) | Self::Integer64(value) | Self::Duration(value) => *value != 0,
            Self::Month(value)
            | Self::Weekday(value)
            | Self::Byte(value)
            | Self::CounterResetHint(value) => *value != 0,
            Self::Integer32(value) => *value != 0,
            Self::Unsigned32(value) => *value > 0,
            Self::FloatSlice(value) | Self::HistogramSpans(value) => !value.is_empty(),
            Self::HistogramSpan(_)
            | Self::FloatHistogram(_)
            | Self::HistogramBucket(_)
            | Self::HistogramIterator(_)
            | Self::TimePointer(_)
            | Self::DurationPointer(_)
            | Self::Sample(_)
            | Self::Time(_)
            | Self::TimeLocation(_)
            | Self::HistogramError(_) => true,
            Self::Float(value) => *value != 0.0,
            Self::Complex(real, imaginary) => *real != 0.0 || *imaginary != 0.0,
            Self::Json(value) => template_json_value_truthy(value),
            Self::Labels(value) => !value.is_empty(),
            Self::Bytes(value) | Self::SafeHtml(value) | Self::ByteSlice(value) => {
                !value.is_empty()
            }
            Self::ByteLabels(value) => !value.is_empty(),
            Self::Object(value) => !value.is_empty(),
            Self::Array(value) => !value.is_empty(),
            Self::QueryResult(value) => !value.is_empty(),
            Self::QueryError(_) | Self::NilError => false,
        }
    }
}

fn float_to_integer(value: f64) -> String {
    // Go's amd64 conversion returns MinInt64 for a non-finite or overflowing float.
    if value.is_finite()
        && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value)
    {
        format!("{:.0}", value.trunc())
    } else {
        i64::MIN.to_string()
    }
}

fn render_map<'a>(entries: impl Iterator<Item = (&'a str, Vec<u8>)>) -> Vec<u8> {
    let mut output = b"map[".to_vec();
    for (index, (key, value)) in entries.enumerate() {
        if index > 0 {
            output.push(b' ');
        }
        output.extend_from_slice(key.as_bytes());
        output.push(b':');
        output.extend(value);
    }
    output.push(b']');
    output
}
