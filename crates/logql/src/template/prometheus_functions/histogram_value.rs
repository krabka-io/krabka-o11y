use std::sync::{Arc, Mutex};

use super::{super::TemplateRuntimeValue as V, histogram::TemplateFloatHistogram};

/// Mutable source histogram pointer, with operations supplied by its producer.
/// This keeps the template engine independent of the metric engine.
#[derive(Clone)]
pub struct TemplateHistogram {
    value: Arc<Mutex<TemplateFloatHistogram>>,
    call: fn(&Self, &str, &[V]) -> Result<V, String>,
    slices: Arc<Mutex<std::collections::BTreeMap<String, TemplateHistogramSlice>>>,
}
impl std::fmt::Debug for TemplateHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.snapshot().fmt(f)
    }
}
impl PartialEq for TemplateHistogram {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}
impl TemplateHistogram {
    #[must_use]
    pub fn new(
        value: TemplateFloatHistogram,
        call: fn(&Self, &str, &[V]) -> Result<V, String>,
    ) -> Self {
        Self {
            value: Arc::new(Mutex::new(value)),
            call,
            slices: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        }
    }
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared histogram or slice lock is poisoned.
    pub fn snapshot(&self) -> TemplateFloatHistogram {
        self.value
            .lock()
            .expect("template histogram poisoned")
            .clone()
    }
    /// # Panics
    ///
    /// Panics if a shared histogram or slice lock is poisoned.
    pub fn replace(&self, value: TemplateFloatHistogram) {
        let mut slices = self.slices.lock().expect("histogram slices poisoned");
        for (name, slice) in slices.iter_mut() {
            let Some(V::FloatSlice(replacement) | V::HistogramSpans(replacement)) =
                value.field(name)
            else {
                continue;
            };
            slice.replace(&replacement.snapshot(), replacement.is_nil());
        }
        *self.value.lock().expect("template histogram poisoned") = value;
    }
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared histogram lock is poisoned.
    pub fn copy(&self) -> Self {
        let mut value = self.snapshot();
        value.nil_slices = 16;
        for (bit, empty) in [
            (1, value.positive_spans.is_empty()),
            (2, value.negative_spans.is_empty()),
            (4, value.positive_buckets.is_empty()),
            (8, value.negative_buckets.is_empty()),
        ] {
            if empty {
                value.nil_slices |= bit;
            }
        }
        if value.schema == -53 {
            value.nil_slices = (value.nil_slices & !16) | (self.snapshot().nil_slices & 16);
            value.zero_count = 0.0;
            value.zero_threshold = 0.0;
            value.negative_spans.clear();
            value.negative_buckets.clear();
            value.nil_slices |= 2 | 8;
        }
        let copy = Self::new(value, self.call);
        if self.snapshot().schema == -53 {
            // Source Copy reuses interned custom bounds, while bucket counts
            // and spans are deep copied. Capture its header, not a shared
            // reflected field header that future assignments could replace.
            let V::FloatSlice(bounds) = self.field("CustomValues").expect("custom bounds") else {
                unreachable!("custom bounds are a float slice")
            };
            copy.slices
                .lock()
                .expect("histogram slices poisoned")
                .insert("CustomValues".into(), bounds.slice(0, bounds.len()));
        }
        copy
    }
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.value, &other.value)
    }
    pub(crate) fn call(&self, name: &str, args: &[V]) -> Result<V, String> {
        (self.call)(self, name, args)
    }
    pub(crate) fn as_string(&self) -> String {
        self.snapshot().as_string()
    }
    /// # Panics
    ///
    /// Panics if a shared histogram or slice lock is poisoned.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<V> {
        let value = self.snapshot().field(name)?;
        match value {
            V::FloatSlice(values) | V::HistogramSpans(values) => {
                let slice = self
                    .slices
                    .lock()
                    .expect("histogram slices poisoned")
                    .entry(name.to_owned())
                    .or_insert(values)
                    .clone();
                Some(if name.ends_with("Spans") {
                    V::HistogramSpans(slice)
                } else {
                    V::FloatSlice(slice)
                })
            }
            value
                if matches!(
                    name,
                    "Count" | "Sum" | "ZeroCount" | "ZeroThreshold" | "Schema" | "CounterResetHint"
                ) =>
            {
                let _ = value;
                Some(V::Reference(TemplateHistogramReference::Field(
                    self.clone(),
                    name.to_owned(),
                )))
            }
            value => Some(value),
        }
    }
}

/// A reflected histogram slice field retains its header across assignments.
/// Explicit slice views capture a header while sharing the element backing.
#[derive(Clone, Debug)]
pub struct TemplateHistogramSlice(
    Arc<Mutex<super::query_result::TemplateQueryResult>>,
    Arc<Mutex<bool>>,
);
impl PartialEq for TemplateHistogramSlice {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl From<Vec<V>> for TemplateHistogramSlice {
    fn from(values: Vec<V>) -> Self {
        Self(
            Arc::new(Mutex::new(values.into())),
            Arc::new(Mutex::new(false)),
        )
    }
}
impl TemplateHistogramSlice {
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared slice header lock is poisoned.
    pub fn capture(&self) -> super::query_result::TemplateQueryResult {
        self.0.lock().expect("slice header poisoned").clone()
    }
    #[must_use]
    /// # Panics
    ///
    /// Panics if the shared histogram or slice lock is poisoned.
    pub fn snapshot(&self) -> Vec<V> {
        self.capture().snapshot()
    }
    #[must_use]
    pub fn get(&self, index: usize) -> Option<V> {
        let values = self.capture();
        values.get(index)?;
        Some(V::Reference(TemplateHistogramReference::Element(
            values,
            index,
            Vec::new(),
        )))
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.capture().len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn slice(&self, start: usize, end: usize) -> Self {
        Self(
            Arc::new(Mutex::new(self.capture().slice(start, end))),
            Arc::new(Mutex::new(self.is_nil())),
        )
    }
    #[must_use]
    pub(crate) fn address(&self) -> usize {
        if self.is_nil() {
            0
        } else {
            self.capture().backing_address()
        }
    }
    /// # Panics
    ///
    /// Panics if the shared slice header lock is poisoned.
    #[must_use]
    pub fn is_nil(&self) -> bool {
        *self.1.lock().expect("slice nil header poisoned")
    }
    pub(crate) fn set_nil(&self, is_nil: bool) {
        *self.1.lock().expect("slice nil header poisoned") = is_nil;
    }
    fn replace(&self, replacement: &[V], is_nil: bool) {
        self.set_nil(is_nil);
        let mut header = self.0.lock().expect("slice header poisoned");
        *header = header.replace_prefix(replacement);
    }
}
/// Addressable source reflect.Value retained by a template variable.
#[derive(Clone, Debug, PartialEq)]
pub enum TemplateHistogramReference {
    Field(TemplateHistogram, String),
    Element(super::query_result::TemplateQueryResult, usize, Vec<String>),
}
impl TemplateHistogramReference {
    pub(crate) fn resolve(&self) -> V {
        match self {
            Self::Field(histogram, name) => {
                histogram.snapshot().field(name).expect("histogram field")
            }
            Self::Element(values, index, path) => {
                let value = values.get(*index).expect("captured slice element");
                super::super::template_variable_path_value(&value, path)
                    .expect("captured element field")
            }
        }
    }
    pub(crate) fn field(&self, name: &str) -> Option<V> {
        match self {
            Self::Element(values, index, path) => {
                let mut path = path.clone();
                path.push(name.to_owned());
                let value = values.get(*index)?;
                super::super::template_variable_path_value(&value, &path)?;
                Some(V::Reference(Self::Element(values.clone(), *index, path)))
            }
            Self::Field(_, _) => None,
        }
    }
}

/// Source error dynamic type and its explicit unwrap chain.
#[derive(Clone, Debug, PartialEq)]
pub struct TemplateHistogramError {
    pub message: String,
    pub kind: &'static str,
    pub unwrap: Option<Arc<Self>>,
}
impl TemplateHistogramError {
    #[must_use]
    pub fn plain(message: String) -> Arc<Self> {
        Arc::new(Self {
            message,
            kind: "*errors.errorString",
            unwrap: None,
        })
    }
    pub(crate) fn field(&self, name: &str) -> Option<V> {
        match name {
            "Error" => Some(V::String(self.message.clone())),
            "Unwrap" if self.kind != "*errors.errorString" => Some(
                self.unwrap
                    .as_ref()
                    .map_or(V::NilError, |value| V::HistogramError(Arc::clone(value))),
            ),
            _ => None,
        }
    }
}

/// Source `histogram.Bucket[float64]`, including its index and endpoint types.
#[derive(Clone, Debug, PartialEq)]
pub struct TemplateBucket {
    pub lower: f64,
    pub upper: f64,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub count: f64,
    pub index: i32,
}
impl Default for TemplateBucket {
    fn default() -> Self {
        Self {
            lower: 0.0,
            upper: 0.0,
            lower_inclusive: false,
            upper_inclusive: false,
            count: 0.0,
            index: 0,
        }
    }
}
impl TemplateBucket {
    pub(crate) fn field(&self, name: &str) -> Option<V> {
        Some(match name {
            "Lower" => V::Float(self.lower),
            "Upper" => V::Float(self.upper),
            "Count" => V::Float(self.count),
            "Index" => V::Integer32(self.index),
            "LowerInclusive" => V::Json(self.lower_inclusive.into()),
            "UpperInclusive" => V::Json(self.upper_inclusive.into()),
            "String" => V::String(self.as_string()),
            _ => return None,
        })
    }
    pub(crate) fn as_string(&self) -> String {
        let float = |v| {
            super::super::format_template_printf::format_template_printf_values(&[
                V::String("%g".into()),
                V::Float(v),
            ])
        };
        format!(
            "{}{},{}{}:{}",
            if self.lower_inclusive { '[' } else { '(' },
            float(self.lower),
            float(self.upper),
            if self.upper_inclusive { ']' } else { ')' },
            float(self.count)
        )
    }
}
/// Pointer identity and cursor state are shared by source iterator aliases.
#[derive(Clone)]
pub struct TemplateBucketIterator {
    state: Arc<Mutex<IteratorState>>,
    pub kind: &'static str,
    reflection: Arc<dyn Fn() -> TemplateHistogramView + Send + Sync>,
}
struct IteratorState {
    current: TemplateBucket,
    next: Box<dyn FnMut() -> (bool, TemplateBucket) + Send>,
}
impl std::fmt::Debug for TemplateBucketIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state.lock().expect("iterator poisoned").current.fmt(f)
    }
}
impl PartialEq for TemplateBucketIterator {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}
impl TemplateBucketIterator {
    pub fn new(
        initial: TemplateBucket,
        kind: &'static str,
        next: impl FnMut() -> (bool, TemplateBucket) + Send + 'static,
        reflection: impl Fn() -> TemplateHistogramView + Send + Sync + 'static,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(IteratorState {
                current: initial,
                next: Box::new(next),
            })),
            kind,
            reflection: Arc::new(reflection),
        }
    }
    pub(crate) fn reflection(&self) -> TemplateHistogramView {
        TemplateHistogramView::Pointer {
            name: self.kind.strip_prefix('*').expect("iterator pointer"),
            address: Arc::as_ptr(&self.state) as usize,
            value: Box::new((self.reflection)()),
        }
    }
    pub(crate) fn call(&self, name: &str, args: &[V]) -> Result<V, String> {
        if !args.is_empty() {
            return Err(format!("wrong number of args for {name}"));
        }
        let mut state = self.state.lock().expect("template iterator poisoned");
        match name {
            "Next" => {
                let (advanced, current) = (state.next)();
                state.current = current;
                Ok(V::Json(advanced.into()))
            }
            "At" => Ok(V::HistogramBucket(state.current.clone())),
            _ => Err(format!("cannot evaluate field or method {name}")),
        }
    }
}

static EMPTY_BACKING: u8 = 0;

/// Ordered, typed source fields needed by Go fmt reflection of histogram objects.
#[derive(Clone, Debug)]
pub enum TemplateHistogramView {
    Scalar(V),
    NamedScalar {
        name: &'static str,
        value: V,
    },
    Struct {
        name: &'static str,
        fields: Vec<(&'static str, Self)>,
    },
    Slice {
        name: &'static str,
        values: Vec<Self>,
        is_nil: bool,
        address: usize,
    },
    Pointer {
        name: &'static str,
        address: usize,
        value: Box<Self>,
    },
}
impl TemplateHistogramView {
    #[must_use]
    pub fn scalar(value: V) -> Self {
        Self::Scalar(value)
    }
    #[must_use]
    pub fn structure(name: &'static str, fields: Vec<(&'static str, Self)>) -> Self {
        Self::Struct { name, fields }
    }
    #[must_use]
    pub fn slice(name: &'static str, values: Vec<V>, is_nil: bool) -> Self {
        Self::Slice {
            name,
            values: values.into_iter().map(Self::Scalar).collect(),
            is_nil,
            address: if is_nil {
                0
            } else {
                std::ptr::addr_of!(EMPTY_BACKING) as usize
            },
        }
    }
    #[must_use]
    pub fn slice_at(name: &'static str, values: Vec<V>, is_nil: bool, address: usize) -> Self {
        Self::Slice {
            name,
            values: values.into_iter().map(Self::Scalar).collect(),
            is_nil,
            address: if is_nil { 0 } else { address },
        }
    }
}
impl TemplateHistogram {
    #[must_use]
    pub fn address(&self) -> usize {
        Arc::as_ptr(&self.value) as usize
    }
    #[must_use]
    /// # Panics
    ///
    /// Panics if a shared histogram lock is poisoned.
    pub fn reflection(&self) -> TemplateHistogramView {
        let fields = [
            "CounterResetHint",
            "Schema",
            "ZeroThreshold",
            "ZeroCount",
            "Count",
            "Sum",
            "PositiveSpans",
            "NegativeSpans",
            "PositiveBuckets",
            "NegativeBuckets",
            "CustomValues",
        ]
        .into_iter()
        .map(|name| {
            (
                name,
                TemplateHistogramView::Scalar(self.field(name).expect("histogram field")),
            )
        })
        .collect();
        TemplateHistogramView::Pointer {
            name: "histogram.FloatHistogram",
            address: self.address(),
            value: Box::new(TemplateHistogramView::structure(
                "histogram.FloatHistogram",
                fields,
            )),
        }
    }
}
impl TemplateBucket {
    #[must_use]
    /// # Panics
    ///
    /// Panics if a shared histogram lock is poisoned.
    pub fn reflection(&self) -> TemplateHistogramView {
        TemplateHistogramView::structure(
            "histogram.Bucket[float64]",
            [
                "Lower",
                "Upper",
                "LowerInclusive",
                "UpperInclusive",
                "Count",
                "Index",
            ]
            .into_iter()
            .map(|name| {
                (
                    name,
                    TemplateHistogramView::Scalar(self.field(name).expect("bucket field")),
                )
            })
            .collect(),
        )
    }
}
