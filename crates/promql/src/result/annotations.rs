/// Warnings and info annotations from a query evaluation.
///
/// This type mirrors the `util/annotations` channel of Prometheus.
/// `PromQLWarning`-class messages go into [`Annotations::warnings`], and
/// `PromQLInfo`-class messages go into [`Annotations::infos`]. The engine
/// removes duplicate messages and keeps the exact Prometheus annotation text.
/// The text has no trailing position suffix, because Krabka does not track that
/// suffix through evaluation.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Annotations {
    /// `PromQL warning:`-class annotations, in first-seen order.
    pub warnings: Vec<String>,
    /// `PromQL info:`-class annotations, in first-seen order.
    pub infos: Vec<String>,
}

impl Annotations {
    /// An empty annotation set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a warning and ignores exact duplicates.
    pub fn warn(&mut self, message: impl Into<String>) {
        let message = message.into();
        if !self.warnings.contains(&message) {
            self.warnings.push(message);
        }
    }

    /// Records an info annotation and ignores exact duplicates.
    pub fn info(&mut self, message: impl Into<String>) {
        let message = message.into();
        if !self.infos.contains(&message) {
            self.infos.push(message);
        }
    }

    /// Merges the annotations of another set into this set, without duplicates.
    pub fn extend(&mut self, other: &Annotations) {
        for warning in &other.warnings {
            self.warn(warning.clone());
        }
        for info in &other.infos {
            self.info(info.clone());
        }
    }

    /// Returns `true` when the set has no annotations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.warnings.is_empty() && self.infos.is_empty()
    }
}
