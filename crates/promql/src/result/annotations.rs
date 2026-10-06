/// Warnings and info annotations from a query evaluation.
///
/// This type mirrors the `util/annotations` channel of Prometheus.
/// `PromQLWarning`-class messages go into [`Annotations::warnings`], and
/// `PromQLInfo`-class messages go into [`Annotations::infos`]. The engine
/// removes duplicate messages and keeps the exact Prometheus annotation text.
/// Each engine annotation includes the original expression source position.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Annotations {
    /// `PromQL warning:`-class annotations, in first-seen order.
    pub warnings: Vec<String>,
    /// `PromQL info:`-class annotations, in first-seen order.
    pub infos: Vec<String>,
    /// Structured classic-histogram repairs; HTTP responses render their
    /// merged details while engine consumers retain the base info messages.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub histogram_quantile_repairs: BTreeMap<String, HistogramQuantileRepair>,
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
        for (message, repair) in &other.histogram_quantile_repairs {
            self.histogram_quantile_repairs
                .entry(message.clone())
                .and_modify(|existing| existing.merge(repair))
                .or_insert_with(|| repair.clone());
        }
    }

    pub(crate) fn histogram_quantile_repair(
        &mut self,
        message: String,
        repair: HistogramQuantileRepair,
    ) {
        self.info(message.clone());
        self.histogram_quantile_repairs
            .entry(message)
            .and_modify(|existing| existing.merge(&repair))
            .or_insert(repair);
    }

    pub(crate) fn http_infos(&self) -> Vec<String> {
        self.infos
            .iter()
            .map(|message| {
                self.histogram_quantile_repairs
                    .get(message)
                    .and_then(|repair| repair.render(message))
                    .unwrap_or_else(|| message.clone())
            })
            .collect()
    }

    /// Returns `true` when the set has no annotations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.warnings.is_empty() && self.infos.is_empty()
    }
}
use std::collections::BTreeMap;

use super::HistogramQuantileRepair;
