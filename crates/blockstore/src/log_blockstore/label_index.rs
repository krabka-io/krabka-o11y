use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LabelIndex {
    pub(crate) series: BTreeMap<String, BTreeMap<SeriesFingerprint, Labels>>,
    pub(crate) postings: BTreeMap<(String, String, String), BTreeSet<SeriesFingerprint>>,
}

impl LabelIndex {
    pub fn insert_series(
        &mut self,
        tenant: impl Into<String>,
        labels: Labels,
    ) -> SeriesFingerprint {
        let tenant = tenant.into();
        let fingerprint = series_fingerprint(&labels);
        for (name, value) in &labels {
            self.postings
                .entry((tenant.clone(), name.clone(), value.clone()))
                .or_default()
                .insert(fingerprint);
        }
        self.series
            .entry(tenant)
            .or_default()
            .insert(fingerprint, labels);
        fingerprint
    }

    #[must_use]
    pub fn match_series(
        &self,
        tenant: &str,
        predicates: &[LabelPredicate],
    ) -> BTreeSet<SeriesFingerprint> {
        let Some(series) = self.series.get(tenant) else {
            return BTreeSet::new();
        };
        let Some(candidates) = self.exact_candidates(tenant, predicates) else {
            return BTreeSet::new();
        };

        candidates
            .into_iter()
            .filter(|fingerprint| {
                series.get(fingerprint).is_some_and(|labels| {
                    predicates
                        .iter()
                        .filter(|predicate| predicate.op != MatchOp::Equal)
                        .all(|predicate| predicate.matches(labels))
                })
            })
            .collect()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> BTreeSet<String> {
        self.postings
            .keys()
            .filter(|(posting_tenant, _, _)| posting_tenant == tenant)
            .map(|(_, name, _)| name.clone())
            .collect()
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, label_name: &str) -> BTreeSet<String> {
        self.postings
            .keys()
            .filter(|(posting_tenant, name, _)| posting_tenant == tenant && name == label_name)
            .map(|(_, _, value)| value.clone())
            .collect()
    }

    #[must_use]
    pub fn labels_for(&self, tenant: &str, fingerprint: SeriesFingerprint) -> Option<&Labels> {
        self.series.get(tenant)?.get(&fingerprint)
    }

    #[must_use]
    pub fn tenant_series(&self, tenant: &str) -> Vec<(SeriesFingerprint, Labels)> {
        self.series
            .get(tenant)
            .map(|series| {
                series
                    .iter()
                    .map(|(fingerprint, labels)| (*fingerprint, labels.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn exact_candidates(
        &self,
        tenant: &str,
        predicates: &[LabelPredicate],
    ) -> Option<BTreeSet<SeriesFingerprint>> {
        let mut matched: Option<BTreeSet<SeriesFingerprint>> = None;
        for predicate in predicates {
            let Some((name, value)) = predicate.exact_posting_key() else {
                continue;
            };
            let key = (tenant.to_string(), name.to_string(), value.to_string());
            let next = self.postings.get(&key)?;
            matched = Some(match matched {
                Some(current) => current.intersection(next).copied().collect(),
                None => next.clone(),
            });
        }

        Some(matched.unwrap_or_else(|| {
            self.series
                .get(tenant)
                .map_or_else(BTreeSet::new, |series| series.keys().copied().collect())
        }))
    }
}
