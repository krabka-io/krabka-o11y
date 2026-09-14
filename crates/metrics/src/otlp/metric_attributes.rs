use super::{KeyValue, ScopeMetrics, TranslationStrategy, scope_attributes};

pub(crate) fn metric_attributes(
    resource_attributes: &[KeyValue],
    scope_metrics: &ScopeMetrics,
    strategy: TranslationStrategy,
) -> Vec<KeyValue> {
    let mut attributes = resource_attributes.to_vec();
    attributes.extend(scope_attributes(scope_metrics, strategy));
    attributes
}
