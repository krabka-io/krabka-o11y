use super::{DistributorState, OtlpDecodeOptions, PushError, TenantId, TranslationStrategy};
use crate::OtlpError;

pub(crate) fn decode_tenant_otlp<T>(
    state: &DistributorState,
    tenant: &TenantId,
    strategy: TranslationStrategy,
    decode: impl for<'a> FnOnce(OtlpDecodeOptions<'a>) -> Result<T, OtlpError>,
) -> Result<T, PushError> {
    let limits = state.limits_for_tenant(tenant);
    let now = state.clock.now();
    let accumulator = state.otlp_delta_accumulators.for_tenant(tenant);
    let mut guard = accumulator.lock().expect("otlp delta accumulator poisoned");
    guard.seen_at(now);
    let decoded = decode(OtlpDecodeOptions {
        strategy,
        accumulator: Some(&mut guard),
        additional_resource_attributes: &state.otlp_promote_resource_attributes,
    });
    guard.bound(limits);
    Ok(decoded?)
}
