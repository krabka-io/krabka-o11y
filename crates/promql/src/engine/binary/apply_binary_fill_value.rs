use super::*;

pub(crate) fn apply_binary_fill_value(
    present: &InstantSample,
    fill_value: f64,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    missing_side: MissingSide,
) -> Result<Option<SampleValue>> {
    let filled = InstantSample {
        labels: Labels::new(),
        ts_ms: present.ts_ms,
        value: SampleValue::Float(fill_value),
    };
    match missing_side {
        MissingSide::Left => apply_binary_sample_value(&filled, present, op, modifier),
        MissingSide::Right => apply_binary_sample_value(present, &filled, op, modifier),
    }
}
