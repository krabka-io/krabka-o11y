use super::*;

/// The three `LogQL` set operators each map to their own variant, and an
/// unknown word maps to none. Deleting an arm does not fail to compile --
/// it falls to the catch-all and the operator simply stops existing.
#[test]
pub(crate) fn every_metric_set_operator_maps_to_its_own_variant() {
    use super::super::prelude::MetricBinarySetOp;

    check!(super::prelude::parse_metric_set_operator("and") == Some(MetricBinarySetOp::And));
    check!(super::prelude::parse_metric_set_operator("or") == Some(MetricBinarySetOp::Or));
    check!(super::prelude::parse_metric_set_operator("unless") == Some(MetricBinarySetOp::Unless));
    check!(super::prelude::parse_metric_set_operator("nor") == None);
    check!(super::prelude::parse_metric_set_operator("") == None);
}
