use super::{SessionContext, prom_max_udaf, prom_min_udaf};

/// Registers `prom_min`/`prom_max` on `ctx` so the aggregation planner can lower
/// `min`/`max` onto NaN-ignoring UDAFs that match the interpreter.
pub fn register_aggregate_udafs(ctx: &SessionContext) {
    ctx.register_udaf(prom_min_udaf());
    ctx.register_udaf(prom_max_udaf());
}
