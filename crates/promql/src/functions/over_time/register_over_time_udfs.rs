use super::{SessionContext, over_time_family_udfs};

/// Registers every `*_over_time` UDF on `ctx`, so a planner can lower onto them.
pub fn register_over_time_udfs(ctx: &SessionContext) {
    for udf in over_time_family_udfs() {
        ctx.register_udf(udf);
    }
}
