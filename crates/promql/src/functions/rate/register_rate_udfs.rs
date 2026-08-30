use super::*;

/// Registers every rate-family UDF on `ctx` so a planner can lower onto them.
pub fn register_rate_udfs(ctx: &SessionContext) {
    for udf in rate_family_udfs() {
        ctx.register_udf(udf);
    }
}
