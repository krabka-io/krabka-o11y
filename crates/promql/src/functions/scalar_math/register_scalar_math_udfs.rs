use super::*;

/// Registers every scalar-math UDF on `ctx` so a planner can lower onto them.
pub fn register_scalar_math_udfs(ctx: &SessionContext) {
    for udf in scalar_math_udfs() {
        ctx.register_udf(udf);
    }
}
