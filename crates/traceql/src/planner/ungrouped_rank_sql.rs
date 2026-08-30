use super::*;

pub(crate) fn ungrouped_rank_sql(spanset_sql: &str, rank: RankLimit) -> String {
    if rank.k == 0 {
        return format!("SELECT * FROM ({spanset_sql}) AS q WHERE FALSE");
    }
    format!("SELECT * FROM ({spanset_sql}) AS q")
}
