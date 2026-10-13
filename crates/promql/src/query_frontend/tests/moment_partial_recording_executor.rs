use super::*;

struct PartialValueCase {
    query: &'static str,
    shard_index: usize,
    value: f64,
}

#[derive(Default)]
pub(crate) struct MomentPartialRecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for MomentPartialRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("moment partial executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("moment partial query shard");
        let mut value = None;
        for case in [
            PartialValueCase {
                query: "sum(up)",
                shard_index: 1,
                value: 12.0,
            },
            PartialValueCase {
                query: "sum(up)",
                shard_index: 2,
                value: 3.0,
            },
            PartialValueCase {
                query: "count(up)",
                shard_index: 1,
                value: 2.0,
            },
            PartialValueCase {
                query: "count(up)",
                shard_index: 2,
                value: 1.0,
            },
            PartialValueCase {
                query: "sum((up) * (up))",
                shard_index: 1,
                value: 104.0,
            },
            PartialValueCase {
                query: "sum((up) * (up))",
                shard_index: 2,
                value: 9.0,
            },
        ] {
            if case.query == query.query && case.shard_index == shard.index {
                value = Some(case.value);
                break;
            }
        }
        let Some(value) = value else {
            return Err(PromqlError::Plan(format!(
                "unexpected moment partial query: {query:?}"
            )));
        };
        Ok(unlabeled_partial(query, value))
    }
}
