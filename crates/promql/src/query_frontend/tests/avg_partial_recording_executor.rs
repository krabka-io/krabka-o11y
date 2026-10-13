use super::*;

struct PartialValueCase {
    query: &'static str,
    shard_index: usize,
    value: f64,
}

#[derive(Default)]
pub(crate) struct AvgPartialRecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for AvgPartialRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("avg partial executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("avg partial query shard");
        let mut value = None;
        for case in [
            PartialValueCase {
                query: "sum(up)",
                shard_index: 1,
                value: 2.0,
            },
            PartialValueCase {
                query: "count(up)",
                shard_index: 2,
                value: 2.0,
            },
            PartialValueCase {
                query: "sum(up)",
                shard_index: 2,
                value: 10.0,
            },
            PartialValueCase {
                query: "count(up)",
                shard_index: 1,
                value: 1.0,
            },
        ] {
            if case.query == query.query && case.shard_index == shard.index {
                value = Some(case.value);
                break;
            }
        }
        let Some(value) = value else {
            return Err(PromqlError::Plan(format!(
                "unexpected avg partial query: {query:?}"
            )));
        };
        Ok(unlabeled_partial(query, value))
    }
}
