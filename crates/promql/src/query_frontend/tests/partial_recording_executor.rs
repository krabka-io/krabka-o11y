use super::*;

struct PartialValueCase {
    query: &'static str,
    shard_index: usize,
    value: f64,
}

#[derive(Default)]
pub(crate) struct PartialRecordingExecutor {
    pub(crate) calls: Mutex<Vec<FrontendRangeQuery>>,
    query_kind: &'static str,
    cases: &'static [PartialValueCase],
}

impl PartialRecordingExecutor {
    pub(crate) fn average() -> Self {
        Self {
            query_kind: "avg",
            cases: &[
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
            ],
            ..Self::default()
        }
    }

    pub(crate) fn moment() -> Self {
        Self {
            query_kind: "moment",
            cases: &[
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
            ],
            ..Self::default()
        }
    }
}

#[async_trait]
impl RangeQueryExecutor for PartialRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("partial recording executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("partial recording query shard");
        let value = self
            .cases
            .iter()
            .find(|case| case.query == query.query && case.shard_index == shard.index)
            .map(|case| case.value);
        let Some(value) = value else {
            return Err(PromqlError::Plan(format!(
                "unexpected {} partial query: {query:?}",
                self.query_kind
            )));
        };
        Ok(unlabeled_partial(query, value))
    }
}
