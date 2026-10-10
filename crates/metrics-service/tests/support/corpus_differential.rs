//! Seeding the `PromQL` corpus into Krabka and an upstream engine, and running
//! every case against both.

use futures::StreamExt as _;
use serde_json::Value;

use crate::{
    promql_corpus::{self, CorpusCase, PromqlCorpus, QueryKind},
    upstream_http::{RemoteWrite, TestResult, post_remote_write},
};

/// Samples per `remote_write` request.
///
/// The whole corpus is a few hundred thousand samples, which is past what
/// either receiver will decode in one body.
pub const SAMPLES_PER_BATCH: usize = 20_000;

/// One engine's Prometheus query API: the base URL it is served on, the
/// prefix it mounts the API under, and the tenant it is queried as.
#[derive(Clone, Copy)]
pub struct PromApi<'a> {
    pub base: &'a str,
    pub prefix: &'a str,
    pub tenant: Option<&'a str>,
}

/// A `query_range` window in milliseconds.
pub struct MillisRange {
    pub start: i64,
    pub end: i64,
    pub step: i64,
}

/// One URL query parameter.
pub struct QueryParam<'a> {
    pub name: &'a str,
    pub argument: String,
}

/// Where Krabka and the upstream engine of one suite take writes and answer
/// queries.
pub struct CorpusDiff<'a> {
    /// The suite name the progress lines carry.
    pub suite: &'static str,
    pub krabka: PromApi<'a>,
    pub upstream: PromApi<'a>,
    pub upstream_write_path: &'static str,
    /// Queries in flight against one engine.
    pub query_concurrency: usize,
}

impl CorpusDiff<'_> {
    /// Writes the corpus to the upstream, then to Krabka, and waits until
    /// both answer for it.
    pub async fn seed(&self, client: &reqwest::Client, corpus: &PromqlCorpus) -> TestResult {
        let batches = promql_corpus::remote_write_batches(&corpus.series, SAMPLES_PER_BATCH);
        println!(
            "{}: seeding {} series / {} samples in {} batches",
            self.suite,
            corpus.series.len(),
            corpus.sample_count(),
            batches.len()
        );
        // The upstream first: it is the stricter receiver of the two, and a
        // corpus shape it refuses is a fault in the seed rather than in Krabka.
        for batch in &batches {
            post_remote_write(
                client,
                RemoteWrite {
                    base: self.upstream.base,
                    path: self.upstream_write_path,
                    tenant: self.upstream.tenant,
                    body: batch,
                },
            )
            .await?;
            post_remote_write(
                client,
                RemoteWrite {
                    base: self.krabka.base,
                    path: "/api/v1/write",
                    tenant: self.krabka.tenant,
                    body: batch,
                },
            )
            .await?;
        }

        let (probe, at) = corpus_probe(corpus).ok_or("the corpus seeded no float samples")?;
        self.krabka.wait_for_query_ready(client, &probe, at).await?;
        self.upstream
            .wait_for_query_ready(client, &probe, at)
            .await?;
        Ok(())
    }

    /// Runs every case against both engines and returns the disagreements.
    pub async fn run(
        &self,
        client: &reqwest::Client,
        corpus: &PromqlCorpus,
    ) -> TestResult<Vec<(String, String)>> {
        let started = std::time::Instant::now();
        let mut mismatches: Vec<(String, String)> = futures::stream::iter(corpus.cases.iter())
            .map(|case| async move {
                let krabka = self.krabka.query_case(client, case).await;
                let upstream = self.upstream.query_case(client, case).await;
                let detail = match (krabka, upstream) {
                    (Ok(krabka), Ok(upstream)) => {
                        promql_corpus::compare_case(case, &krabka, &upstream)
                    }
                    (krabka, upstream) => Some(format!(
                        "{} `{}`: transport failure\n      krabka:   {krabka:?}\n      upstream: {upstream:?}",
                        case.name, case.promql
                    )),
                };
                detail.map(|detail| (case.name.clone(), detail))
            })
            .buffer_unordered(self.query_concurrency)
            .filter_map(|mismatch| async move { mismatch })
            .collect()
            .await;
        mismatches.sort();
        println!(
            "{}: {} cases in {:.1}s, {} skipped, {} disagreed",
            self.suite,
            corpus.cases.len(),
            started.elapsed().as_secs_f64(),
            corpus.skipped.len(),
            mismatches.len()
        );
        Ok(mismatches)
    }
}

/// A selector and timestamp that must return something once the seed has
/// landed, taken from the corpus rather than assumed.
fn corpus_probe(corpus: &PromqlCorpus) -> Option<(String, i64)> {
    let series = corpus
        .series
        .iter()
        .find(|series| !series.floats.is_empty())?;
    let selector = series
        .labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quoted(value)))
        .collect::<Vec<_>>()
        .join(",");
    Some((format!("{{{selector}}}"), series.floats.first()?.0))
}

pub fn quoted(label_value: &str) -> String {
    format!(
        "\"{}\"",
        label_value.replace('\\', "\\\\").replace('"', "\\\"")
    )
}

impl PromApi<'_> {
    /// Polls an instant query until its result is non-empty, for at most 30s.
    pub async fn wait_for_query_ready(
        &self,
        client: &reqwest::Client,
        query: &str,
        at_ms: i64,
    ) -> TestResult {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            let json = self.query_instant(client, query, at_ms).await?;
            if json["data"]["result"]
                .as_array()
                .is_some_and(|result| !result.is_empty())
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        Err(format!("query `{query}` did not become non-empty on {}", self.base).into())
    }

    pub async fn query_case(
        &self,
        client: &reqwest::Client,
        case: &CorpusCase,
    ) -> TestResult<Value> {
        match case.kind {
            QueryKind::Instant { time } => self.query_instant(client, &case.promql, time).await,
            QueryKind::Range { start, end, step } => {
                self.query_range(client, &case.promql, MillisRange { start, end, step })
                    .await
            }
        }
    }

    pub async fn query_instant(
        &self,
        client: &reqwest::Client,
        promql: &str,
        time_ms: i64,
    ) -> TestResult<Value> {
        self.get_json(
            client,
            "/api/v1/query",
            &[
                QueryParam {
                    name: "query",
                    argument: promql.to_string(),
                },
                QueryParam {
                    name: "time",
                    argument: seconds_param(time_ms),
                },
            ],
        )
        .await
    }

    async fn query_range(
        &self,
        client: &reqwest::Client,
        promql: &str,
        range: MillisRange,
    ) -> TestResult<Value> {
        self.get_json(
            client,
            "/api/v1/query_range",
            &[
                QueryParam {
                    name: "query",
                    argument: promql.to_string(),
                },
                QueryParam {
                    name: "start",
                    argument: seconds_param(range.start),
                },
                QueryParam {
                    name: "end",
                    argument: seconds_param(range.end),
                },
                QueryParam {
                    name: "step",
                    argument: seconds_param(range.step),
                },
            ],
        )
        .await
    }

    async fn get_json(
        &self,
        client: &reqwest::Client,
        endpoint: &str,
        query: &[QueryParam<'_>],
    ) -> TestResult<Value> {
        let mut request = client.get(query_url(
            self.base,
            &format!("{}{endpoint}", self.prefix),
            query,
        ));
        if let Some(tenant) = self.tenant {
            request = request.header("X-Scope-OrgID", tenant);
        }
        json_body(request).await
    }
}

async fn json_body(request: reqwest::RequestBuilder) -> TestResult<Value> {
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    serde_json::from_str(&text)
        .map_err(|error| format!("{status} response was not JSON: {error}: {text}").into())
}

pub fn query_url(base: &str, path: &str, query: &[QueryParam<'_>]) -> String {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(
            query
                .iter()
                .map(|param| (param.name, param.argument.as_str())),
        )
        .finish();
    format!("{base}{path}?{query}")
}

pub fn seconds_param(ms: i64) -> String {
    let sign = if ms < 0 { "-" } else { "" };
    let abs_ms = i128::from(ms).abs();
    format!("{sign}{}.{:03}", abs_ms / 1000, abs_ms % 1000)
}
