use std::future::Future;

tokio::task_local! {
    static HISTOGRAM_STATS: ();
}

pub(crate) async fn with_histogram_stats<T>(future: impl Future<Output = T>) -> T {
    HISTOGRAM_STATS.scope((), future).await
}

pub(crate) fn histogram_stats_enabled() -> bool {
    HISTOGRAM_STATS.try_with(|()| ()).is_ok()
}
