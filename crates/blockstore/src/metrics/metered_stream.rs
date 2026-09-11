use super::{
    BoxStream, Context, Instant, ObjectStoreMetrics, ObjectStoreOperation, Pin, Poll, Stream, Time,
    TimeExt,
};

/// A stream that records its operation once, when it ends.
///
/// `list` and `delete_stream` hand back a stream rather than a future, so the
/// request is not over when the call returns. Timing the call would time the
/// construction of the stream and nothing else. This type records when the
/// stream reaches its end or yields its first error, so the duration covers
/// the whole listing and the outcome is the listing's outcome.
///
/// A stream a caller drops part way through records nothing. A partial listing
/// has no duration to report, and counting it as a success would say a
/// listing finished when it did not.
pub(super) struct MeteredStream<T> {
    inner: BoxStream<'static, Result<T, object_store::Error>>,
    metrics: ObjectStoreMetrics,
    operation: ObjectStoreOperation,
    started: Instant,
    done: bool,
}

impl<T> MeteredStream<T> {
    pub(super) fn new(
        inner: BoxStream<'static, Result<T, object_store::Error>>,
        metrics: ObjectStoreMetrics,
        operation: ObjectStoreOperation,
    ) -> Self {
        Self {
            inner,
            metrics,
            operation,
            started: Instant::now(),
            done: false,
        }
    }

    fn finish(&mut self, ok: bool) {
        if self.done {
            return;
        }
        self.done = true;
        self.metrics
            .record_operation(self.operation, ok, Time::from_std(self.started.elapsed()));
    }
}

impl<T> Stream for MeteredStream<T> {
    type Item = Result<T, object_store::Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let next = Pin::new(&mut this.inner).poll_next(context);
        match &next {
            Poll::Ready(None) => this.finish(true),
            Poll::Ready(Some(Err(_))) => this.finish(false),
            Poll::Ready(Some(Ok(_))) | Poll::Pending => {}
        }
        next
    }
}
