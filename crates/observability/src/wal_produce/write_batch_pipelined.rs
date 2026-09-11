use super::{Future, ProduceWindow, VecDeque, WalBatchError};

/// Appends `records` with a bounded window of unacked sends.
///
/// `enqueue` hands one record to the producer and returns the future that
/// resolves when the broker acks it. The two halves must be separate: this
/// function awaits every `enqueue` in argument order, so the producer sees the
/// records in that order, and it awaits the acks out of that order.
///
/// The first failure stops the enqueue. The sends already in flight are
/// drained, so the call does not return while the producer still holds records
/// from this request, and the acks they resolve to are counted.
///
/// # Errors
/// Returns [`WalBatchError`] when a send or an ack fails. The error carries the
/// count of records the broker acked, which is below the count asked for.
pub async fn write_batch_pipelined<Record, Ack, Error, Enqueue, Enqueued>(
    records: Vec<Record>,
    window: ProduceWindow,
    enqueue: Enqueue,
) -> Result<(), WalBatchError<Error>>
where
    Enqueue: Fn(Record) -> Enqueued,
    Enqueued: Future<Output = Result<Ack, Error>>,
    Ack: Future<Output = Result<(), Error>>,
    Error: std::error::Error + 'static,
{
    let total = records.len();
    let mut records = records.into_iter();
    let mut in_flight: VecDeque<Ack> = VecDeque::with_capacity(window.get());
    let mut appended = 0_usize;
    let mut failure: Option<Error> = None;

    loop {
        while failure.is_none() && in_flight.len() < window.get() {
            let Some(record) = records.next() else { break };
            match enqueue(record).await {
                Ok(ack) => in_flight.push_back(ack),
                Err(error) => failure = Some(error),
            }
        }
        let Some(ack) = in_flight.pop_front() else {
            break;
        };
        match ack.await {
            Ok(()) => appended += 1,
            Err(error) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
    }

    failure.map_or(Ok(()), |source| {
        Err(WalBatchError::new(appended, total, source))
    })
}
