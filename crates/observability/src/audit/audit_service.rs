use std::{pin::pin, sync::Arc};

use krabka_audit::{
    AuditLog, AuditStats, AuditWriter, AuditWriterParams, ChainState, FileEd25519Signer, Spool,
};
use krabka_units::prelude::{Time, secs};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{
    AuditArgs, AuditBuildError, AuditClocks, AuditHandle, AuditSink, KafkaTopicAuditSink,
    ProductInfo,
};

/// The count of records after which the writer signs a checkpoint, when a
/// signing key is set: the broker's default.
///
/// The writer also signs a checkpoint when `--audit-checkpoint-every` passes,
/// and when it stops.
pub const AUDIT_CHECKPOINT_EVERY_RECORDS: u64 = 1000;

/// How often the writer tries to write the spool to the topic while the spool
/// holds records: the broker's default.
pub const AUDIT_SPOOL_REPLAY_EVERY: Time = secs(2);

/// The audit layer of one service process: the handle that request handlers
/// use, and the writer task behind it.
///
/// The handle is cheap to clone. The writer's [`JoinHandle`] is not, so
/// [`into_parts`](Self::into_parts) gives the two apart. Give the handle to the
/// request state, and give the join handle to the role's supervisor, for
/// example `SupervisedTasks::adopt("audit writer", writer)`.
///
/// The writer runs until the shutdown token is cancelled. It then stops the
/// queue, writes the events already in the queue, signs a last checkpoint when
/// a key is set, and returns. A writer that stops before the token is
/// cancelled has panicked. Cancel the token after the listeners stop, so that
/// no request emits an event after the queue stops.
#[derive(Debug)]
pub struct AuditService {
    handle: AuditHandle,
    writer: Option<JoinHandle<()>>,
}

impl AuditService {
    /// An audit layer that records nothing and has no writer task.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            handle: AuditHandle::disabled(),
            writer: None,
        }
    }

    /// Starts the audit layer that `args` configures.
    ///
    /// When `--audit-topic` is unset, this returns [`AuditService::disabled`]
    /// and does not connect to a broker. Otherwise it loads the signing key,
    /// opens the spool, starts a producer, and spawns the writer.
    /// [`AuditArgs::resolve_bootstrap`] gives the producer's bootstrap servers
    /// from `service_bootstrap`. `security` is the broker connection policy that
    /// `WalClientSecurityArgs::load` gives, and `None` connects in plain text.
    ///
    /// # Errors
    ///
    /// Returns [`AuditBuildError`] when no bootstrap server is set, when the
    /// signing key does not load, when the spool does not open, or when the
    /// producer does not start.
    ///
    /// # Panics
    ///
    /// Panics when it runs outside a Tokio runtime.
    pub async fn start(
        args: &AuditArgs,
        product: ProductInfo,
        service_bootstrap: Option<&str>,
        security: Option<&krabka_client_core::ClientSecurity>,
        shutdown: CancellationToken,
    ) -> Result<Self, AuditBuildError> {
        let Some(topic) = args.topic.as_deref() else {
            return Ok(Self::disabled());
        };
        let bootstrap = args.resolve_bootstrap(service_bootstrap)?;
        // The key and the spool load before the producer connects, so an
        // operator error stops the service without a broker round trip.
        let state = WriterState::load(args)?;
        let sink = KafkaTopicAuditSink::connect(
            bootstrap,
            format!("{}-audit", product.name),
            topic,
            args.partition,
            security,
        )
        .await
        .map_err(|source| AuditBuildError::Producer {
            bootstrap: bootstrap.to_owned(),
            source,
        })?;
        Ok(state.spawn(
            args,
            product,
            Arc::new(sink),
            AuditClocks::system(),
            shutdown,
        ))
    }

    /// Starts an enabled audit layer that writes to `sink`.
    ///
    /// This ignores `--audit-topic`, `--audit-bootstrap` and
    /// `--audit-partition`, because the caller gives the sink. It reads the
    /// spool, queue, checkpoint and signing-key flags. A test gives a
    /// `krabka_audit::MemorySink` and mock clocks.
    ///
    /// # Errors
    ///
    /// Returns [`AuditBuildError`] when the signing key does not load, or when
    /// the spool does not open.
    ///
    /// # Panics
    ///
    /// Panics when it runs outside a Tokio runtime.
    pub fn start_with_sink(
        args: &AuditArgs,
        product: ProductInfo,
        sink: Arc<dyn AuditSink>,
        clocks: AuditClocks,
        shutdown: CancellationToken,
    ) -> Result<Self, AuditBuildError> {
        Ok(WriterState::load(args)?.spawn(args, product, sink, clocks, shutdown))
    }

    /// The handle that request handlers record events through.
    #[must_use]
    pub fn handle(&self) -> &AuditHandle {
        &self.handle
    }

    /// The handle, and the writer task when audit is on.
    #[must_use]
    pub fn into_parts(self) -> (AuditHandle, Option<JoinHandle<()>>) {
        (self.handle, self.writer)
    }
}

/// What the writer needs from disk before it starts.
struct WriterState {
    signer: Option<Arc<FileEd25519Signer>>,
    spool: Option<Spool>,
    chain: ChainState,
}

impl WriterState {
    /// Loads the signing key and opens the spool that `args` name.
    ///
    /// A spool that survived a restart ends at the newest chained record, so
    /// the chain goes on from that record and not from the genesis head.
    fn load(args: &AuditArgs) -> Result<Self, AuditBuildError> {
        let signer = match (&args.signing_key_path, &args.signing_key_id) {
            (None, None) => None,
            (Some(path), Some(key_id)) => Some(Arc::new(
                FileEd25519Signer::from_pkcs8_file(path, key_id.clone()).map_err(|source| {
                    AuditBuildError::SigningKey {
                        path: path.clone(),
                        source,
                    }
                })?,
            )),
            _ => return Err(AuditBuildError::IncompleteSigningKey),
        };
        let Some(dir) = args.spool_dir.as_deref() else {
            return Ok(Self {
                signer,
                spool: None,
                chain: ChainState::new(),
            });
        };
        let spool_error = |source| AuditBuildError::Spool {
            dir: dir.to_path_buf(),
            source,
        };
        let spool = Spool::open(dir, args.spool_max).map_err(spool_error)?;
        let chain = spool
            .resume_point()
            .map_err(spool_error)?
            .map_or_else(ChainState::new, |(next_seq, head)| {
                ChainState::resume(next_seq, head)
            });
        Ok(Self {
            signer,
            spool: Some(spool),
            chain,
        })
    }

    /// Spawns the writer, and stops its queue when `shutdown` is cancelled.
    fn spawn(
        self,
        args: &AuditArgs,
        product: ProductInfo,
        sink: Arc<dyn AuditSink>,
        clocks: AuditClocks,
        shutdown: CancellationToken,
    ) -> AuditService {
        let (log, receiver) = AuditLog::new(args.queue_capacity.get());
        let stats = Arc::new(AuditStats::new());
        let handle = AuditHandle::new(log, Arc::clone(&stats), clocks.clock);
        let writer = AuditWriter::new(
            receiver,
            AuditWriterParams {
                sink,
                product,
                signer: self.signer,
                checkpoint_every_n: AUDIT_CHECKPOINT_EVERY_RECORDS,
                checkpoint_every: args.checkpoint_every,
                chain: self.chain,
                spool: self.spool,
                stats,
                replay_every: AUDIT_SPOOL_REPLAY_EVERY,
                sleeper: clocks.sleeper,
            },
        );
        let closer = handle.clone();
        let writer = tokio::spawn(async move {
            let mut run = pin!(writer.run());
            tokio::select! {
                () = &mut run => {}
                () = shutdown.cancelled() => {
                    closer.close();
                    run.await;
                }
            }
        });
        AuditService {
            handle,
            writer: Some(writer),
        }
    }
}
