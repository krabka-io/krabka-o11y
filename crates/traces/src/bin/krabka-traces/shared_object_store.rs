use krabka_blockstore::{
    ConditionalUpdateRequirement, ObjectStoreAccess, ObjectStoreMetrics, verify_object_store_access,
};

use super::{Arc, Cli, ConfiguredObjectStore, build_object_store};

/// The one object store a process configures, however many roles ask for it.
///
/// Four of this binary's roles read or write blocks -- the block builder, the
/// querier, the query-frontend's catalog, and the compactor -- and each of
/// them used to call [`build_object_store`] for itself. For a single-role
/// process that is one call either way. For `--target all` it is four
/// independent stores, and with the default `--object-store-url memory:///`
/// that means four independent *heaps*: the block builder would write blocks
/// into one `InMemory` and the querier would search another that nothing ever
/// wrote to, so the process would start, pass every probe, and answer every
/// query with nothing. That failure has no symptom anywhere -- no error, no
/// warning, not even a missing object -- which is why the sharing is a type
/// rather than a convention.
///
/// The store is built on first use rather than up front. A distributor's or a
/// metrics-generator's startup does not touch object storage, and building an
/// S3 client for them would make a bad `--object-store-url`, or a credential
/// that role never needs, into a reason it will not start. Building it lazily
/// also keeps each role's readiness gates honest: the role still reaches its
/// store at the point in its own startup where it always did, and marks
/// `object-store` ready there, and only the second and later roles find the
/// work already done.
///
/// The [`ObjectStoreMetrics`] handle of whichever role asks first is the one
/// the decorator keeps. Every role in a process shares one registry, so this
/// changes no counter's value; it only means the request counts of all four
/// roles land on one set of series, which is what a single-process stack has
/// to report anyway.
#[derive(Clone)]
pub(crate) struct SharedObjectStore {
    cell: Arc<tokio::sync::OnceCell<ConfiguredObjectStore>>,
    read_verified: Arc<tokio::sync::OnceCell<()>>,
    read_write_verified: Arc<tokio::sync::OnceCell<()>>,
}

impl SharedObjectStore {
    /// A handle that has not built its store yet.
    pub(crate) fn new() -> Self {
        Self {
            cell: Arc::new(tokio::sync::OnceCell::new()),
            read_verified: Arc::new(tokio::sync::OnceCell::new()),
            read_write_verified: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// The process's object store, building it on the first call.
    ///
    /// The store is probed once for each `access` below the configured
    /// prefix, before the role that asked accepts any data. The block builder
    /// and the compactor ask for [`ObjectStoreAccess::ReadWrite`] and get
    /// [`verify_object_store_access`]'s full probe. The querier and the
    /// query-frontend ask for [`ObjectStoreAccess::ReadOnly`], which writes
    /// nothing, so they can run with a read-only credential. A process whose
    /// store already passed the full probe does not run the read probe.
    ///
    /// # Errors
    /// Returns whatever [`build_object_store`] returns: a `--object-store-url`
    /// that does not parse, or a backend that rejects the configuration it was
    /// given. Returns the probe's error when the store fails it. A failed build
    /// or probe is not remembered, so the next role to ask tries again rather
    /// than inheriting a poisoned cell.
    pub(crate) async fn get(
        &self,
        cli: &Cli,
        metrics: ObjectStoreMetrics,
        access: ObjectStoreAccess,
    ) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
        let configured = self
            .cell
            .get_or_try_init(|| async { build_object_store(cli, metrics) })
            .await?;
        let verified = match access {
            ObjectStoreAccess::ReadWrite => &self.read_write_verified,
            ObjectStoreAccess::ReadOnly if self.read_write_verified.initialized() => {
                return Ok(configured.clone());
            }
            ObjectStoreAccess::ReadOnly => &self.read_verified,
        };
        verified
            .get_or_try_init(|| {
                verify_object_store_access(
                    configured.store.as_ref(),
                    &configured.prefix,
                    access,
                    ConditionalUpdateRequirement::for_object_store_url(&cli.object_store_url),
                )
            })
            .await?;
        Ok(configured.clone())
    }
}
