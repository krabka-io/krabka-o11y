/// Expands to the public save and load methods of a sharded snapshot index.
///
/// The trace and profile indexes publish through the same manifest protocol
/// and differ only in how they merge and read shards, so each `impl` names its
/// snapshot label and supplies `pending_removals`, `pending_additions`,
/// `snapshot_tenants`, `merge_tenant_shards`, `load_shard`, `finish_load` and
/// `new`; this macro writes the rest.
macro_rules! snapshot_persistence_methods {
    ($label:expr) => {
        /// Publishes this writer's contribution as the next generation.
        ///
        /// # Errors
        /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
        pub async fn save_latest_snapshot(
            &self,
            store: &::std::sync::Arc<dyn ::object_store::ObjectStore>,
            key: &str,
        ) -> $crate::error::Result<String> {
            self.save_latest_snapshot_with_retain(
                store,
                key,
                $crate::index_snapshot::IndexSnapshotRetain::default(),
            )
            .await
        }

        /// # Errors
        /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
        pub async fn save_latest_snapshot_with_retain(
            &self,
            store: &::std::sync::Arc<dyn ::object_store::ObjectStore>,
            key: &str,
            retain: $crate::index_snapshot::IndexSnapshotRetain,
        ) -> $crate::error::Result<String> {
            self.save_latest_snapshot_with_retain_and_max_bytes(
                store,
                key,
                retain,
                $crate::index_snapshot::DEFAULT_INDEX_SNAPSHOT_MAX,
            )
            .await
        }

        /// # Errors
        /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
        pub async fn save_latest_snapshot_with_retain_and_max_bytes(
            &self,
            store: &::std::sync::Arc<dyn ::object_store::ObjectStore>,
            key: &str,
            retain: $crate::index_snapshot::IndexSnapshotRetain,
            max_bytes: ::krabka_units::ByteSize,
        ) -> $crate::error::Result<String> {
            let removals = self.pending_removals.pending();
            let additions = self.pending_additions.pending();
            let snapshot_key = $crate::index_snapshot::put_manifest_snapshot(
                store,
                key,
                retain,
                max_bytes,
                $label,
                |base| async {
                    self.merged_manifest(
                        &$crate::index_snapshot::SnapshotContribution {
                            store,
                            key,
                            removals: &removals,
                            additions: &additions,
                            max_bytes,
                        },
                        base,
                    )
                    .await
                },
            )
            .await?;
            self.pending_removals.commit(&removals);
            self.pending_additions.commit(&additions);
            Ok(snapshot_key)
        }

        /// Loads the whole published index, across every tenant and every shard.
        ///
        /// This is the whole-fleet load, and it is the one a query should not be
        /// doing: see [`Self::load_latest_snapshot_for_range_with_max_bytes`].
        ///
        /// # Errors
        /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
        pub async fn load_latest_snapshot(
            store: &::std::sync::Arc<dyn ::object_store::ObjectStore>,
            key: &str,
        ) -> $crate::error::Result<Self> {
            Self::load_latest_snapshot_with_max_bytes(
                store,
                key,
                $crate::index_snapshot::DEFAULT_INDEX_SNAPSHOT_MAX,
            )
            .await
        }

        /// # Errors
        /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
        pub async fn load_latest_snapshot_with_max_bytes(
            store: &::std::sync::Arc<dyn ::object_store::ObjectStore>,
            key: &str,
            max_bytes: ::krabka_units::ByteSize,
        ) -> $crate::error::Result<Self> {
            let Some(manifest) = $crate::index_snapshot::read_latest_snapshot_manifest(
                store, key, max_bytes, $label,
            )
            .await?
            else {
                return Err($crate::error::BlockStoreError::ObjectStore(format!(
                    "no {} published under `{key}`",
                    $label,
                )));
            };
            Self::from_manifest($crate::index_snapshot::ManifestRead {
                store,
                key,
                manifest: &manifest,
                window: None,
                max_bytes,
            })
            .await
        }

        /// Loads only the shards of one tenant that meet `[min_ts, max_ts]`.
        ///
        /// A shard's span is in the manifest, so the loader decides from the
        /// manifest alone which payloads it has to fetch and never touches the
        /// rest. This is the load a query wants: what it holds is proportional to
        /// the range it asked about, not to the retention.
        ///
        /// # Errors
        /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
        pub async fn load_latest_snapshot_for_range_with_max_bytes(
            store: &::std::sync::Arc<dyn ::object_store::ObjectStore>,
            key: &str,
            tenant: &str,
            min_ts: i64,
            max_ts: i64,
            max_bytes: ::krabka_units::ByteSize,
        ) -> $crate::error::Result<Self> {
            Self::load_latest_snapshot_read($crate::index_snapshot::LatestSnapshotRead {
                store,
                key,
                window: Some($crate::index_snapshot::ShardWindow {
                    tenant,
                    span: $crate::IndexShardRange::new(min_ts, max_ts),
                }),
                max_bytes,
            })
            .await
        }

        /// Loads the newest snapshot, returning an empty index when the key has no
        /// generation yet.
        ///
        /// # Errors
        /// Returns an error when listing or reading object storage fails, or when
        /// persisted metadata is malformed.
        pub async fn load_latest_snapshot_or_empty_with_max_bytes(
            store: &::std::sync::Arc<dyn ::object_store::ObjectStore>,
            key: &str,
            max_bytes: ::krabka_units::ByteSize,
        ) -> $crate::error::Result<Self> {
            Self::load_latest_snapshot_read($crate::index_snapshot::LatestSnapshotRead {
                store,
                key,
                window: None,
                max_bytes,
            })
            .await
        }

        /// Loads the latest generation that `read` names, or an empty index
        /// when nothing is published yet.
        async fn load_latest_snapshot_read(
            read: $crate::index_snapshot::LatestSnapshotRead<'_>,
        ) -> $crate::error::Result<Self> {
            let Some(manifest) = $crate::index_snapshot::read_latest_snapshot_manifest(
                read.store,
                read.key,
                read.max_bytes,
                $label,
            )
            .await?
            else {
                return Ok(Self::new());
            };
            Self::from_manifest($crate::index_snapshot::ManifestRead {
                store: read.store,
                key: read.key,
                manifest: &manifest,
                window: read.window,
                max_bytes: read.max_bytes,
            })
            .await
        }

        /// Folds this index into the manifest `base` and returns the manifest that
        /// replaces it.
        ///
        /// The base is the state of the whole system; this writer contributes what
        /// only it knows. That is `additions`, the blocks it has registered since
        /// its last successful write, plus a replay of `removals`.
        ///
        /// Contributing every block this index names instead would be the wider
        /// bug. A writer that read a block from a snapshot and has held it in
        /// memory ever since has no removal to replay when a *concurrent*
        /// compactor retires that block, so a full union would put the compaction's
        /// input back beside its output and the querier would read both. Anything
        /// this writer has already published is in the chain the base descends
        /// from, so leaving it out loses nothing.
        ///
        /// A `base` of `None` is the exception: there is no chain, so there is no
        /// concurrent writer whose removal could be undone, and everything this
        /// index names is contributed.
        ///
        /// Each tenant's shards are merged by `merge_tenant_shards`.
        #[::tracing::instrument(
            level = "debug",
            skip_all,
            fields(key = %contribution.key, fetched = ::tracing::field::Empty, written = ::tracing::field::Empty),
            err
        )]
        async fn merged_manifest(
            &self,
            contribution: &$crate::index_snapshot::SnapshotContribution<'_>,
            base: Option<$crate::index_snapshot::SnapshotManifest>,
        ) -> $crate::error::Result<$crate::index_snapshot::SnapshotManifest> {
            let $crate::index_snapshot::SnapshotContribution {
                store,
                key,
                removals,
                additions,
                max_bytes,
            } = *contribution;
            let contribute_all = base.is_none();
            let base = base.unwrap_or_default();
            let mut fetched_shards = 0_usize;
            let mut written_shards = 0_usize;

            let mut tenants: ::std::collections::BTreeSet<&str> =
                base.tenants().map(String::as_str).collect();
            tenants.extend(self.snapshot_tenants());
            tenants.extend(removals.keys().map(String::as_str));

            let mut manifest = $crate::index_snapshot::SnapshotManifest::new();
            for tenant in tenants {
                let mut merge = $crate::index_snapshot::TenantShardMerge {
                    store,
                    key,
                    base: &base,
                    manifest: &mut manifest,
                    tenant,
                    removed: removals.get(tenant),
                    contribute_all,
                    additions,
                    max_bytes,
                    fetched: 0,
                    written: 0,
                };
                self.merge_tenant_shards(&mut merge).await?;
                fetched_shards += merge.fetched;
                written_shards += merge.written;
            }

            manifest.sort();
            ::tracing::Span::current().record("fetched", fetched_shards);
            ::tracing::Span::current().record("written", written_shards);
            Ok(manifest)
        }

        /// Reads the payloads `manifest` names and folds them into one index.
        ///
        /// `window`, when given, keeps one tenant and the shards whose span
        /// meets the range; everything else is listed in the manifest and then
        /// not read.
        #[::tracing::instrument(
            level = "debug",
            skip_all,
            fields(key = %read.key, listed = ::tracing::field::Empty, read = ::tracing::field::Empty),
            err
        )]
        async fn from_manifest(
            read: $crate::index_snapshot::ManifestRead<'_>,
        ) -> $crate::error::Result<Self> {
            let mut index = Self::new();
            let reads = read
                .read_shards(|tenant, object_key, bytes| index.load_shard(tenant, object_key, bytes))
                .await?;
            index.finish_load();
            ::tracing::Span::current().record("listed", reads.listed);
            ::tracing::Span::current().record("read", reads.read);
            Ok(index)
        }
    };
}

pub(crate) use snapshot_persistence_methods;
