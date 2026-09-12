use krabka_blockstore::ProfileIndex;
use krabka_units::{ByteSize, Time, convert::TimeExt as _};
use object_store::ObjectStore;

use super::{AllStage, Arc, DebuginfodConfig};

pub(crate) fn symbolizer_stage(
    urls: Vec<String>,
    config: DebuginfodConfig,
    store: Arc<dyn ObjectStore>,
    index_key: String,
    index_max: ByteSize,
    interval: Time,
) -> AllStage {
    Box::new(move |token| {
        Box::pin(async move {
            let Ok(resolver) =
                krabka_profiles::symbolizer::offline_resolver_from_debuginfod_config(urls, config)
            else {
                tracing::error!("profiles symbolizer could not build its resolver");
                return;
            };
            loop {
                match ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &store, &index_key, index_max,
                )
                .await
                {
                    Ok(index) => match krabka_profiles::symbolizer::symbolize_blocks_once(
                        &store, &index, &resolver,
                    )
                    .await
                    {
                        Ok(updated) => {
                            tracing::info!(updated, "offline symbolization pass complete");
                        }
                        Err(error) => tracing::warn!(%error, "offline symbolization pass failed"),
                    },
                    Err(error) => tracing::warn!(%error, "profile index refresh failed"),
                }
                tokio::select! {
                    () = token.cancelled() => return,
                    () = tokio::time::sleep(interval.to_std()) => {}
                }
            }
        })
    })
}
