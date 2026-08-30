use super::*;

pub(crate) async fn build_querier_state_with_object_store_prefix(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<QuerierState, ServiceConfigError> {
    let state = match config.querier_index_source {
        QuerierIndexSource::LocalManifest => QuerierState::from_manifest(config.data_root.clone())?,
        QuerierIndexSource::TenantObjectStoreManifest => {
            let (store, tenant, prefix) =
                querier_object_store_inputs(config, object_store, object_store_prefix)?;
            QuerierState::from_tenant_object_store(config.data_root.clone(), store, &prefix, tenant)
                .await?
        }
        QuerierIndexSource::TenantObjectStoreShards => {
            let (store, tenant, prefix) =
                querier_object_store_inputs(config, object_store, object_store_prefix)?;
            let start_ns = config
                .query_start_ns
                .ok_or(ServiceConfigError::MissingQueryStartNs)?;
            let end_ns = config
                .query_end_ns
                .ok_or(ServiceConfigError::MissingQueryEndNs)?;

            QuerierState::from_tenant_object_store_shards(
                config.data_root.clone(),
                store,
                &prefix,
                tenant,
                TimeRange::new(start_ns, end_ns)?,
            )
            .await?
        }
    }
    .with_runtime_policy(config);

    let state = if let Some(max_query_range) = config.max_query_range {
        state.with_max_query_range(max_query_range)
    } else {
        state
    };

    let state = if let Some(max_query_series) = config.max_query_series {
        state.with_max_query_series(max_query_series)
    } else {
        state
    };

    let state = if let Some(max_query_read) = config.max_query_read {
        state.with_max_query_read(max_query_read)
    } else {
        state
    };

    Ok(if let Some(max_query_length) = config.max_query_length {
        state.with_max_query_length(max_query_length)
    } else {
        state
    })
}
