use futures::StreamExt as _;
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};

use super::{MetricStore, PrometheusApiState, TenantId};

const RULER_PREFIX: &str = "mimir-configs/ruler";
const ALERTMANAGER_PREFIX: &str = "mimir-configs/alertmanager";
const ALERTS_PREFIX: &str = "mimir-configs/alertmanager-alerts";
const SILENCES_PREFIX: &str = "mimir-configs/alertmanager-silences";

impl<S: MetricStore> PrometheusApiState<S> {
    /// Reloads every persisted tenant configuration before the role starts evaluating.
    ///
    /// # Errors
    ///
    /// Returns an error when persisted objects cannot be listed, read, or decoded.
    pub async fn reload_mimir_configs(&self) -> Result<(), String> {
        let Some(store) = &self.mimir_config_store else {
            return Ok(());
        };
        let mut loaded_rules = Vec::new();
        let mut loaded_alertmanager = Vec::new();
        let mut loaded_alerts = Vec::new();
        let mut loaded_silences = Vec::new();
        for (prefix, is_ruler) in [(RULER_PREFIX, true), (ALERTMANAGER_PREFIX, false)] {
            let mut objects = store.list(Some(&Path::from(prefix)));
            while let Some(object) = objects.next().await {
                let object = object.map_err(|error| error.to_string())?;
                let Some(name) = object.location.filename() else {
                    continue;
                };
                let Some(tenant_name) = name.strip_suffix(".yaml") else {
                    continue;
                };
                let tenant = TenantId::new(tenant_name).map_err(|error| error.to_string())?;
                let bytes = store
                    .get(&object.location)
                    .await
                    .map_err(|error| error.to_string())?
                    .bytes()
                    .await
                    .map_err(|error| error.to_string())?;
                if is_ruler {
                    let namespaces =
                        serde_yaml::from_slice(&bytes).map_err(|error| error.to_string())?;
                    loaded_rules.push((tenant, namespaces));
                } else {
                    let config =
                        String::from_utf8(bytes.to_vec()).map_err(|error| error.to_string())?;
                    loaded_alertmanager.push((tenant, config));
                }
            }
        }
        for (prefix, silences) in [(ALERTS_PREFIX, false), (SILENCES_PREFIX, true)] {
            let mut objects = store.list(Some(&Path::from(prefix)));
            while let Some(object) = objects.next().await {
                let object = object.map_err(|error| error.to_string())?;
                let Some(name) = object.location.filename() else {
                    continue;
                };
                let Some(tenant_name) = name.strip_suffix(".json") else {
                    continue;
                };
                let tenant = TenantId::new(tenant_name).map_err(|error| error.to_string())?;
                let bytes = store
                    .get(&object.location)
                    .await
                    .map_err(|error| error.to_string())?
                    .bytes()
                    .await
                    .map_err(|error| error.to_string())?;
                if silences {
                    loaded_silences.push((
                        tenant,
                        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?,
                    ));
                } else {
                    loaded_alerts.push((
                        tenant,
                        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?,
                    ));
                }
            }
        }
        self.ruler_rules
            .write()
            .map_err(|_| "ruler rules lock poisoned")?
            .extend(loaded_rules);
        self.alertmanager_configs
            .write()
            .map_err(|_| "alertmanager configs lock poisoned")?
            .extend(loaded_alertmanager);
        self.alertmanager_alerts
            .write()
            .map_err(|_| "alertmanager alerts lock poisoned")?
            .extend(loaded_alerts);
        self.alertmanager_silences
            .write()
            .map_err(|_| "alertmanager silences lock poisoned")?
            .extend(loaded_silences);
        Ok(())
    }

    pub(crate) async fn persist_ruler_config(&self, tenant: &TenantId) -> Result<(), String> {
        let Some(store) = &self.mimir_config_store else {
            return Ok(());
        };
        let namespaces = self
            .ruler_rules
            .read()
            .map_err(|_| "ruler rules lock poisoned")?
            .get(tenant)
            .cloned();
        persist_optional(
            store.as_ref(),
            &ruler_path(tenant),
            namespaces
                .as_ref()
                .map(serde_yaml::to_string)
                .transpose()
                .map_err(|error| error.to_string())?,
        )
        .await
    }

    pub(crate) async fn persist_alertmanager_config(
        &self,
        tenant: &TenantId,
    ) -> Result<(), String> {
        let Some(store) = &self.mimir_config_store else {
            return Ok(());
        };
        let config = self
            .alertmanager_configs
            .read()
            .map_err(|_| "alertmanager configs lock poisoned")?
            .get(tenant)
            .cloned();
        persist_optional(store.as_ref(), &alertmanager_path(tenant), config).await
    }

    pub(crate) async fn persist_alertmanager_alerts(
        &self,
        tenant: &TenantId,
    ) -> Result<(), String> {
        let Some(store) = &self.mimir_config_store else {
            return Ok(());
        };
        let alerts = self
            .alertmanager_alerts
            .read()
            .map_err(|_| "alertmanager alerts lock poisoned")?
            .get(tenant)
            .cloned();
        persist_json(
            store.as_ref(),
            &json_path(ALERTS_PREFIX, tenant),
            alerts.as_ref(),
        )
        .await
    }

    pub(crate) async fn persist_alertmanager_silences(
        &self,
        tenant: &TenantId,
    ) -> Result<(), String> {
        let Some(store) = &self.mimir_config_store else {
            return Ok(());
        };
        let silences = self
            .alertmanager_silences
            .read()
            .map_err(|_| "alertmanager silences lock poisoned")?
            .get(tenant)
            .cloned();
        persist_json(
            store.as_ref(),
            &json_path(SILENCES_PREFIX, tenant),
            silences.as_ref(),
        )
        .await
    }
}

async fn persist_json<T: serde::Serialize>(
    store: &dyn ObjectStore,
    path: &Path,
    value: Option<&T>,
) -> Result<(), String> {
    let value = value
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| error.to_string())?;
    persist_optional(store, path, value).await
}

async fn persist_optional(
    store: &dyn ObjectStore,
    path: &Path,
    value: Option<String>,
) -> Result<(), String> {
    match value {
        Some(value) => store
            .put(path, value.into())
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        None => match store.delete(path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.to_string()),
        },
    }
}

fn ruler_path(tenant: &TenantId) -> Path {
    Path::from(format!("{RULER_PREFIX}/{}.yaml", tenant.as_str()))
}
fn alertmanager_path(tenant: &TenantId) -> Path {
    Path::from(format!("{ALERTMANAGER_PREFIX}/{}.yaml", tenant.as_str()))
}
fn json_path(prefix: &str, tenant: &TenantId) -> Path {
    Path::from(format!("{prefix}/{}.json", tenant.as_str()))
}
