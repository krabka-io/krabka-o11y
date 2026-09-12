use super::{
    DEFAULT_LATENCY_BUCKETS_NS, Deserialize, HashMap, ProcessorConfig, Serialize, Time, secs,
};

#[derive(Deserialize)]
struct RuntimeOverrides {
    #[serde(default)]
    overrides: HashMap<String, TenantOverride>,
}

#[derive(Deserialize)]
struct TenantOverride {
    metrics_generator: Option<TenantMetricsGenerator>,
}

#[derive(Deserialize)]
struct TenantMetricsGenerator {
    processor: ProcessorConfig,
}

/// Metrics-generator runtime configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsGenConfig {
    #[serde(
        rename = "collection_interval_secs",
        with = "krabka_units::serde_units::numeric::secs_i64"
    )]
    pub collection_interval: Time,
    pub histogram_buckets_ns: Vec<f64>,
    pub max_exemplars_per_series: usize,
    #[serde(
        rename = "edge_ttl_secs",
        with = "krabka_units::serde_units::numeric::secs_i64"
    )]
    pub edge_ttl: Time,
    pub edge_store_max_items: usize,
    /// Ceiling on the span-metrics dimension keys one tenant may hold. Zero is
    /// unlimited.
    ///
    /// This is Tempo's `metrics_generator.max_active_series` key, with Tempo's
    /// semantics: a dimension key already in the registry is always recorded,
    /// and only a new key is refused once the registry is full. One entry here
    /// carries the three RED series and the `traces_target_info` gauge that the
    /// key produces, so the count is of keys, not of emitted series.
    ///
    /// The default diverges from Tempo, which ships `0` and so leaves the
    /// registry unbounded. A span name is the classic unbounded dimension of a
    /// trace, and one tenant that puts a SQL statement or a job id there grows
    /// this map, and the metrics store behind `remote_write`, without end.
    /// The number matches `edge_store_max_items`, so both processors of the
    /// generator hold the same cardinality budget.
    pub max_active_series: usize,
    /// Ceiling on the tenants one generator holds state for. Zero is unlimited.
    ///
    /// Tempo has no counterpart: it shards tenants over a ring. Krabka reads
    /// every tenant from one WAL topic, so the tenant map is a third unbounded
    /// map and needs its own cap.
    pub max_tenants: usize,
    pub enable_target_info: bool,
    pub enable_status_message: bool,
    pub enable_messaging_system_latency: bool,
    pub remote_write_url: String,
    pub processor: ProcessorConfig,
    /// Per-tenant processor settings, over the process defaults above.
    pub overrides: HashMap<String, ProcessorConfig>,
}

impl Default for MetricsGenConfig {
    fn default() -> Self {
        Self {
            collection_interval: secs(15),
            histogram_buckets_ns: DEFAULT_LATENCY_BUCKETS_NS.to_vec(),
            max_exemplars_per_series: 0,
            edge_ttl: secs(10),
            edge_store_max_items: 10_000,
            max_active_series: 10_000,
            max_tenants: 10_000,
            enable_target_info: false,
            enable_status_message: false,
            enable_messaging_system_latency: false,
            remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
            processor: ProcessorConfig::default(),
            overrides: HashMap::new(),
        }
    }
}

impl MetricsGenConfig {
    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> Self {
        let mut config = self.clone();
        if let Some(processor) = self.overrides.get(tenant) {
            config.processor = processor.clone();
        }
        config.overrides.clear();
        config
    }

    /// Read processor overrides from the same runtime file as trace limits.
    ///
    /// # Errors
    /// Returns an error when `yaml` is not a valid runtime overrides document.
    pub fn apply_runtime_overrides(&mut self, yaml: &str) -> Result<(), serde_yaml::Error> {
        let file = serde_yaml::from_str::<RuntimeOverrides>(yaml)?;
        self.overrides = file
            .overrides
            .into_iter()
            .filter_map(|(tenant, value)| {
                value
                    .metrics_generator
                    .map(|config| (tenant, config.processor))
            })
            .collect();
        Ok(())
    }
}
