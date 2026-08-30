use super::*;

pub struct DebuginfodResolver {
    pub(crate) base_urls: Vec<reqwest::Url>,
    pub(crate) client: reqwest::blocking::Client,
    pub(crate) cache: Mutex<HashMap<String, Option<ObjectSymbolResolver>>>,
    pub(crate) max_debuginfo: ByteSize,
}

impl DebuginfodResolver {
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn new(base_urls: Vec<String>) -> Result<Self, String> {
        Self::with_config(base_urls, DebuginfodConfig::default())
    }

    /// Create a resolver with an explicit debuginfod resource policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the caller gives no base URL. Returns an error
    /// when a URL is invalid. Returns an error when `reqwest` cannot build the
    /// HTTP client.
    pub fn with_config(base_urls: Vec<String>, config: DebuginfodConfig) -> Result<Self, String> {
        let base_urls = base_urls
            .into_iter()
            .filter(|url| !url.trim().is_empty())
            .map(|url| reqwest::Url::parse(url.trim()).map_err(|err| err.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        if base_urls.is_empty() {
            return Err("at least one debuginfod base URL is required".to_string());
        }
        // Do not follow redirects: a redirect from a debuginfod server is a
        // vector for SSRF pivots (e.g. to internal hosts or 169.254.169.254).
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(config.connect_timeout().to_std())
            .timeout(config.request_timeout().to_std())
            .build()
            .map_err(|err| err.to_string())?;
        Ok(Self {
            base_urls,
            client,
            cache: Mutex::new(HashMap::new()),
            max_debuginfo: config.max_artifact_size(),
        })
    }

    /// Build the `<base>/buildid/<build_id>/debuginfo` URL.
    ///
    /// This function pushes the path segments through the URL parser, so an
    /// attacker-controlled `build_id` cannot alter the host or escape the path.
    /// It returns `None` when the base URL cannot be a base, for example
    /// `mailto:`.
    pub(crate) fn build_url(base: &reqwest::Url, build_id: &str) -> Option<reqwest::Url> {
        let mut url = base.clone();
        {
            let mut segments = url.path_segments_mut().ok()?;
            // Drop any trailing empty segment from a base URL ending in '/'.
            segments.pop_if_empty();
            segments.push("buildid").push(build_id).push("debuginfo");
        }
        Some(url)
    }

    pub(crate) fn resolver_for_build_id(&self, build_id: &str) -> Option<ObjectSymbolResolver> {
        let mut cache = lock_recover(&self.cache);
        if let Some(cached) = cache.get(build_id) {
            return cached.clone();
        }
        let resolver = self.fetch_build_id(build_id);
        cache.insert(build_id.to_string(), resolver.clone());
        resolver
    }

    pub(crate) fn fetch_build_id(&self, build_id: &str) -> Option<ObjectSymbolResolver> {
        // `build_id` is attacker-controlled (it comes from an uploaded
        // profile's mapping). Validate it is a plain hex build-id before it is
        // used to construct any URL or issued in any request.
        if !is_valid_build_id(build_id) {
            return None;
        }
        for base_url in &self.base_urls {
            let Some(url) = Self::build_url(base_url, build_id) else {
                continue;
            };
            let Ok(response) = self.client.get(url).send() else {
                continue;
            };
            if !response.status().is_success() {
                continue;
            }
            // Reject artifacts whose advertised length already exceeds the cap,
            // then read the body with a hard byte ceiling so a server that
            // lies about (or omits) Content-Length still cannot exhaust memory.
            let cap = self.max_debuginfo.bytes_u64();
            if !content_length_within_cap(response.content_length(), cap) {
                continue;
            }
            let Some(bytes) = read_capped(response, cap) else {
                continue;
            };
            if let Ok(resolver) = ObjectSymbolResolver::from_bytes(bytes) {
                return Some(resolver);
            }
        }
        None
    }
}

impl NativeResolver for DebuginfodResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        // Reject an attacker-controlled `build_id` up front: never cache or
        // fetch anything for a non-hex / path-traversal value.
        if !is_valid_build_id(&request.build_id) {
            return None;
        }
        self.resolver_for_build_id(&request.build_id)
            .and_then(|resolver| resolver.symbolize(request))
    }
}
