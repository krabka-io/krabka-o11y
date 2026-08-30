use super::*;

/// Validated resource policy for debuginfod requests.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DebuginfodConfig {
    pub(crate) max_artifact_size: ByteSize,
    pub(crate) connect_timeout: Time,
    pub(crate) request_timeout: Time,
}

impl DebuginfodConfig {
    /// Validate a debuginfod resource policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the maximum artifact size is not a positive
    /// whole-byte value. Returns an error when either timeout is not positive
    /// and finite. Returns an error when the connect timeout is more than the
    /// whole-request timeout.
    pub fn new(
        max_artifact_size: ByteSize,
        connect_timeout: Time,
        request_timeout: Time,
    ) -> Result<Self, String> {
        let bytes = max_artifact_size.bytes_f64();
        if !bytes.is_finite() || bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
            return Err(
                "debuginfod maximum artifact size must be a positive whole-byte value exactly representable by UOM"
                    .to_string(),
            );
        }
        GreaterU64::<0>::new(max_artifact_size.bytes_u64())
            .map(Refined::into_value)
            .map_err(|error| format!("debuginfod maximum artifact size: {error}"))?;

        validate_positive_timeout("connect", connect_timeout)?;
        validate_positive_timeout("request", request_timeout)?;
        if connect_timeout > request_timeout {
            return Err("debuginfod connect timeout must not exceed request timeout".to_string());
        }

        Ok(Self {
            max_artifact_size,
            connect_timeout,
            request_timeout,
        })
    }

    /// Return the maximum downloaded artifact size.
    #[must_use]
    pub const fn max_artifact_size(self) -> ByteSize {
        self.max_artifact_size
    }

    /// Return the connection timeout.
    #[must_use]
    pub const fn connect_timeout(self) -> Time {
        self.connect_timeout
    }

    /// Return the whole-request timeout.
    #[must_use]
    pub const fn request_timeout(self) -> Time {
        self.request_timeout
    }
}

impl Default for DebuginfodConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_DEBUGINFOD_MAX_ARTIFACT_SIZE,
            DEFAULT_DEBUGINFOD_CONNECT_TIMEOUT,
            DEFAULT_DEBUGINFOD_REQUEST_TIMEOUT,
        )
        .expect("default debuginfod configuration is valid")
    }
}
