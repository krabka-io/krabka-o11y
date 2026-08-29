use super::*;

pub(crate) fn parse_decimal_seconds_timestamp(value: &str) -> Option<i64> {
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let (seconds, fraction) = unsigned.split_once('.')?;
    if seconds.is_empty() && fraction.is_empty() {
        return None;
    }
    if !seconds.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let seconds = if seconds.is_empty() {
        0
    } else {
        seconds.parse::<i128>().ok()?
    };
    let mut fraction_ns = 0_i128;
    let mut scale = 100_000_000_i128;
    for digit in fraction.bytes().take(9) {
        fraction_ns += i128::from(digit - b'0') * scale;
        scale /= 10;
    }

    let timestamp_ns = seconds
        .checked_mul(1_000_000_000)?
        .checked_add(fraction_ns)?;
    let timestamp_ns = if negative {
        timestamp_ns.checked_neg()?
    } else {
        timestamp_ns
    };
    i64::try_from(timestamp_ns).ok()
}

pub(crate) fn parse_usize_query_param(
    name: &'static str,
    value: &str,
) -> Result<usize, HttpQueryError> {
    if name == "limit" {
        let limit = value
            .parse::<i64>()
            .map_err(|_| HttpQueryError::InvalidLimit(value.to_string()))?;
        if limit <= 0 {
            return Err(HttpQueryError::LimitNotPositive);
        }
        return usize::try_from(limit).map_err(|_| HttpQueryError::InvalidLimit(value.to_string()));
    }

    value
        .parse()
        .map_err(|_| HttpQueryError::InvalidQueryParameter {
            name,
            value: value.to_string(),
        })
}

pub(crate) fn decode_form_component(component: &str) -> Result<String, HttpQueryError> {
    let mut bytes = Vec::with_capacity(component.len());
    let mut iter = component.as_bytes().iter().copied();
    while let Some(byte) = iter.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = iter
                    .next()
                    .and_then(hex_value)
                    .ok_or(HttpQueryError::InvalidPercentEncoding)?;
                let low = iter
                    .next()
                    .and_then(hex_value)
                    .ok_or(HttpQueryError::InvalidPercentEncoding)?;
                bytes.push(high << 4 | low);
            }
            _ => bytes.push(byte),
        }
    }

    String::from_utf8(bytes).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}

pub(crate) fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn grpc_tenant(metadata: &tonic::metadata::MetadataMap) -> Result<&str, tonic::Status> {
    metadata
        .get("x-scope-orgid")
        .ok_or_else(|| tonic::Status::invalid_argument("missing tenant header"))?
        .to_str()
        .map_err(|_| tonic::Status::invalid_argument("invalid tenant header"))
        .and_then(|tenant| {
            if tenant.is_empty() {
                Err(tonic::Status::invalid_argument("invalid tenant header"))
            } else {
                Ok(tenant)
            }
        })
}

pub(crate) async fn authorized_tenant<'a>(
    state: &QuerierState,
    headers: &'a HeaderMap,
) -> Result<&'a str, HttpQueryError> {
    let tenant = tenant(headers)?;
    state.query_authorizer.check(tenant).await?;
    Ok(tenant)
}

pub(crate) async fn authorized_tenants(
    state: &QuerierState,
    headers: &HeaderMap,
) -> Result<Vec<String>, HttpQueryError> {
    let header = tenant(headers)?;
    let tenants = header
        .split('|')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    if tenants.iter().any(String::is_empty) {
        return Err(HttpQueryError::InvalidTenant);
    }
    for tenant in &tenants {
        state.query_authorizer.check(tenant).await?;
    }
    Ok(tenants)
}

pub(crate) fn tenant(headers: &HeaderMap) -> Result<&str, HttpQueryError> {
    headers
        .get("X-Scope-OrgID")
        .ok_or(HttpQueryError::MissingTenant)?
        .to_str()
        .map_err(|_| HttpQueryError::InvalidTenant)
        .and_then(|tenant| {
            if tenant.is_empty() {
                Err(HttpQueryError::InvalidTenant)
            } else {
                Ok(tenant)
            }
        })
}

pub(crate) fn time_range(
    params: &QueryParams,
    kind: QueryKind,
) -> Result<TimeRange, HttpQueryError> {
    match kind {
        QueryKind::Instant => {
            if let Some(time) = params.time {
                TimeRange::new(time, time).map_err(HttpQueryError::from)
            } else {
                optional_start_end_range(params.start, params.since, params.end)
            }
        }
        QueryKind::Range => {
            let end = params.end.unwrap_or_else(current_unix_time_ns);
            let start = start_or_since(params.start, params.since, Some(end))?
                .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE.nanos_i64()));
            TimeRange::new(start, end).map_err(HttpQueryError::from)
        }
    }
}

/// Window a query covers when it names neither `start` nor `since`.
pub(crate) const LOKI_DEFAULT_QUERY_RANGE: Time = hours(1);
/// Window the metadata endpoints index over when the request names no range.
pub(crate) const LOKI_METADATA_DEFAULT_INDEX_RANGE: Time = hours(6);
pub(crate) const LOKI_DEFAULT_TAIL_LIMIT: usize = 100;
pub(crate) const LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS: i64 = 11_000;
/// Longest `delay_for` a tail request may ask the querier to hold back.
pub(crate) const LOKI_MAX_TAIL_DELAY: Time = secs(5);
/// Widest window `/loki/api/v1/index/volume` and the range endpoints accept
/// (`Loki`'s 30d 1h default, to the nanosecond).
pub(crate) const LOKI_VOLUME_MAX_QUERY_RANGE: Time = secs(2_595_600);

pub(crate) fn current_unix_time_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        })
}

pub(crate) fn optional_start_end_range(
    start: Option<i64>,
    since: Option<i64>,
    end: Option<i64>,
) -> Result<TimeRange, HttpQueryError> {
    let start = start_or_since(start, since, end)?.unwrap_or(i64::MIN);
    TimeRange::new(start, end.unwrap_or(i64::MAX)).map_err(HttpQueryError::from)
}

pub(crate) fn start_or_since(
    start: Option<i64>,
    since: Option<i64>,
    end: Option<i64>,
) -> Result<Option<i64>, HttpQueryError> {
    if start.is_some() {
        return Ok(start);
    }
    let Some(since) = since else {
        return Ok(None);
    };
    if since <= 0 {
        return Err(HttpQueryError::InvalidSinceQueryParameter {
            value: since.to_string(),
        });
    }
    let Some(end) = end else {
        return Ok(None);
    };
    end.checked_sub(since)
        .map(Some)
        .ok_or_else(|| HttpQueryError::InvalidSinceQueryParameter {
            value: since.to_string(),
        })
}

#[derive(Clone, Copy)]
pub(crate) enum QueryKind {
    Instant,
    Range,
}

#[derive(Clone, Copy)]
pub(crate) enum LokiDirection {
    Forward,
    Backward,
}

pub(crate) fn loki_direction(direction: Option<&str>) -> Result<LokiDirection, HttpQueryError> {
    match direction {
        None | Some("backward") => Ok(LokiDirection::Backward),
        Some("forward") => Ok(LokiDirection::Forward),
        Some(value) => Err(HttpQueryError::InvalidDirection(value.to_string())),
    }
}
