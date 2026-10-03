use super::Url;

/// Refuses two stores of one command that are the same store, or where one
/// holds the other.
///
/// Each item is a label for the message and a store URL. The check compares
/// the scheme, the host and port, and the path segments of each URL, so it
/// runs before any store is opened. Two URLs that name one store in two
/// forms, such as an `s3://` URL and an `https://` endpoint URL, are not
/// found.
pub(crate) fn refuse_overlapping_stores(stores: &[(String, &str)]) -> Result<(), String> {
    let locations = stores
        .iter()
        .map(|(label, raw)| Ok((label, store_location(raw)?)))
        .collect::<Result<Vec<_>, String>>()?;
    for (index, (label, location)) in locations.iter().enumerate() {
        for (other_label, other) in &locations[index + 1..] {
            if location.contains(other) || other.contains(location) {
                return Err(format!(
                    "{label} and {other_label} name overlapping stores; give each one its own \
                     store"
                ));
            }
        }
    }
    Ok(())
}

/// Where a store URL points: its scheme, its host and port, and its non-empty
/// path segments.
#[derive(Debug, Eq, PartialEq)]
struct StoreLocation {
    scheme: String,
    authority: String,
    segments: Vec<String>,
}

impl StoreLocation {
    /// Whether `other` is this store or a prefix inside it.
    fn contains(&self, other: &Self) -> bool {
        self.scheme == other.scheme
            && self.authority == other.authority
            && other.segments.starts_with(&self.segments)
    }
}

fn store_location(raw: &str) -> Result<StoreLocation, String> {
    let url = Url::parse(raw).map_err(|error| format!("invalid store URL `{raw}`: {error}"))?;
    let authority = match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_string(),
        (None, _) => String::new(),
    };
    let segments = url
        .path_segments()
        .map(|segments| {
            segments
                .filter(|segment| !segment.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    Ok(StoreLocation {
        scheme: url.scheme().to_string(),
        authority,
        segments,
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn overlapping_stores_are_refused_before_a_store_is_opened() {
        let cases = [
            ("s3://restore-metrics", "s3://restore-logs", true),
            ("s3://restore-metrics", "s3://restore-metrics", false),
            ("s3://restore-metrics", "s3://restore-metrics/", false),
            ("s3://restore", "s3://restore/metrics", false),
            ("s3://restore/metrics/", "s3://restore", false),
            ("s3://restore/metrics", "s3://restore/logs", true),
            ("s3://restore/metrics", "s3://restore/metrics-old", true),
            ("s3://restore/metrics", "gs://restore/metrics", true),
            ("file:///mnt/logs", "file:///mnt/logs/querier", false),
            ("file:///mnt/logs-a", "file:///mnt/logs-b", true),
            (
                "http://minio:9000/restore",
                "http://minio:9001/restore",
                true,
            ),
        ];
        for (first, second, disjoint) in cases {
            let result = refuse_overlapping_stores(&[
                ("part `metrics`".into(), first),
                ("part `logs`".into(), second),
            ]);
            check!(result.is_ok() == disjoint, "{first} {second}: {result:?}");
        }
        check!(
            refuse_overlapping_stores(&[
                ("the backup set".into(), "s3://backups/cut-1"),
                ("part `metrics`".into(), "s3://krabka-metrics"),
                ("part `logs`".into(), "s3://backups"),
            ]) == Err(
                "the backup set and part `logs` name overlapping stores; give each one its own \
                 store"
                    .into()
            )
        );
        check!(refuse_overlapping_stores(&[("part `metrics`".into(), "not a url")]).is_err());
    }
}
