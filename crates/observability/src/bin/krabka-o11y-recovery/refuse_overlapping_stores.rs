use super::Url;

/// Refuses two stores of one command that are the same store, or where one
/// holds the other.
///
/// Each item is a label for the message and a store URL. The check compares
/// the scheme, the host and port, and the path segments of each URL, so it
/// runs before any store is opened. Two URLs that name one store in two
/// forms, such as an `s3://` URL and an `https://` endpoint URL, are not
/// found.
pub(crate) struct StoreComparison<'a> {
    pub(crate) label: String,
    pub(crate) raw: &'a str,
}

pub(crate) fn refuse_overlapping_stores(stores: &[StoreComparison<'_>]) -> Result<(), String> {
    let locations = stores
        .iter()
        .map(|store| {
            Ok(StoreLocationEntry {
                label: store.label.clone(),
                location: store_location(store.raw)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    for index in 0..locations.len() {
        let current = &locations[index];
        for other in &locations[index + 1..] {
            if current.location.contains(&other.location)
                || other.location.contains(&current.location)
            {
                return Err(format!(
                    "{} and {} name overlapping stores; give each one its own \
                     store",
                    current.label, other.label
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

struct StoreLocationEntry {
    label: String,
    location: StoreLocation,
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
    let authority = match url.host_str() {
        Some(host) => match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        },
        None => String::new(),
    };
    let segments = url.path_segments().map_or_default(|segments| {
        segments
            .filter(|segment| !segment.is_empty())
            .map(String::from)
            .collect()
    });
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

    struct OverlapCase {
        first: &'static str,
        second: &'static str,
        disjoint: bool,
    }

    #[test]
    fn overlapping_stores_are_refused_before_a_store_is_opened() {
        let cases = [
            OverlapCase {
                first: "s3://restore-metrics",
                second: "s3://restore-logs",
                disjoint: true,
            },
            OverlapCase {
                first: "s3://restore-metrics",
                second: "s3://restore-metrics",
                disjoint: false,
            },
            OverlapCase {
                first: "s3://restore-metrics",
                second: "s3://restore-metrics/",
                disjoint: false,
            },
            OverlapCase {
                first: "s3://restore",
                second: "s3://restore/metrics",
                disjoint: false,
            },
            OverlapCase {
                first: "s3://restore/metrics/",
                second: "s3://restore",
                disjoint: false,
            },
            OverlapCase {
                first: "s3://restore/metrics",
                second: "s3://restore/logs",
                disjoint: true,
            },
            OverlapCase {
                first: "s3://restore/metrics",
                second: "s3://restore/metrics-old",
                disjoint: true,
            },
            OverlapCase {
                first: "s3://restore/metrics",
                second: "gs://restore/metrics",
                disjoint: true,
            },
            OverlapCase {
                first: "file:///mnt/logs",
                second: "file:///mnt/logs/querier",
                disjoint: false,
            },
            OverlapCase {
                first: "file:///mnt/logs-a",
                second: "file:///mnt/logs-b",
                disjoint: true,
            },
            OverlapCase {
                first: "http://minio:9000/restore",
                second: "http://minio:9001/restore",
                disjoint: true,
            },
        ];
        for case in cases {
            let result = refuse_overlapping_stores(&[
                StoreComparison {
                    label: "part `metrics`".into(),
                    raw: case.first,
                },
                StoreComparison {
                    label: "part `logs`".into(),
                    raw: case.second,
                },
            ]);
            check!(
                result.is_ok() == case.disjoint,
                "{} {}: {result:?}",
                case.first,
                case.second
            );
        }
        check!(
            refuse_overlapping_stores(&[
                StoreComparison {
                    label: "the backup set".into(),
                    raw: "s3://backups/cut-1"
                },
                StoreComparison {
                    label: "part `metrics`".into(),
                    raw: "s3://krabka-metrics"
                },
                StoreComparison {
                    label: "part `logs`".into(),
                    raw: "s3://backups"
                },
            ]) == Err(
                "the backup set and part `logs` name overlapping stores; give each one its own \
                 store"
                    .into()
            )
        );
        check!(
            refuse_overlapping_stores(&[StoreComparison {
                label: "part `metrics`".into(),
                raw: "not a url",
            }])
            .is_err()
        );
    }
}
