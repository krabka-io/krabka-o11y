//! Typed Go `time.Time` values for pinned Loki templates. Unix arithmetic wraps
//! like Go; civil dates include year zero and Go's full Unix-second domain.
//! Go1.26.5 src/time/format.go SHA256 f288c56858ab25828551681778f006b591ed140fc12e60122b6a9a7e35b07eb7;
//! src/time/time.go SHA256 e8b6ea9e6e7cb890aeed1d24ee7163a2237b6fa78ffc564efb8688dba717f02a.
use std::fmt::Write as _;

mod calendar;
pub(super) mod duration;
pub(super) mod layout;
mod location_reflection;
pub(super) mod methods;
mod parse;
mod zone;

#[derive(Clone, Debug, PartialEq)]
pub struct TemplateTime {
    seconds: i64,
    nanos: u32,
    location: Location,
    monotonic: Option<i64>,
}
#[derive(Clone, Debug)]
enum Location {
    Utc,
    Local,
    Named(
        std::sync::Arc<zone::Zone>,
        std::sync::Arc<location_reflection::LocationData>,
    ),
    Fixed {
        name: String,
        offset: i32,
        identity: std::sync::Arc<location_reflection::LocationData>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct TemplateTimeLocation(Location);
impl TemplateTimeLocation {
    pub(crate) fn address(&self) -> usize {
        match &self.0 {
            Location::Utc => std::ptr::from_ref(&*UTC_DATA) as usize,
            Location::Local => std::ptr::addr_of!(LOCAL_IDENTITY) as usize,
            Location::Named(_, data) | Location::Fixed { identity: data, .. } => {
                std::sync::Arc::as_ptr(data) as usize
            }
        }
    }
    pub(crate) fn reflection(&self) -> crate::template::TemplateHistogramView {
        let address = self.address();
        match &self.0 {
            Location::Utc => UTC_DATA.reflection(address),
            Location::Local => LOCAL_DATA.get().map_or_else(
                || location_reflection::LocationData::empty("").reflection(address),
                |data| data.reflection.reflection(address),
            ),
            Location::Named(_, data) | Location::Fixed { identity: data, .. } => {
                data.reflection(address)
            }
        }
    }
    pub(crate) fn name(&self) -> String {
        match &self.0 {
            Location::Utc => "UTC".into(),
            Location::Local => {
                local_zone().map_or_else(|| "UTC".into(), |zone| zone.name.to_string())
            }
            Location::Named(zone, _) => zone.name.to_string(),
            Location::Fixed { name, .. } => name.clone(),
        }
    }
}

impl TemplateTime {
    pub(crate) fn from_offset(seconds: i64, nanos: u32, offset: i32) -> Self {
        Self {
            seconds,
            nanos,
            monotonic: None,
            location: if offset == 0 {
                Location::Utc
            } else {
                Location::fixed(String::new(), offset)
            },
        }
    }
    pub(crate) fn from_unix_nanos(nanos: i64) -> Self {
        Self {
            seconds: nanos.div_euclid(1_000_000_000),
            nanos: u32::try_from(nanos.rem_euclid(1_000_000_000))
                .expect("normalized nanoseconds are below one billion"),
            location: Location::Local,
            monotonic: None,
        }
    }
    pub(crate) fn from_unix_seconds(seconds: i64) -> Self {
        Self {
            seconds,
            nanos: 0,
            location: Location::Local,
            monotonic: None,
        }
    }
    pub(crate) fn from_epoch_string(epoch: &str) -> Option<Self> {
        let value = epoch.parse::<i64>().ok()?;
        Some(match epoch.len() {
            5 => Self::from_unix_seconds(value.wrapping_mul(86400)),
            10 => Self::from_unix_seconds(value),
            13 => Self::from_unix_nanos(value.wrapping_mul(1_000_000)),
            16 => Self::from_unix_nanos(value.wrapping_mul(1000)),
            19 => Self::from_unix_nanos(value),
            _ => return None,
        })
    }
    pub(crate) fn zero() -> Self {
        Self {
            seconds: -62_135_596_800,
            nanos: 0,
            location: Location::Utc,
            monotonic: None,
        }
    }
    pub(crate) fn now() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        Self {
            seconds: i64::from_ne_bytes(
                u64::try_from(nanos / 1_000_000_000)
                    .expect("SystemTime duration seconds fit u64")
                    .to_ne_bytes(),
            ),
            nanos: u32::try_from(nanos % 1_000_000_000)
                .expect("nanosecond remainder is below one billion"),
            location: Location::Local,
            monotonic: Some(process_monotonic_nanos()),
        }
    }
    pub(crate) fn unix_seconds(&self) -> i64 {
        self.seconds
    }
    pub(crate) fn unix_millis(&self) -> i64 {
        self.seconds
            .wrapping_mul(1000)
            .wrapping_add(i64::from(self.nanos / 1_000_000))
    }
    pub(crate) fn unix_nanos(&self) -> i64 {
        self.seconds
            .wrapping_mul(1_000_000_000)
            .wrapping_add(i64::from(self.nanos))
    }
    pub(crate) fn in_local(&self) -> Self {
        Self {
            location: Location::Local,
            monotonic: None,
            ..self.clone()
        }
    }
    pub(crate) fn format(&self, layout: &str) -> String {
        layout::format(self, layout)
    }
    pub(crate) fn format_bytes(&self, mut bytes: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();
        while !bytes.is_empty() {
            match std::str::from_utf8(bytes) {
                Ok(layout) => {
                    output.extend(self.format(layout).as_bytes());
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    output.extend(
                        self.format(
                            std::str::from_utf8(&bytes[..valid]).expect("validated layout prefix"),
                        )
                        .as_bytes(),
                    );
                    output.push(bytes[valid]);
                    bytes = &bytes[valid + 1..];
                }
            }
        }
        output
    }
    pub(crate) fn parse(layout: &str, zone: &str, value: &str) -> Option<Self> {
        parse::parse(layout, zone, value)
    }
    pub(crate) fn as_string(&self) -> String {
        let mut value = self.format("2006-01-02 15:04:05.999999999 -0700 MST");
        if let Some(monotonic) = self.monotonic {
            let magnitude = monotonic.unsigned_abs();
            write!(
                value,
                " m={}{:}.{:09}",
                if monotonic < 0 { "-" } else { "+" },
                magnitude / 1_000_000_000,
                magnitude % 1_000_000_000
            )
            .expect("writing to a String cannot fail");
        }
        value
    }
    pub(crate) fn go_string(&self) -> String {
        let parts = self.parts();
        let location = match &self.location {
            Location::Utc => "time.UTC".into(),
            Location::Local => "time.Local".into(),
            Location::Named(zone, _) => format!("time.Location({:?})", zone.name),
            Location::Fixed { name, .. } => format!("time.Location({name:?})"),
        };
        format!(
            "time.Date({}, time.{}, {}, {}, {}, {}, {}, {})",
            parts.year,
            layout::MONTHS[usize::try_from(parts.month - 1).expect("month index is in 0..12")],
            parts.day,
            parts.hour,
            parts.minute,
            parts.second,
            self.nanos,
            location
        )
    }
    pub(crate) fn reflection_fields(&self) -> (u64, i64, Option<usize>) {
        // time.Time without a monotonic reading stores nanoseconds in wall,
        // seconds since January 1 year 1 in ext, and nil for UTC's location.
        let location = match &self.location {
            Location::Utc => None,
            Location::Local => Some(std::ptr::addr_of!(LOCAL_IDENTITY) as usize),
            Location::Named(_, identity) | Location::Fixed { identity, .. } => {
                Some(std::sync::Arc::as_ptr(identity) as usize)
            }
        };
        if let Some(monotonic) = self.monotonic {
            let wall_seconds = self.seconds.wrapping_add(2_682_288_000);
            if (0..1i64 << 33).contains(&wall_seconds) {
                return (
                    (1u64 << 63)
                        | u64::try_from(wall_seconds)
                            .expect("monotonic wall seconds are nonnegative")
                            << 30
                        | u64::from(self.nanos),
                    monotonic,
                    location,
                );
            }
        }
        (
            u64::from(self.nanos),
            self.seconds.wrapping_add(62_135_596_800),
            location,
        )
    }
    #[cfg(test)]
    pub(crate) fn parsed_fields(&self) -> (i32, u32, u32, u32, u32, u32, u32, i32) {
        let parts = self.parts();
        (
            i32::try_from(parts.year).expect("parsed test year fits i32"),
            parts.month,
            parts.day,
            parts.hour,
            parts.minute,
            parts.second,
            self.nanos,
            self.zone().1,
        )
    }
    fn parts(&self) -> calendar::Parts {
        let (_, offset) = self.zone();
        calendar::parts(self.seconds.wrapping_add(i64::from(offset)))
    }
    fn zone(&self) -> (String, i32) {
        self.location.zone(self.seconds)
    }
}

fn process_monotonic_nanos() -> i64 {
    static START: std::sync::LazyLock<std::time::Instant> =
        std::sync::LazyLock::new(std::time::Instant::now);
    i64::from_le_bytes(
        START.elapsed().as_nanos().to_le_bytes()[..8]
            .try_into()
            .expect("u128 has eight low bytes"),
    )
}

static LOCAL_IDENTITY: u8 = 0;
static UTC_DATA: std::sync::LazyLock<location_reflection::LocationData> =
    std::sync::LazyLock::new(|| location_reflection::LocationData::empty("UTC"));
struct LocalData {
    zone: Option<std::sync::Arc<zone::Zone>>,
    reflection: location_reflection::LocationData,
}
static LOCAL_DATA: std::sync::OnceLock<LocalData> = std::sync::OnceLock::new();
fn load_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |value| i64::from_ne_bytes(value.as_secs().to_ne_bytes()))
}
fn local_data() -> &'static LocalData {
    LOCAL_DATA.get_or_init(|| {
        let zone = match std::env::var("TZ") {
            Ok(name) if name.is_empty() => None,
            Ok(name) => {
                let name = name.strip_prefix(':').unwrap_or(&name);
                if std::path::Path::new(name).is_absolute() {
                    std::fs::read(name).ok().and_then(|data| {
                        let zone_name = if name == "/etc/localtime" {
                            "Local"
                        } else {
                            Box::leak(name.to_owned().into_boxed_str())
                        };
                        zone::Zone::parse(zone_name, Box::leak(data.into_boxed_slice()))
                            .map(std::sync::Arc::new)
                    })
                } else if name == "UTC" {
                    None
                } else {
                    zone::load(name)
                }
            }
            Err(_) => std::fs::read("/etc/localtime").ok().and_then(|data| {
                zone::Zone::parse("Local", Box::leak(data.into_boxed_slice()))
                    .map(std::sync::Arc::new)
            }),
        };
        let reflection = zone.as_ref().map_or_else(
            || location_reflection::LocationData::empty("UTC"),
            |zone| zone.reflection_data(load_seconds()),
        );
        LocalData { zone, reflection }
    })
}
fn local_zone() -> Option<std::sync::Arc<zone::Zone>> {
    local_data().zone.clone()
}

impl PartialEq for Location {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Utc, Self::Utc) | (Self::Local, Self::Local) => true,
            (Self::Named(_, left), Self::Named(_, right))
            | (
                Self::Fixed { identity: left, .. },
                Self::Fixed {
                    identity: right, ..
                },
            ) => std::sync::Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

impl Location {
    fn fixed(name: String, offset: i32) -> Self {
        static HOURS: std::sync::LazyLock<Vec<std::sync::Arc<location_reflection::LocationData>>> =
            std::sync::LazyLock::new(|| {
                (-12..=14)
                    .map(|hour| {
                        std::sync::Arc::new(location_reflection::LocationData::fixed(
                            "",
                            hour * 3600,
                        ))
                    })
                    .collect()
            });
        let identity = if name.is_empty()
            && offset % 3600 == 0
            && (-12 * 3600..=14 * 3600).contains(&offset)
        {
            HOURS[usize::try_from(offset / 3600 + 12).expect("fixed-zone hour index is in 0..=26")]
                .clone()
        } else {
            std::sync::Arc::new(location_reflection::LocationData::fixed(&name, offset))
        };
        Self::Fixed {
            name,
            offset,
            identity,
        }
    }
    fn load(name: &str) -> Self {
        match name {
            "UTC" | "" => Self::Utc,
            "Local" => Self::Local,
            _ => zone::load(name).map_or(Self::Utc, |zone| {
                let identity = std::sync::Arc::new(zone.reflection_data(load_seconds()));
                Self::Named(zone, identity)
            }),
        }
    }
    fn zone(&self, seconds: i64) -> (String, i32) {
        match self {
            Self::Utc => ("UTC".into(), 0),
            Self::Fixed { name, offset, .. } => (name.clone(), *offset),
            Self::Local => local_zone().map_or(("UTC".into(), 0), |zone| zone.lookup(seconds)),
            Self::Named(zone, _) => zone.lookup(seconds),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    #[test]
    fn every_go_layout_token_and_fraction_guard_has_an_independent_golden() {
        let value = TemplateTime::parse(
            "2006-01-02 15:04:05.999999999 -07:00:00 MST",
            "UTC",
            "2024-02-03 13:04:05.12 +01:01:01 ODD",
        )
        .unwrap();
        for (layout, expected) in [
            (
                "January Jan Janitor|Monday Mon Mondayish|MST",
                "February Feb Janitor|Saturday Sat Saturdayish|ODD",
            ),
            (
                "2006 06 1 01 2 _2 02 __2 002 3 03 15 4 04 5 05 PM pm",
                "2024 24 2 02 3  3 03  34 034 1 01 13 4 04 5 05 PM pm",
            ),
            (
                "-07 -0700 -07:00 -070000 -07:00:00 Z07 Z0700 Z07:00 Z070000 Z07:00:00",
                "+01 +0101 +01:01 +010101 +01:01:01 +01 +0101 +01:01 +010101 +01:01:01",
            ),
            (
                "15:04:05.0000000000|15:04:05.999999999|15:04:05,000|15:04:05,999|.0999|.9990",
                "13:04:05.120000000|13:04:05.12|13:04:05,120|13:04:05,12|.0999|.9990",
            ),
        ] {
            check!(value.format(layout) == expected);
        }
    }
    #[test]
    fn parse_defaults_year_day_fraction_and_zone_definitions_match_go() {
        for (layout, zone, text, expected, seconds) in [
            (
                "06-002 03:04pm",
                "UTC",
                "69-060 12:30am",
                "1969-03-01 00:30:00 +0000 UTC",
                -26_436_600,
            ),
            (
                "2006-01-02 15:04:05",
                "UTC",
                "2024-02-29 9:05:06,123456789123",
                "2024-02-29 09:05:06.123456789 +0000 UTC",
                1_709_197_506,
            ),
            (
                "Monday January _2 2006 3:4:5PM",
                "UTC",
                "monday february  3 2024 1:4:5PM",
                "2024-02-03 13:04:05 +0000 UTC",
                1_706_965_445,
            ),
            (
                "2006 002 01 02",
                "UTC",
                "2024 060 02 29",
                "2024-02-29 00:00:00 +0000 UTC",
                1_709_164_800,
            ),
            (
                "15:04:05.999",
                "UTC",
                "12:34:56",
                "0000-01-01 12:34:56 +0000 UTC",
                -62_167_173_904,
            ),
            (
                "15:04:05.000",
                "UTC",
                "12:34:56,123",
                "0000-01-01 12:34:56.123 +0000 UTC",
                -62_167_173_904,
            ),
            (
                "2006-01-02 -07:00",
                "UTC",
                "2024-01-02 +24:00",
                "2024-01-02 00:00:00 +2400 +2400",
                1_704_067_200,
            ),
            (
                "2006-01-02 MST",
                "UTC",
                "2024-01-02 XYZ",
                "2024-01-02 00:00:00 +0000 XYZ",
                1_704_153_600,
            ),
            (
                "2006-01-02 MST",
                "UTC",
                "2024-01-02 GMT+2",
                "2024-01-02 02:00:00 +0200 GMT+2",
                1_704_153_600,
            ),
            (
                "2006-01-02 15:04:05 MST",
                "America/New_York",
                "2024-07-02 12:00:00 EST",
                "2024-07-02 13:00:00 -0400 EDT",
                1_719_939_600,
            ),
            (
                "15:04 MST",
                "America/New_York",
                "12:00 EST",
                "0000-01-01 12:03:58 -0456 LMT",
                -62_167_158_000,
            ),
            (
                "2006-01-02 MST",
                "Europe/Paris",
                "2024-01-02 PMT",
                "2024-01-02 00:50:39 +0100 CET",
                1_704_153_039,
            ),
            (
                "2006-01-02 MST",
                "Asia/Kolkata",
                "2024-01-02 MMT",
                "2024-01-02 00:08:50 +0530 IST",
                1_704_134_330,
            ),
        ] {
            let value = TemplateTime::parse(layout, zone, text).unwrap();
            check!(value.as_string() == expected);
            check!(value.unix_seconds() == seconds);
        }
        for (layout, text) in [
            ("2006-01-02", "2024-02-30"),
            ("2006 002 01 02", "2024 060 03 01"),
            ("2006-01-02", "2023-02-29"),
            ("2006-01-02", "2024-01-01extra"),
            ("15:04:05.000", "12:34:56.1"),
            ("15:04:05", "12:60:00"),
            ("15:04:05", "25:00:00"),
            ("2006-01-02 -07:00", "2024-01-02 +25:00"),
        ] {
            check!(TemplateTime::parse(layout, "UTC", text).is_none());
        }
    }
    #[test]
    fn named_zone_dst_gap_and_overlap_resolution_matches_time_date() {
        for (zone, text, expected) in [
            (
                "America/New_York",
                "2024-03-10 02:30:00",
                "2024-03-10 01:30:00 -0500 EST",
            ),
            (
                "America/New_York",
                "2024-11-03 01:30:00",
                "2024-11-03 01:30:00 -0400 EDT",
            ),
            (
                "Europe/Berlin",
                "2024-03-31 02:30:00",
                "2024-03-31 03:30:00 +0200 CEST",
            ),
            (
                "Europe/Berlin",
                "2024-10-27 02:30:00",
                "2024-10-27 02:30:00 +0100 CET",
            ),
            (
                "invalid",
                "2024-01-02 12:04:05",
                "2024-01-02 12:04:05 +0000 UTC",
            ),
        ] {
            let value = TemplateTime::parse("2006-01-02 15:04:05", zone, text).unwrap();
            check!(value.as_string() == expected);
        }
    }
    #[test]
    fn year_zero_zero_time_and_full_unix_domain_preserve_go_arithmetic() {
        let zero = TemplateTime::zero();
        check!(zero.as_string() == "0001-01-01 00:00:00 +0000 UTC");
        check!(zero.unix_nanos() == -6_795_364_578_871_345_152);
        let year_zero = TemplateTime::parse("15:04:05.000", "UTC", "12:34:56,123").unwrap();
        check!(year_zero.unix_nanos() == -6_826_941_682_748_345_152);
        for (seconds, expected) in [
            (i64::MAX, "292277026596-12-04 15:30:07 +0000 UTC"),
            (i64::MIN, "292277026596-12-04 15:30:08 +0000 UTC"),
        ] {
            let value = TemplateTime {
                seconds,
                nanos: 0,
                location: Location::Utc,
                monotonic: None,
            };
            check!(value.as_string() == expected);
            check!(value.unix_seconds() == seconds);
        }
        let pre_epoch = TemplateTime::from_unix_nanos(-1);
        check!(pre_epoch.unix_seconds() == -1);
        check!(pre_epoch.unix_millis() == -1);
        check!(pre_epoch.unix_nanos() == -1);
        let parsed = TemplateTime::parse(
            "2006-01-02 15:04:05",
            "America/New_York",
            "2024-01-02 12:04:05",
        )
        .unwrap();
        check!(
            parsed.go_string()
                == "time.Date(2024, time.January, 2, 12, 4, 5, 0, time.Location(\"America/New_York\"))"
        );
    }
    #[test]
    fn epoch_string_units_overflow_and_location_identity_match_go() {
        for (input, seconds, nanos) in [
            ("9999999999999", -8_446_744_074, -8_446_744_073_710_551_616),
            (
                "9999999999999999",
                -8_446_744_074,
                -8_446_744_073_709_552_616,
            ),
            ("-1234", -106_617_600, -106_617_600_000_000_000),
            ("+1234", 106_617_600, 106_617_600_000_000_000),
        ] {
            let value = TemplateTime::from_epoch_string(input).unwrap();
            check!(value.unix_seconds() == seconds);
            check!(value.unix_nanos() == nanos);
        }
        for input in ["1", "123456", "9999999999999999999", "not a time"] {
            check!(TemplateTime::from_epoch_string(input).is_none());
        }
        let utc = TemplateTime::parse("2006", "UTC", "2024").unwrap();
        check!(utc == TemplateTime::parse("2006", "UTC", "2024").unwrap());
        let named = TemplateTime::parse("2006", "Europe/Berlin", "2024").unwrap();
        check!(named == named.clone());
        check!(named != TemplateTime::parse("2006", "Europe/Berlin", "2024").unwrap());
        for (name, seconds, expected) in [
            (
                "America/New_York",
                i64::MIN,
                "292277026596-12-04 10:34:06 -0456 LMT",
            ),
            (
                "America/New_York",
                i64::MAX,
                "292277026596-12-04 10:30:07 -0500 EST",
            ),
            (
                "Europe/Berlin",
                i64::MIN,
                "292277026596-12-04 16:23:36 +0053 LMT",
            ),
            (
                "Europe/Berlin",
                i64::MAX,
                "292277026596-12-04 16:30:07 +0100 CET",
            ),
        ] {
            let value = TemplateTime {
                seconds,
                nanos: 0,
                location: Location::load(name),
                monotonic: None,
            };
            check!(value.as_string() == expected);
        }
        let now = TemplateTime::now();
        check!(now.as_string().contains(" m=+"));
        check!(!now.in_local().as_string().contains(" m="));
        check!(
            !TemplateTime::from_unix_seconds(0)
                .as_string()
                .contains(" m=")
        );
    }
}
