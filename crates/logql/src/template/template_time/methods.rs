use num_traits::ToPrimitive as _;

use super::{
    super::TemplateRuntimeValue as V, Location, TemplateTime, TemplateTimeLocation, calendar,
    duration,
};

pub(in crate::template) fn signature(name: &str, index: usize) -> Option<&'static str> {
    match name {
        "Format" => Some("string"),
        "AppendFormat" => Some(if index == 0 { "[]byte" } else { "string" }),
        "Add" | "Round" | "Truncate" => Some("time.Duration"),
        "AddDate" => Some("int"),
        "After" | "Before" | "Compare" | "Equal" | "Sub" => Some("time.Time"),
        "In" => Some("*time.Location"),
        "AppendBinary" | "AppendText" => Some("[]byte"),
        _ => None,
    }
}

pub(in crate::template) fn call(value: &V, name: &str, args: &[V]) -> Result<V, String> {
    match value {
        V::Time(time) => time.call(name, args),
        V::TimePointer(time) => time.call(name, args),
        V::DurationPointer(value) => duration_call(**value, name, args),
        V::Month(value) if name == "String" && args.is_empty() => Ok(V::String(
            super::layout::MONTHS
                .get(usize::from(value.wrapping_sub(1)))
                .map_or_else(|| format!("%!Month({value})"), |name| (*name).into()),
        )),
        V::Weekday(value) if name == "String" && args.is_empty() => Ok(V::String(
            super::layout::DAYS
                .get(usize::from(*value))
                .map_or_else(|| format!("%!Weekday({value})"), |name| (*name).into()),
        )),
        V::Duration(value) => duration_call(*value, name, args),
        V::TimeLocation(location) if name == "String" && args.is_empty() => {
            Ok(V::String(location.name()))
        }
        _ => Err(format!("cannot evaluate field or method {name}")),
    }
}

impl TemplateTime {
    fn call(&self, name: &str, args: &[V]) -> Result<V, String> {
        let count = match name {
            "Format" | "Add" | "Sub" | "After" | "Before" | "Compare" | "Equal" | "In"
            | "Round" | "Truncate" | "AppendBinary" | "AppendText" => 1,
            "AppendFormat" => 2,
            "AddDate" => 3,
            _ => 0,
        };
        if args.len() != count {
            return Err(format!("wrong number of args for {name}"));
        }
        let parts = self.parts();
        let result = match name {
            "String" => V::String(self.as_string()),
            "GoString" => V::String(self.go_string()),
            "Format" => V::Bytes(self.format_bytes(string(&args[0])?)),
            "Year" => V::Integer(parts.year),
            "Month" => V::Month(u8::try_from(parts.month).expect("civil month is in 1..=12")),
            "Day" => V::Integer(i64::from(parts.day)),
            "Weekday" => V::Weekday(u8::try_from(parts.weekday).expect("weekday is in 0..7")),
            "YearDay" => V::Integer(i64::from(parts.yearday)),
            "Hour" => V::Integer(i64::from(parts.hour)),
            "Minute" => V::Integer(i64::from(parts.minute)),
            "Second" => V::Integer(i64::from(parts.second)),
            "Nanosecond" => V::Integer(i64::from(self.nanos)),
            "Unix" => V::Integer64(self.unix_seconds()),
            "UnixMilli" => V::Integer64(self.unix_millis()),
            "UnixMicro" => V::Integer64(
                self.seconds
                    .wrapping_mul(1_000_000)
                    .wrapping_add(i64::from(self.nanos / 1000)),
            ),
            "UnixNano" => V::Integer64(self.unix_nanos()),
            "IsZero" => boolean(self.seconds == -62_135_596_800 && self.nanos == 0),
            "IsDST" => boolean(match &self.location {
                Location::Named(zone, _) => zone.lookup_full(self.seconds).2,
                Location::Local => {
                    super::local_zone().is_some_and(|zone| zone.lookup_full(self.seconds).2)
                }
                _ => false,
            }),
            "Location" => V::TimeLocation(TemplateTimeLocation(self.location.clone())),
            "UTC" | "Local" => V::Time(Self {
                location: if name == "UTC" {
                    Location::Utc
                } else {
                    Location::Local
                },
                monotonic: None,
                ..self.clone()
            }),
            "In" => {
                let V::TimeLocation(location) = &args[0] else {
                    return Err("Time.In requires a non-nil location".into());
                };
                V::Time(Self {
                    location: location.0.clone(),
                    monotonic: None,
                    ..self.clone()
                })
            }
            "Add" => V::Time(self.add(span(&args[0])?)),
            "AddDate" => {
                let years = integer(&args[0])?;
                let months = integer(&args[1])?;
                let days = integer(&args[2])?;
                let months = (i64::from(parts.month) - 1).wrapping_add(months);
                let year = parts
                    .year
                    .wrapping_add(years)
                    .wrapping_add(months.div_euclid(12));
                let month =
                    u32::try_from(months.rem_euclid(12)).expect("month remainder is in 0..12") + 1;
                let seconds = calendar::date_seconds(
                    year,
                    month,
                    i64::from(parts.day).wrapping_add(days),
                    parts.hour,
                    parts.minute,
                    parts.second,
                );
                let (_, offset) = self.location.zone(seconds);
                let (_, adjusted) = self.location.zone(seconds.wrapping_sub(i64::from(offset)));
                V::Time(Self {
                    seconds: seconds.wrapping_sub(i64::from(adjusted)),
                    monotonic: None,
                    ..self.clone()
                })
            }
            "Sub" | "After" | "Before" | "Compare" | "Equal" => {
                let V::Time(other) = &args[0] else {
                    return Err(format!("{name} requires time.Time"));
                };
                let compare = if let (Some(left), Some(right)) = (self.monotonic, other.monotonic) {
                    left.cmp(&right)
                } else {
                    self.seconds
                        .wrapping_add(62_135_596_800)
                        .cmp(&other.seconds.wrapping_add(62_135_596_800))
                        .then(self.nanos.cmp(&other.nanos))
                };
                match name {
                    "Sub" => {
                        let delta =
                            if let (Some(left), Some(right)) = (self.monotonic, other.monotonic) {
                                i128::from(left) - i128::from(right)
                            } else {
                                (i128::from(self.seconds.wrapping_add(62_135_596_800))
                                    - i128::from(other.seconds.wrapping_add(62_135_596_800)))
                                    * 1_000_000_000
                                    + i128::from(self.nanos)
                                    - i128::from(other.nanos)
                            };
                        V::Duration(
                            i64::try_from(delta.clamp(i128::from(i64::MIN), i128::from(i64::MAX)))
                                .expect("clamped duration fits i64"),
                        )
                    }
                    "After" => boolean(compare.is_gt()),
                    "Before" => boolean(compare.is_lt()),
                    "Equal" => boolean(compare.is_eq()),
                    _ => V::Integer(if compare.is_lt() {
                        -1
                    } else {
                        i64::from(compare.is_gt())
                    }),
                }
            }
            "Round" | "Truncate" => {
                let multiple = span(&args[0])?;
                let time = Self {
                    monotonic: None,
                    ..self.clone()
                };
                if multiple <= 0 {
                    V::Time(time)
                } else {
                    let total = i128::from(self.seconds.wrapping_add(62_135_596_800))
                        * 1_000_000_000
                        + i128::from(self.nanos);
                    let remainder = i64::try_from(total.rem_euclid(i128::from(multiple)))
                        .expect("remainder is below the positive i64 multiple");
                    let delta = if name == "Truncate"
                        || remainder.unsigned_abs() * 2 < multiple.unsigned_abs()
                    {
                        -remainder
                    } else {
                        multiple - remainder
                    };
                    V::Time(time.add(delta))
                }
            }
            "MarshalBinary" | "GobEncode" => V::ByteSlice(self.binary()?),
            "AppendBinary" => {
                let mut bytes = byte_slice(&args[0])?;
                bytes.extend(self.binary()?);
                V::ByteSlice(bytes)
            }
            "MarshalText" => V::ByteSlice(self.rfc3339()?.into_bytes()),
            "AppendText" => {
                let mut bytes = byte_slice(&args[0])?;
                bytes.extend(self.rfc3339()?.as_bytes());
                V::ByteSlice(bytes)
            }
            "MarshalJSON" => V::ByteSlice(format!("\"{}\"", self.rfc3339()?).into_bytes()),
            "AppendFormat" => {
                let mut bytes = byte_slice(&args[0])?;
                bytes.extend(self.format_bytes(string(&args[1])?));
                V::ByteSlice(bytes)
            }
            // These signatures cannot be called by text/template: only a
            // second error result is legal, not calendar/zone tuples.
            "Date" | "Clock" | "ISOWeek" | "Zone" | "ZoneBounds" => {
                return Err(format!("invalid result signature for {name}"));
            }
            _ => {
                return Err(format!(
                    "cannot evaluate field or method {name} in time.Time"
                ));
            }
        };
        Ok(result)
    }
    fn add(&self, span: i64) -> Self {
        let nanos = i64::from(self.nanos) + span % 1_000_000_000;
        let delta = span / 1_000_000_000 + nanos.div_euclid(1_000_000_000);
        let internal = self
            .seconds
            .wrapping_add(62_135_596_800)
            .saturating_add(delta);
        Self {
            seconds: internal.wrapping_sub(62_135_596_800),
            nanos: u32::try_from(nanos.rem_euclid(1_000_000_000))
                .expect("normalized nanoseconds are below one billion"),
            monotonic: self.monotonic.and_then(|value| value.checked_add(span)),
            ..self.clone()
        }
    }
    fn binary(&self) -> Result<Vec<u8>, String> {
        let (_, offset) = self.zone();
        let minutes = if matches!(self.location, Location::Utc) {
            -1
        } else {
            offset / 60
        };
        if !matches!(self.location, Location::Utc)
            && (minutes == -1 || i16::try_from(minutes).is_err())
        {
            return Err("Time.MarshalBinary: unexpected zone offset".into());
        }
        let extended = !matches!(self.location, Location::Utc) && offset % 60 != 0;
        let mut bytes = vec![if extended { 2 } else { 1 }];
        bytes.extend(self.seconds.wrapping_add(62_135_596_800).to_be_bytes());
        bytes.extend(self.nanos.to_be_bytes());
        bytes.extend(
            i16::try_from(minutes)
                .expect("validated zone minutes fit i16")
                .to_be_bytes(),
        );
        if extended {
            bytes.push((offset % 60).to_le_bytes()[0]);
        }
        Ok(bytes)
    }
    fn rfc3339(&self) -> Result<String, String> {
        if !(0..=9999).contains(&self.parts().year) || self.zone().1.unsigned_abs() / 3600 >= 24 {
            return Err("Time.MarshalText: date or zone outside RFC3339 range".into());
        }
        Ok(self.format("2006-01-02T15:04:05.999999999Z07:00"))
    }
}

fn duration_call(value: i64, name: &str, args: &[V]) -> Result<V, String> {
    let count = usize::from(matches!(name, "Round" | "Truncate"));
    if args.len() != count {
        return Err(format!("wrong number of args for {name}"));
    }
    Ok(match name {
        "String" => V::String(duration::string(value)),
        "Nanoseconds" => V::Integer64(value),
        "Microseconds" => V::Integer64(value / 1000),
        "Milliseconds" => V::Integer64(value / 1_000_000),
        "Seconds" | "Minutes" | "Hours" => {
            let scale = match name {
                "Seconds" => 1_000_000_000,
                "Minutes" => 60_000_000_000,
                _ => 3_600_000_000_000,
            };
            V::Float(
                (value / scale).to_f64().expect("i64 converts to f64")
                    + (value % scale).to_f64().expect("i64 converts to f64")
                        / scale.to_f64().expect("i64 converts to f64"),
            )
        }
        "Abs" => V::Duration(value.saturating_abs()),
        "Round" => V::Duration(duration::round(value, span(&args[0])?)),
        "Truncate" => {
            let multiple = span(&args[0])?;
            V::Duration(if multiple <= 0 {
                value
            } else {
                value - value % multiple
            })
        }
        _ => {
            return Err(format!(
                "cannot evaluate field or method {name} in time.Duration"
            ));
        }
    })
}
fn integer(value: &V) -> Result<i64, String> {
    if let V::Integer(value) = value {
        Ok(*value)
    } else {
        Err("expected int argument".into())
    }
}
fn span(value: &V) -> Result<i64, String> {
    if let V::Duration(value) = value {
        Ok(*value)
    } else {
        Err("expected time.Duration argument".into())
    }
}
fn string(value: &V) -> Result<&[u8], String> {
    value
        .string_bytes()
        .ok_or_else(|| "expected string argument".into())
}

fn byte_slice(value: &V) -> Result<Vec<u8>, String> {
    match value {
        V::ByteSlice(value) => Ok(value.clone()),
        V::Json(serde_json::Value::Null) => Ok(Vec::new()),
        _ => Err("expected []uint8 argument".into()),
    }
}
fn boolean(value: bool) -> V {
    V::Json(serde_json::Value::Bool(value))
}
