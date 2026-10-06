//! Pinned Go1.26.5 / `IANA2025c` `TZif` data. See TIMEZONE-DATA.md and LICENSE-GO.
//! Selection follows time.Location.lookup, lookupFirstZone and lookupName.
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

use super::Location;
mod recurrence;
use recurrence::Recurrence;

const ARCHIVE: &[u8] = include_bytes!("zoneinfo.zip");
static ZONES: LazyLock<BTreeMap<&'static str, Arc<Zone>>> = LazyLock::new(|| {
    entries(ARCHIVE)
        .expect("pinned stored ZIP")
        .into_iter()
        .map(|(name, data)| {
            (
                name,
                Arc::new(Zone::parse(name, data).expect("pinned TZif data")),
            )
        })
        .collect()
});

#[derive(Clone, Debug)]
pub(super) struct Definition {
    pub(super) name: String,
    pub(super) offset: i32,
    pub(super) daylight: bool,
}
#[derive(Debug)]
pub(super) struct Zone {
    pub(super) name: &'static str,
    width: usize,
    times: &'static [u8],
    indices: &'static [u8],
    definitions: Vec<Definition>,
    recurrence: Option<Recurrence>,
    footer: &'static str,
    standard: &'static [u8],
    utc: &'static [u8],
}

pub(super) fn load(name: &str) -> Option<Arc<Zone>> {
    ZONES.get(name).cloned()
}

impl Zone {
    pub(super) fn parse(name: &'static str, mut data: &'static [u8]) -> Option<Self> {
        let mut fields = counts(data)?;
        let width = if *data.get(4)? == 0 {
            4
        } else {
            let [gmt, standard, leaps, transitions, zones, names] = fields;
            let first_size = 44usize
                .checked_add(transitions.checked_mul(5)?)?
                .checked_add(zones.checked_mul(6)?)?
                .checked_add(names)?
                .checked_add(leaps.checked_mul(8)?)?
                .checked_add(standard)?
                .checked_add(gmt)?;
            data = data.get(first_size..)?;
            fields = counts(data)?;
            8
        };
        let [gmt, standard, leaps, transitions, zones, names] = fields;
        let index_start = 44usize.checked_add(transitions.checked_mul(width)?)?;
        let zone_start = index_start.checked_add(transitions)?;
        let name_start = zone_start.checked_add(zones.checked_mul(6)?)?;
        let name_end = name_start.checked_add(names)?;
        let names = data.get(name_start..name_end)?;
        let mut definitions = Vec::with_capacity(zones.min(256));
        for index in 0..zones {
            let field = data.get(zone_start + index * 6..zone_start + (index + 1) * 6)?;
            let offset = i32::from_be_bytes(field[..4].try_into().ok()?);
            let bytes = names.get(usize::from(field[5])..)?;
            let end = bytes.iter().position(|byte| *byte == 0)?;
            definitions.push(Definition {
                name: std::str::from_utf8(&bytes[..end]).ok()?.to_owned(),
                offset,
                daylight: field[4] != 0,
            });
        }
        if definitions.is_empty() {
            return None;
        }
        let indices = data.get(index_start..zone_start)?;
        if indices
            .iter()
            .any(|index| usize::from(*index) >= definitions.len())
        {
            return None;
        }
        let flags = name_end.checked_add(leaps.checked_mul(width + 4)?)?;
        let tail = name_end
            .checked_add(leaps.checked_mul(width + 4)?)?
            .checked_add(standard)?
            .checked_add(gmt)?;
        let footer = std::str::from_utf8(data.get(tail..)?)
            .ok()?
            .trim_matches('\n');
        let recurrence = if footer.is_empty() {
            None
        } else {
            Some(Recurrence::parse(footer)?)
        };
        Some(Self {
            name,
            width,
            times: data.get(44..index_start)?,
            indices,
            definitions,
            recurrence,
            footer,
            standard: data.get(flags..flags + standard)?,
            utc: data.get(flags + standard..flags + standard + gmt)?,
        })
    }
    pub(super) fn reflection_data(&self, seconds: i64) -> super::location_reflection::LocationData {
        use super::location_reflection::{LocationData, Transition};
        let mut transitions = self
            .indices
            .iter()
            .enumerate()
            .map(|(i, index)| Transition {
                when: self.transition(i),
                index: *index,
                standard: self.standard.get(i).is_some_and(|flag| *flag != 0),
                utc: self.utc.get(i).is_some_and(|flag| *flag != 0),
            })
            .collect::<Vec<_>>();
        if transitions.is_empty() {
            transitions.push(Transition {
                when: i64::MIN,
                index: 0,
                standard: false,
                utc: false,
            });
        }
        let mut data = LocationData::new(
            self.name,
            self.definitions.clone(),
            transitions,
            self.footer,
        );
        if let Some(i) = data.transitions.iter().rposition(|tx| tx.when <= seconds) {
            data.cache_start = data.transitions[i].when;
            data.cache_end = data.transitions.get(i + 1).map_or(i64::MAX, |tx| tx.when);
            data.cache_index = Some(usize::from(data.transitions[i].index));
            if i + 1 == data.transitions.len()
                && let Some(recurrence) = &self.recurrence
            {
                (data.cache_start, data.cache_end) =
                    recurrence.cache_bounds(data.cache_start, seconds);
                let (name, offset, daylight) = recurrence.lookup_full(seconds);
                data.cache_index = data.definitions.iter().position(|zone| {
                    zone.name == name && zone.offset == offset && zone.daylight == daylight
                });
                if data.cache_index.is_none() {
                    data.cache_extra = Some(Box::new(Definition {
                        name,
                        offset,
                        daylight,
                    }));
                }
            }
        }
        data
    }
    fn transition(&self, index: usize) -> i64 {
        let data = &self.times[index * self.width..(index + 1) * self.width];
        if self.width == 8 {
            i64::from_be_bytes(data.try_into().expect("transition width"))
        } else {
            i64::from(i32::from_be_bytes(
                data.try_into().expect("transition width"),
            ))
        }
    }
    fn first_zone(&self) -> usize {
        if !self.indices.contains(&0) {
            return 0;
        }
        if let Some(&first) = self.indices.first()
            && self.definitions[usize::from(first)].daylight
        {
            for index in (0..usize::from(first)).rev() {
                if !self.definitions[index].daylight {
                    return index;
                }
            }
        }
        self.definitions
            .iter()
            .position(|zone| !zone.daylight)
            .unwrap_or(0)
    }
    pub(super) fn lookup(&self, seconds: i64) -> (String, i32) {
        let (name, offset, _) = self.lookup_full(seconds);
        (name, offset)
    }
    pub(super) fn lookup_full(&self, seconds: i64) -> (String, i32, bool) {
        let mut low = 0;
        let mut high = self.indices.len();
        while low < high {
            let mid = low + (high - low) / 2;
            if self.transition(mid) <= seconds {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        if low == self.indices.len()
            && let Some(recurrence) = &self.recurrence
        {
            return recurrence.lookup_full(seconds);
        }
        let index = if low == 0 {
            self.first_zone()
        } else {
            usize::from(self.indices[low - 1])
        };
        let definition = &self.definitions[index];
        (
            definition.name.clone(),
            definition.offset,
            definition.daylight,
        )
    }
    fn lookup_name(&self, name: &str, seconds: i64) -> Option<i32> {
        let mut first = None;
        for definition in &self.definitions {
            if definition.name != name {
                continue;
            }
            first.get_or_insert(definition.offset);
            let (actual, offset) = self.lookup(seconds - i64::from(definition.offset));
            if actual == name {
                return Some(offset);
            }
        }
        first
    }
}

pub(super) fn lookup_name(location: &Location, name: &str, seconds: i64) -> Option<i32> {
    match location {
        Location::Named(zone, _) => zone.lookup_name(name, seconds),
        Location::Local => super::local_zone()
            .and_then(|zone| zone.lookup_name(name, seconds))
            .or_else(|| (name == "UTC").then_some(0)),
        Location::Utc => (name == "UTC").then_some(0),
        Location::Fixed {
            name: actual,
            offset,
            ..
        } => (name == actual).then_some(*offset),
    }
}

fn counts(data: &[u8]) -> Option<[usize; 6]> {
    if data.get(..4)? != b"TZif" {
        return None;
    }
    let mut counts = [0; 6];
    for (index, count) in counts.iter_mut().enumerate() {
        *count = usize::try_from(u32::from_be_bytes(
            data.get(20 + index * 4..24 + index * 4)?.try_into().ok()?,
        ))
        .ok()?;
    }
    Some(counts)
}

/// The pinned Go archive uses stored entries, so no decompressor is required.
fn entries(mut data: &[u8]) -> Option<Vec<(&str, &[u8])>> {
    let mut entries = Vec::new();
    while data.get(..4)? == b"PK\x03\x04" {
        let word = |index: usize| {
            Some(usize::from(u16::from_le_bytes(
                data.get(index..index + 2)?.try_into().ok()?,
            )))
        };
        let dword = |index: usize| {
            usize::try_from(u32::from_le_bytes(
                data.get(index..index + 4)?.try_into().ok()?,
            ))
            .ok()
        };
        if word(8)? != 0 || dword(18)? != dword(22)? {
            return None;
        }
        let name_end = 30usize.checked_add(word(26)?)?;
        let body = name_end.checked_add(word(28)?)?;
        let end = body.checked_add(dword(18)?)?;
        let name = std::str::from_utf8(data.get(30..name_end)?).ok()?;
        entries.push((name, data.get(body..end)?));
        data = data.get(end..)?;
    }
    (data.starts_with(b"PK\x01\x02") && !entries.is_empty()).then_some(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_zone_matches_the_independent_go_ledger_in_history_recurrence_and_unix_extremes() {
        let ledger = include_str!("zoneinfo-oracle.tsv");
        let mut observations = 0;
        for row in ledger.lines().filter(|row| !row.starts_with('#')) {
            let fields = row.split('\t').collect::<Vec<_>>();
            assert2::check!(fields.len() == 4);
            let zone = load(fields[0]).unwrap();
            let seconds = fields[1].parse::<i64>().unwrap();
            let expected = (fields[3].to_string(), fields[2].parse::<i32>().unwrap());
            assert2::check!(
                zone.lookup(seconds) == expected,
                "zone={} seconds={seconds}",
                fields[0]
            );
            observations += 1;
        }
        assert2::check!(observations == 4784);
        // This ledger detects a biased timezone offset, rather than merely
        // proving that the parser accepts its own input.
        let data = entries(ARCHIVE)
            .unwrap()
            .into_iter()
            .find(|(name, _)| *name == "America/New_York")
            .unwrap()
            .1;
        let mut wrong = Zone::parse("America/New_York", data).unwrap();
        for definition in &mut wrong.definitions {
            definition.offset += 60;
        }
        assert2::check!(wrong.lookup(0) != ("EST".into(), -18000));
    }
    #[test]
    fn every_pinned_archive_entry_is_complete_and_has_valid_recurrence() {
        assert2::check!(ZONES.len() == 598);
        assert2::check!(
            load("America/Coyhaique").unwrap().lookup(1_751_328_000) == ("-03".into(), -10800)
        );
        for (name, data) in entries(ARCHIVE).unwrap() {
            assert2::check!(Zone::parse(name, data).is_some());
        }
        assert2::check!(entries(b"PK\x03\x04").is_none());
        assert2::check!(counts(b"TZif").is_none());
        let mut header = [0u8; 44];
        header[..4].copy_from_slice(b"TZif");
        header[36..40].copy_from_slice(&u32::MAX.to_be_bytes());
        assert2::check!(Zone::parse("malformed", Box::leak(Box::new(header))).is_none());
    }
}
