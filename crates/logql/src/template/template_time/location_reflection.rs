//! Go1.26.5 time.Location private fields, with cache captured at load time.
//! Source: time/zoneinfo.go Location, `FixedZone`, tzset; `zoneinfo_read.go` cache.
//! Formatter goldens come from the pinned Go probe, never Rust rendering.
use super::zone::Definition;
use crate::template::{TemplateHistogramView as R, TemplateRuntimeValue as V};

#[derive(Debug)]
pub(super) struct Transition {
    pub(super) when: i64,
    pub(super) index: u8,
    pub(super) standard: bool,
    pub(super) utc: bool,
}
#[derive(Debug)]
pub(super) struct LocationData {
    pub(super) name: String,
    pub(super) definitions: Vec<Definition>,
    pub(super) transitions: Vec<Transition>,
    extend: String,
    pub(super) cache_start: i64,
    pub(super) cache_end: i64,
    pub(super) cache_index: Option<usize>,
    pub(super) cache_extra: Option<Box<Definition>>,
}
impl LocationData {
    pub(super) fn new(
        name: &str,
        definitions: Vec<Definition>,
        transitions: Vec<Transition>,
        extend: &str,
    ) -> Self {
        Self {
            name: name.into(),
            definitions,
            transitions,
            extend: extend.into(),
            cache_start: 0,
            cache_end: 0,
            cache_index: None,
            cache_extra: None,
        }
    }
    pub(super) fn empty(name: &str) -> Self {
        Self::new(name, Vec::new(), Vec::new(), "")
    }
    pub(super) fn fixed(name: &str, offset: i32) -> Self {
        let mut data = Self::new(
            name,
            vec![Definition {
                name: name.into(),
                offset,
                daylight: false,
            }],
            vec![Transition {
                when: i64::MIN,
                index: 0,
                standard: false,
                utc: false,
            }],
            "",
        );
        data.cache_start = i64::MIN;
        data.cache_end = i64::MAX;
        data.cache_index = Some(0);
        data
    }
    pub(super) fn reflection(&self, address: usize) -> R {
        let cache = self
            .cache_index
            .map(|i| &self.definitions[i])
            .or(self.cache_extra.as_deref());
        let zone_view = |zone: &Definition| {
            R::structure(
                "time.zone",
                vec![
                    ("name", R::scalar(V::String(zone.name.clone()))),
                    ("offset", R::scalar(V::Integer(i64::from(zone.offset)))),
                    (
                        "isDST",
                        R::scalar(V::Json(serde_json::Value::Bool(zone.daylight))),
                    ),
                ],
            )
        };
        let zones = R::Slice {
            name: "[]time.zone",
            values: self.definitions.iter().map(zone_view).collect(),
            is_nil: self.definitions.is_empty(),
            address: self.definitions.as_ptr() as usize,
        };
        let transitions = R::Slice {
            name: "[]time.zoneTrans",
            values: self
                .transitions
                .iter()
                .map(|tx| {
                    R::structure(
                        "time.zoneTrans",
                        vec![
                            ("when", R::scalar(V::Integer64(tx.when))),
                            ("index", R::scalar(V::Byte(tx.index))),
                            (
                                "isstd",
                                R::scalar(V::Json(serde_json::Value::Bool(tx.standard))),
                            ),
                            ("isutc", R::scalar(V::Json(serde_json::Value::Bool(tx.utc)))),
                        ],
                    )
                })
                .collect(),
            is_nil: self.transitions.is_empty(),
            address: self.transitions.as_ptr() as usize,
        };
        let object = R::structure(
            "time.Location",
            vec![
                ("name", R::scalar(V::String(self.name.clone()))),
                ("zone", zones),
                ("tx", transitions),
                ("extend", R::scalar(V::String(self.extend.clone()))),
                ("cacheStart", R::scalar(V::Integer64(self.cache_start))),
                ("cacheEnd", R::scalar(V::Integer64(self.cache_end))),
                (
                    "cacheZone",
                    R::Pointer {
                        name: "time.zone",
                        address: cache.map_or(0, |zone| std::ptr::from_ref(zone) as usize),
                        value: Box::new(
                            cache.map_or_else(|| R::structure("time.zone", Vec::new()), zone_view),
                        ),
                    },
                ),
            ],
        );
        R::Pointer {
            name: "time.Location",
            address,
            value: Box::new(object),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    #[test]
    fn fixed_and_utc_fields_match_the_independent_go_probe() {
        let utc = LocationData::empty("UTC");
        check!(utc.cache_start == 0 && utc.cache_end == 0);
        check!(
            utc.definitions.is_empty() && utc.transitions.is_empty() && utc.cache_index.is_none()
        );
        let fixed = LocationData::fixed("TEST", 19800);
        check!(fixed.definitions.len() == 1 && fixed.definitions[0].offset == 19800);
        check!(fixed.transitions[0].when == i64::MIN && fixed.transitions[0].index == 0);
        check!(
            fixed.cache_start == i64::MIN
                && fixed.cache_end == i64::MAX
                && fixed.cache_index == Some(0)
        );
        let R::Pointer { value, .. } = fixed.reflection(42) else {
            panic!("location pointer");
        };
        let R::Struct { fields, .. } = *value else {
            panic!("location fields");
        };
        check!(
            fields.iter().map(|(name, _)| *name).collect::<Vec<_>>()
                == [
                    "name",
                    "zone",
                    "tx",
                    "extend",
                    "cacheStart",
                    "cacheEnd",
                    "cacheZone"
                ]
        );
        let R::Pointer { address, .. } = &fields[6].1 else {
            panic!("cache pointer");
        };
        check!(*address == std::ptr::from_ref(&fixed.definitions[0]) as usize);
    }
    fn printf(format: &str, location: super::super::TemplateTimeLocation) -> String {
        crate::template::format_template_printf::format_template_printf_values(&[
            V::String(format.into()),
            V::TimeLocation(location),
        ])
    }
    #[test]
    fn source_location_formats_reflect_types_fields_and_real_pointer_identity() {
        use super::super::{Location, TemplateTimeLocation};
        let utc = TemplateTimeLocation(Location::Utc);
        for (format, expected) in [
            ("%v", "UTC"),
            ("%+v", "UTC"),
            ("%q", "\"UTC\""),
            ("%T", "*time.Location"),
            (
                "%#v",
                "&time.Location{name:\"UTC\", zone:[]time.zone(nil), tx:[]time.zoneTrans(nil), extend:\"\", cacheStart:0, cacheEnd:0, cacheZone:(*time.zone)(nil)}",
            ),
            ("%d", "&{%!d(string=UTC) [] [] %!d(string=) 0 0 0}"),
        ] {
            check!(printf(format, utc.clone()) == expected);
        }
        check!(printf("%p", utc.clone()) == format!("0x{:x}", utc.address()));
        check!(utc.address() != TemplateTimeLocation(Location::Local).address());
        let fixed = TemplateTimeLocation(Location::fixed("TEST".into(), 19800));
        let Location::Fixed { identity, .. } = &fixed.0 else {
            panic!("fixed location");
        };
        let cache = std::ptr::from_ref(&identity.definitions[0]) as usize;
        check!(
            printf("%#v", fixed.clone())
                == format!(
                    "&time.Location{{name:\"TEST\", zone:[]time.zone{{time.zone{{name:\"TEST\", offset:19800, isDST:false}}}}, tx:[]time.zoneTrans{{time.zoneTrans{{when:-9223372036854775808, index:0x0, isstd:false, isutc:false}}}}, extend:\"\", cacheStart:-9223372036854775808, cacheEnd:9223372036854775807, cacheZone:(*time.zone)(0x{cache:x})}}"
                )
        );
        check!(printf("%p", fixed.clone()) == format!("0x{:x}", fixed.address()));
        check!(
            printf("%d", fixed)
                == format!(
                    "&{{%!d(string=TEST) [{{%!d(string=TEST) 19800 %!d(bool=false)}}] [{{-9223372036854775808 0 %!d(bool=false) %!d(bool=false)}}] %!d(string=) -9223372036854775808 9223372036854775807 {cache}}}"
                )
        );
    }
    #[test]
    fn go_fixed_zone_singletons_and_separately_loaded_zones_have_correct_aliasing() {
        use super::super::{Location, TemplateTimeLocation};
        let left = TemplateTimeLocation(Location::fixed(String::new(), 3600));
        let same = TemplateTimeLocation(Location::fixed(String::new(), 3600));
        let different = TemplateTimeLocation(Location::fixed(String::new(), 7200));
        check!(left == same && left.address() == same.address());
        check!(left != different && left.address() != different.address());
        let named = TemplateTimeLocation(Location::fixed("named".into(), 3600));
        let named_again = TemplateTimeLocation(Location::fixed("named".into(), 3600));
        check!(named != named_again && named.address() != named_again.address());
        let odd = TemplateTimeLocation(Location::fixed(String::new(), 3601));
        let odd_again = TemplateTimeLocation(Location::fixed(String::new(), 3601));
        check!(odd != odd_again);
        for offset in [-12 * 3600, 14 * 3600] {
            check!(
                Location::fixed(String::new(), offset) == Location::fixed(String::new(), offset)
            );
        }
        for offset in [-13 * 3600, 15 * 3600] {
            check!(
                Location::fixed(String::new(), offset) != Location::fixed(String::new(), offset)
            );
        }
        let a = TemplateTimeLocation(Location::load("America/New_York"));
        let b = TemplateTimeLocation(Location::load("America/New_York"));
        check!(a != b && a.address() != b.address());
        let (Location::Named(_, a), Location::Named(_, b)) = (&a.0, &b.0) else {
            panic!("named zone");
        };
        check!(a.definitions.as_ptr() != b.definitions.as_ptr());
    }
    #[test]
    fn named_cache_is_load_anchored_and_southern_recurrence_has_source_bounds() {
        let ny = super::super::zone::load("America/New_York").unwrap();
        let loaded = ny.reflection_data(1_780_000_000);
        check!(loaded.cache_start == 1_772_953_200 && loaded.cache_end == 1_793_512_800);
        check!(ny.lookup(4_102_444_800) == ("EST".into(), -18000));
        check!(loaded.cache_start == 1_772_953_200 && loaded.cache_end == 1_793_512_800);
        let future = ny.reflection_data(4_102_444_800);
        check!(future.cache_start == 4_102_444_800 && future.cache_end == 4_108_690_800);
        let before = ny.reflection_data(4_108_690_799);
        let boundary = ny.reflection_data(4_108_690_800);
        check!(before.cache_end == 4_108_690_800);
        check!(boundary.cache_start == 4_108_690_800 && boundary.cache_end == 4_129_250_400);
        check!(!before.definitions[before.cache_index.unwrap()].daylight);
        check!(boundary.definitions[boundary.cache_index.unwrap()].daylight);
        let sydney = super::super::zone::load("Australia/Sydney")
            .unwrap()
            .reflection_data(4_102_444_800);
        check!(sydney.cache_start == 4_102_444_800 && sydney.cache_end == 4_110_451_200);
    }
}
