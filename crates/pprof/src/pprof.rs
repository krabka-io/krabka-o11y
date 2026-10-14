//! Wrapper for the pprof wire model.

use prost::Message;

use crate::ProfileError;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use prost::Message;

    use super::*;

    fn sample_pprof() -> crate::proto::Profile {
        let mut profile = crate::proto::Profile::default();
        profile
            .sample_type
            .push(crate::proto::ValueType { r#type: 1, unit: 2 });
        profile.period_type = Some(crate::proto::ValueType { r#type: 3, unit: 4 });
        profile.sample.push(crate::proto::Sample {
            location_id: vec![1],
            value: vec![42],
            ..Default::default()
        });
        profile.string_table = ["", "cpu", "nanoseconds", "wall", "milliseconds"]
            .map(str::to_string)
            .to_vec();
        profile
    }

    #[test]
    fn decode_and_encode_round_trip_profile_bytes() {
        let bytes = sample_pprof().encode_to_vec();
        let profile = PprofProfile::decode(&bytes).unwrap();

        assert!(profile.inner() == &sample_pprof());
        assert!(
            crate::proto::Profile::decode(profile.encode().as_slice()).unwrap() == sample_pprof()
        );
    }

    #[test]
    fn invalid_bytes_report_decode_error() {
        let error = PprofProfile::decode(&[0xff]).unwrap_err();

        assert!(matches!(error, ProfileError::Decode(_)));
    }

    #[test]
    fn from_conversions_preserve_inner_profile() {
        let inner = sample_pprof();
        let profile = PprofProfile::from(inner.clone());

        assert!(crate::proto::Profile::from(profile) == inner);
    }

    #[test]
    fn accessors_return_decoded_profile_contents() {
        let inner = sample_pprof();
        let profile = PprofProfile::from(inner.clone());

        check!(profile.string(1) == Some("cpu"));
        check!(profile.string(-1).is_none());
        check!(profile.string(99).is_none());
        check!(
            profile.sample_types()
                == vec![SampleType {
                    name: "cpu".to_string(),
                    unit: "nanoseconds".to_string(),
                }]
        );
        check!(
            profile.period_type_strings()
                == SampleType {
                    name: "wall".to_string(),
                    unit: "milliseconds".to_string(),
                }
        );
        check!(
            profile.samples()
                == vec![crate::proto::Sample {
                    location_id: vec![1],
                    value: vec![42],
                    label: Vec::new(),
                }]
        );
        check!(profile.into_inner() == inner);
    }
}

mod pprof_profile;
mod profile;

pub use pprof_profile::{PprofProfile, SampleType};
