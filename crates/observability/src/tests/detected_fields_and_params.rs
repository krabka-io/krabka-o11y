use super::prelude::check;

trait CommonRepeatedParams {
    fn query(&self) -> &str;
    fn limit(&self) -> usize;
    fn start(&self) -> i64;
    fn end(&self) -> i64;
}

macro_rules! impl_common_repeated_params {
    ($params:ty) => {
        impl CommonRepeatedParams for $params {
            fn query(&self) -> &str {
                &self.query
            }

            fn limit(&self) -> usize {
                self.limit
            }

            fn start(&self) -> i64 {
                self.start
            }

            fn end(&self) -> i64 {
                self.end
            }
        }
    };
}

impl_common_repeated_params!(super::prelude::DetectedFieldsParams);
impl_common_repeated_params!(super::prelude::VolumeParams);

fn assert_common_repeated_fields<Params: CommonRepeatedParams>(parse: impl Fn(&str) -> Params) {
    let repeated = parse("query=a&query=b");
    check!(repeated.query() == "a");
    let repeated = parse("query=a&limit=5&limit=9");
    check!(repeated.limit() == 5);
    let repeated = parse("query=a&start=100&start=200");
    check!(repeated.start() == 100_000_000_000);
    let repeated = parse("query=a&end=500&end=900");
    check!(repeated.end() == 500_000_000_000);
}

mod a_repeated_detected_fields_parameter_keeps_the_first_value;
mod a_repeated_detected_labels_parameter_keeps_the_first_value;
mod a_repeated_log_query_parameter_keeps_the_first_value;
mod a_repeated_volume_parameter_keeps_the_first_value;
mod detected_field_types_merge_to_what_still_describes_both;
mod duration_unit_ratios;
mod every_duration_unit_maps_to_its_ordinal_and_bit;
mod logfmt_pairs_split_on_unquoted_whitespace;
mod scalar_samples_compare_as_rationals;
