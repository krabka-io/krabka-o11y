use super::{encode_xor_chunk::BitStream, v1};

pub(super) fn encode_histogram_chunks(histograms: &[v1::Histogram]) -> Vec<v1::Chunk> {
    let mut histograms = histograms.to_vec();
    histograms.sort_by_key(|histogram| histogram.timestamp);
    histograms.iter().map(encode_histogram_chunk).collect()
}

fn encode_histogram_chunk(histogram: &v1::Histogram) -> v1::Chunk {
    let is_float = matches!(histogram.count, Some(v1::histogram::Count::CountFloat(_)));
    let mut bits = BitStream::new(1);
    bits.write_byte(reset_header(histogram.reset_hint));
    write_layout(&mut bits, histogram);
    put_varbit_int(&mut bits, histogram.timestamp);
    if is_float {
        let count = match histogram.count {
            Some(v1::histogram::Count::CountFloat(count)) => count,
            _ => 0.0,
        };
        let zero_count = match histogram.zero_count {
            Some(v1::histogram::ZeroCount::ZeroCountFloat(count)) => count,
            _ => 0.0,
        };
        bits.write_bits(count.to_bits(), 64);
        bits.write_bits(zero_count.to_bits(), 64);
        bits.write_bits(histogram.sum.to_bits(), 64);
        for count in histogram
            .positive_counts
            .iter()
            .chain(&histogram.negative_counts)
        {
            bits.write_bits(count.to_bits(), 64);
        }
    } else {
        let count = match histogram.count {
            Some(v1::histogram::Count::CountInt(count)) => count,
            _ => 0,
        };
        let zero_count = match histogram.zero_count {
            Some(v1::histogram::ZeroCount::ZeroCountInt(count)) => count,
            _ => 0,
        };
        put_varbit_uint(&mut bits, count);
        put_varbit_uint(&mut bits, zero_count);
        bits.write_bits(histogram.sum.to_bits(), 64);
        for delta in histogram
            .positive_deltas
            .iter()
            .chain(&histogram.negative_deltas)
        {
            put_varbit_int(&mut bits, *delta);
        }
    }
    v1::Chunk {
        min_time_ms: histogram.timestamp,
        max_time_ms: histogram.timestamp,
        r#type: if is_float {
            v1::chunk::Encoding::FloatHistogram as i32
        } else {
            v1::chunk::Encoding::Histogram as i32
        },
        data: bits.bytes,
    }
}

fn reset_header(reset_hint: i32) -> u8 {
    match v1::histogram::ResetHint::try_from(reset_hint) {
        Ok(v1::histogram::ResetHint::Yes) => 0x80,
        Ok(v1::histogram::ResetHint::No) => 0x40,
        Ok(v1::histogram::ResetHint::Gauge) => 0xc0,
        _ => 0,
    }
}

fn write_layout(bits: &mut BitStream, histogram: &v1::Histogram) {
    if histogram.zero_threshold == 0.0 {
        bits.write_byte(0);
    } else {
        bits.write_byte(0xff);
        bits.write_bits(histogram.zero_threshold.to_bits(), 64);
    }
    put_varbit_int(bits, i64::from(histogram.schema));
    write_spans(bits, &histogram.positive_spans);
    write_spans(bits, &histogram.negative_spans);
    if histogram.schema == -53 {
        put_varbit_uint(
            bits,
            u64::try_from(histogram.custom_values.len()).expect("custom bound count fits in u64"),
        );
        for bound in &histogram.custom_values {
            bits.write_bit(false);
            bits.write_bits(bound.to_bits(), 64);
        }
    }
}

fn write_spans(bits: &mut BitStream, spans: &[v1::BucketSpan]) {
    put_varbit_uint(
        bits,
        u64::try_from(spans.len()).expect("span count fits in u64"),
    );
    for span in spans {
        put_varbit_uint(bits, u64::from(span.length));
        put_varbit_int(bits, i64::from(span.offset));
    }
}

fn put_varbit_int(bits: &mut BitStream, value: i64) {
    if value == 0 {
        bits.write_bit(false);
    } else if fits_signed(value, 3) {
        bits.write_bits(0b0000_0010, 2);
        bits.write_bits(value.cast_unsigned(), 3);
    } else if fits_signed(value, 6) {
        bits.write_bits(0b0000_0110, 3);
        bits.write_bits(value.cast_unsigned(), 6);
    } else if fits_signed(value, 9) {
        bits.write_bits(0b0000_1110, 4);
        bits.write_bits(value.cast_unsigned(), 9);
    } else if fits_signed(value, 12) {
        bits.write_bits(0b0001_1110, 5);
        bits.write_bits(value.cast_unsigned(), 12);
    } else if fits_signed(value, 18) {
        bits.write_bits(0b0011_1110, 6);
        bits.write_bits(value.cast_unsigned(), 18);
    } else if fits_signed(value, 25) {
        bits.write_bits(0b0111_1110, 7);
        bits.write_bits(value.cast_unsigned(), 25);
    } else if fits_signed(value, 56) {
        bits.write_bits(0b1111_1110, 8);
        bits.write_bits(value.cast_unsigned(), 56);
    } else {
        bits.write_bits(0xff, 8);
        bits.write_bits(value.cast_unsigned(), 64);
    }
}

fn put_varbit_uint(bits: &mut BitStream, value: u64) {
    if value == 0 {
        bits.write_bit(false);
    } else if fits_unsigned(value, 3) {
        bits.write_bits(0b0000_0010, 2);
        bits.write_bits(value, 3);
    } else if fits_unsigned(value, 6) {
        bits.write_bits(0b0000_0110, 3);
        bits.write_bits(value, 6);
    } else if fits_unsigned(value, 9) {
        bits.write_bits(0b0000_1110, 4);
        bits.write_bits(value, 9);
    } else if fits_unsigned(value, 12) {
        bits.write_bits(0b0001_1110, 5);
        bits.write_bits(value, 12);
    } else if fits_unsigned(value, 18) {
        bits.write_bits(0b0011_1110, 6);
        bits.write_bits(value, 18);
    } else if fits_unsigned(value, 25) {
        bits.write_bits(0b0111_1110, 7);
        bits.write_bits(value, 25);
    } else if fits_unsigned(value, 56) {
        bits.write_bits(0b1111_1110, 8);
        bits.write_bits(value, 56);
    } else {
        bits.write_bits(0xff, 8);
        bits.write_bits(value, 64);
    }
}

fn fits_signed(value: i64, width: u32) -> bool {
    -((1_i64 << (width - 1)) - 1) <= value && value <= 1_i64 << (width - 1)
}

fn fits_unsigned(value: u64, width: u32) -> bool {
    value.leading_zeros() >= 64 - width
}
