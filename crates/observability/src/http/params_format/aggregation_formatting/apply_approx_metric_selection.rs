//! Loki 3.7.7 `count_min_sketch.go` and `sketch/{cms,hash}.go`: a fixed-size
//! count-min sketch and a bounded label heap, followed by ordinary top-k.
use std::collections::BTreeSet;

use serde_json::Value;

use crate::http::handlers::query_execution::format_float_sample;

const WIDTH: usize = 27_183;
const DEPTH: usize = 7;

pub(crate) fn apply_approx_metric_selection(value: &mut Value, limit: usize, heap_size: usize) {
    let Some(series) = value["data"]["result"].as_array_mut() else {
        return;
    };
    let mut sketch = Sketch::new(WIDTH, DEPTH);
    let mut heap: Vec<(Vec<u8>, Value)> = Vec::new();
    let mut retained = BTreeSet::new();
    for sample in std::mem::take(series) {
        let bytes = label_bytes(&sample["metric"]);
        let Some(count) = sample["value"][1].as_str().and_then(parse_float) else {
            continue;
        };
        sketch.add(&bytes, count);
        if retained.insert(bytes.clone()) {
            heap.push((bytes, sample));
            sift_up(&mut heap, &sketch);
        } else if heap.first().is_some_and(|(key, _)| key == &bytes) {
            sift_down(&mut heap, &sketch);
        }
        if heap.len() > heap_size {
            retained.remove(&heap.swap_remove(0).0);
            sift_down(&mut heap, &sketch);
        }
    }
    let mut candidates = heap
        .into_iter()
        .map(|(bytes, mut sample)| {
            let count = sketch.count(&bytes);
            sample["value"][1] = Value::String(format_float_sample(count));
            (count, bytes, sample)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    series.extend(
        candidates
            .into_iter()
            .take(limit)
            .map(|(_, _, sample)| sample),
    );
}

fn parse_float(text: &str) -> Option<f64> {
    match text {
        "+Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        _ => text.parse().ok(),
    }
}

fn label_bytes(labels: &Value) -> Vec<u8> {
    let mut bytes = vec![0xfe];
    if let Some(labels) = labels.as_object() {
        // serde_json can use insertion order when preserve_order is enabled.
        let mut labels = labels.iter().collect::<Vec<_>>();
        labels.sort_unstable_by_key(|(name, _)| *name);
        for (i, (name, value)) in labels.into_iter().enumerate() {
            if i != 0 {
                bytes.push(0xff);
            }
            bytes.extend_from_slice(name.as_bytes());
            bytes.push(0xff);
            bytes.extend_from_slice(value.as_str().unwrap_or_default().as_bytes());
        }
    }
    bytes
}

struct Sketch {
    width: usize,
    counters: Vec<Vec<f64>>,
}

impl Sketch {
    fn new(width: usize, depth: usize) -> Self {
        Self {
            width,
            counters: vec![vec![0.0; width]; depth],
        }
    }
    fn add(&mut self, bytes: &[u8], count: f64) {
        let (first, second) = hashes(bytes);
        for (row, counters) in self.counters.iter_mut().enumerate() {
            let position = sketch_position(first, second, row, self.width);
            counters[position] += count;
        }
    }
    fn count(&self, bytes: &[u8]) -> f64 {
        let (first, second) = hashes(bytes);
        // Go's source deliberately starts at float64(math.MaxUint64).
        let mut result = 18_446_744_073_709_551_616.0;
        for (row, counters) in self.counters.iter().enumerate() {
            let position = sketch_position(first, second, row, self.width);
            if counters[position] < result {
                result = counters[position];
            }
        }
        result
    }
}

fn sketch_position(first: u32, second: u32, row: usize, width: usize) -> usize {
    let row = u32::try_from(row).expect("count-min sketch depth fits u32");
    usize::try_from(first.wrapping_add(row.wrapping_mul(second)))
        .expect("u32 sketch hash fits usize")
        % width
}

fn hashes(bytes: &[u8]) -> (u32, u32) {
    let mut first = 2_166_136_261_u32;
    let mut second = 0_u32;
    for byte in bytes {
        first = (first ^ u32::from(*byte)).wrapping_mul(16_777_619);
        second = second.wrapping_add(u32::from(*byte));
        second = second.wrapping_add(second << 10);
        second ^= second >> 6;
    }
    second = second.wrapping_add(second << 3);
    second ^= second >> 11;
    second = second.wrapping_add(second << 15);
    (first, second)
}

fn sift_up(heap: &mut [(Vec<u8>, Value)], sketch: &Sketch) {
    let mut index = heap.len() - 1;
    while index != 0 {
        let parent = (index - 1) / 2;
        if sketch.count(&heap[index].0) >= sketch.count(&heap[parent].0) {
            break;
        }
        heap.swap(index, parent);
        index = parent;
    }
}

fn sift_down(heap: &mut [(Vec<u8>, Value)], sketch: &Sketch) {
    let mut index = 0_usize;
    while let Some(left) = index
        .checked_mul(2)
        .and_then(|i| i.checked_add(1))
        .filter(|i| *i < heap.len())
    {
        let right = left + 1;
        let child =
            if right < heap.len() && sketch.count(&heap[right].0) < sketch.count(&heap[left].0) {
                right
            } else {
                left
            };
        if sketch.count(&heap[child].0) >= sketch.count(&heap[index].0) {
            break;
        }
        heap.swap(index, child);
        index = child;
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use serde_json::json;

    use super::*;

    #[test]
    fn pinned_go_sketch_positions_and_collision_estimates() {
        // Captured by calling NewCountMinSketch/Add/Count in the unmodified
        // Loki 7a40404f32b3e6464c9cfc6cc7dd75a40f3931da vendor graph,
        // with Go 1.26.5. Expectations do not use our hash implementation.
        let positions: &[(&[u8], [usize; DEPTH])] = &[
            (b"", [4540; DEPTH]),
            (b"abc", [12071, 22801, 5278, 16008, 26738, 10285, 21015]),
            (
                b"\xfejob\xffapi",
                [13119, 15199, 17279, 19359, 21439, 23519, 25599],
            ),
            (
                b"\xfejob\xff\xff\x00",
                [7107, 5749, 4391, 4103, 2745, 2457, 1099],
            ),
        ];
        for (key, expected) in positions {
            let mut sketch = Sketch::new(WIDTH, DEPTH);
            sketch.add(key, 3.0);
            let actual = sketch
                .counters
                .iter()
                .map(|row| {
                    row.iter()
                        .position(|value| value.to_bits() == 3.0_f64.to_bits())
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert!(actual == expected.to_vec());
            assert!(sketch.count(key).to_bits() == 3.0_f64.to_bits());
        }
        let mut collision = Sketch::new(3, 2);
        for (key, value) in [(b"a", 3.0), (b"b", 7.0), (b"c", 11.0), (b"a", 5.0)] {
            collision.add(key, value);
        }
        assert!(collision.counters == vec![vec![0.0, 15.0, 11.0], vec![15.0, 11.0, 0.0]]);
        assert!(collision.count(b"a").to_bits() == 15.0_f64.to_bits());
        assert!(collision.count(b"b").to_bits() == 15.0_f64.to_bits());
        assert!(collision.count(b"c").to_bits() == 11.0_f64.to_bits());
        // Using exact counts instead of the sketch would incorrectly return 8.
        assert!(collision.count(b"a").to_bits() != 8.0_f64.to_bits());
    }

    #[test]
    fn approximation_keeps_labels_values_timestamps_and_bounds_the_heap() {
        let original = json!({"status":"success","data":{"resultType":"vector","result":[
            {"metric":{"job":"small"},"value":[123,"3"]},
            {"metric":{"job":"large"},"value":[123,"11"]},
            {"metric":{"job":"middle"},"value":[123,"7"]}
        ]}});
        let mut response = original.clone();
        apply_approx_metric_selection(&mut response, 3, 2);
        assert!(
            response["data"]["result"]
                == json!([
                    {"metric":{"job":"large"},"value":[123,"11"]},
                    {"metric":{"job":"middle"},"value":[123,"7"]}
                ])
        );
        let mut empty = original;
        apply_approx_metric_selection(&mut empty, 3, 0);
        assert!(empty["data"]["result"] == json!([]));
    }

    #[test]
    fn repeated_labels_update_the_sketch_without_duplicate_heap_entries() {
        let mut response = json!({"data":{"result":[
            {"metric":{"a":"one"},"value":[123,"3"]},
            {"metric":{"a":"two"},"value":[123,"7"]},
            {"metric":{"a":"one"},"value":[123,"8"]}
        ]}});
        apply_approx_metric_selection(&mut response, 2, 2);
        assert!(
            response["data"]["result"]
                == json!([
                    {"metric":{"a":"one"},"value":[123,"11"]},
                    {"metric":{"a":"two"},"value":[123,"7"]}
                ])
        );
        assert!(label_bytes(&json!({"b":"two", "a":"one"})) == b"\xfea\xffone\xffb\xfftwo");
    }
}
