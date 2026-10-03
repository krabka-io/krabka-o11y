# Prometheus TSDB block fixture

The block `01M3MJXM7R4M5X4Q4CKHW5Q8N0/` is a real block that Prometheus wrote. The TSDB import tests decode it and compare the result with `expected_samples.json.gz`, which the Prometheus TSDB library read from the same block.

## Provenance

- Writer: the `prom/prometheus:v3.14.0` image. The arm64 image has the digest `sha256:2d25f68eb7aa2e654dadd83b45e5757259b32a1e4b6bc370f1eec927f491265b`, from the manifest list `sha256:5ce7540c3c00ef4ab0c9d2c995c6a5b9c421f44b4a115d97a2c7af3b1c21cbb0`. `MODULE.bazel` pins the amd64 image of the same list, `sha256:e906cef998316bbe319f98711e1b4d8613ad37e14b08ff831d7036e77b7464f9`.
- Dataset: `generator/main.go`. It sends 840 samples per series at a 15 s step from 2025-06-15T14:00:00Z over remote write, so the head compacts the first two hours into this block.
- Reference decoder: `dumper/main.go`, built against `github.com/prometheus/prometheus v0.307.0`. It opens the block with `tsdb.OpenBlock`, applies the tombstones, and prints every sample. Float values and histogram fields are the hex bits of the f64. Histograms come from `AtFloatHistogram`, so the bucket counts are absolute.

The block holds:

| Series | Samples | Features |
| --- | --- | --- |
| `imported_counter_total{instance="i1"}` | 480 floats | A counter reset and a stale marker |
| `imported_gauge{instance="i1"}` | 480 floats | Jittered timestamps, `-1e300` values |
| `imported_gauge{instance="zürich"}` | 480 floats | A non-ASCII label value |
| `imported_deleted` | 419 floats | A tombstone deletes 61 samples |
| `imported_hist` | 480 integer histograms | Schema 3, positive and negative spans, a counter reset, a stale marker |
| `imported_float_hist` | 480 float histograms | A gauge histogram and a stale marker |
| `imported_nhcb` | 480 integer histograms | Custom buckets (schema -53) |

## Commands

```bash
cat > prometheus.yml <<'EOF'
global: {}
EOF
docker run -d --name tsdbfix --platform linux/arm64 -p 19090:9090 \
  -v "$PWD/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  prom/prometheus:v3.14.0 --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus --web.enable-remote-write-receiver \
  --web.enable-admin-api --enable-feature=native-histograms \
  --storage.tsdb.retention.time=1000d
(cd generator && go run . http://127.0.0.1:19090)
# Wait until the head compacts the first two hours (about a minute).
curl -XPOST http://127.0.0.1:19090/api/v1/admin/tsdb/delete_series \
  --data-urlencode 'match[]=imported_deleted' \
  --data-urlencode start=1749997800 --data-urlencode end=1749998700
docker cp tsdbfix:/prometheus/<ulid> blk
(cd dumper && go run . ../blk) | gzip -9n > expected_samples.json.gz
```

Prometheus names the block with a new ULID on each run. The checked-in block keeps the ULID of the run that produced it.

## Digests

| File | sha256 |
| --- | --- |
| `01M3MJXM7R4M5X4Q4CKHW5Q8N0/index` | `3b9a6bba29fd441568f75f6d4d245483fbc3375ee10b4f169d5df994bbe6f882` |
| `01M3MJXM7R4M5X4Q4CKHW5Q8N0/chunks/000001` | `9e96be1b34ee5e8a63576fb3fdda7948cfc4aab7e27b3da76078c77e7cf128e4` |
| `01M3MJXM7R4M5X4Q4CKHW5Q8N0/tombstones` | `6d6d94394c53586609710873baa4e3edaf66ab9776e139414ad4ce9ea5cf3f85` |
| `01M3MJXM7R4M5X4Q4CKHW5Q8N0/meta.json` | `0a860067132282bcd07a42cd5ffed6494d097957e584e8c9954404c5c29e22bb` |
| `expected_samples.json.gz` | `4055f8da535e89aee4b37db72600d94dc6c1d51a72af75c3c387da21208a5eb9` |
