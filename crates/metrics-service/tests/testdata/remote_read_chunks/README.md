# Remote-read chunk goldens

`main.go` generates the golden bytes in
[`mimir_deployment.rs`](../../support/mimir_deployment.rs) with Prometheus's
`chunkenc` appenders. `go.mod` pins Prometheus v0.314.0. The Docker suite uses
the bytes directly and requires no Go toolchain.

To reproduce them:

```bash
cd crates/metrics-service/tests/testdata/remote_read_chunks
go run -mod=readonly .
```

The XOR chunk contains values 7, 8, and 9 at timestamps 1700000000000,
1700000001000, and 1700000002000 milliseconds. The narrowed XOR chunk contains
only the middle sample, value 8 at 1700000001000. Each histogram chunk contains
one gauge histogram at either of the first two timestamps: schema 0, count 5,
sum 8, no zero bucket, and positive bucket counts 2 and 3 for `(0.5, 1]` and
`(1, 2]`. Both integer and float histogram encodings are generated.

The test checks the full chunk protobuf and payload bytes, frame lengths,
query indexes, and CRC-32C checksums against these independently generated
inputs.
