# Prometheus 3.14.0 qualification corpus

All 21 `.test` files are copied from `prometheus/prometheus`, revision
`d7598b7141418fa35be2b5ec5d0fefb634199610`, `promql/promqltest/testdata`.
Apache-2.0; original headers retained, trailing whitespace removed and leading indentation normalized.

No cases are excluded or annotated as expected Krabka divergences. Both build
configurations report all declared evaluations; feature-disabled cases remain
in the denominator. A completed execution is not complete conformance: inspect
`complete_corpus` and every case status in the JSON report.
