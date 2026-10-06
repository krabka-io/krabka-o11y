The embedded `zoneinfo.zip` is copied unchanged from Go 1.26.5:
https://github.com/golang/go/blob/go1.26.5/lib/time/zoneinfo.zip
SHA256: `8f55634d05f8bca1f7bc7c69c5933428c69357e0bdf565e5ba224e3f88ff12e8`.

Go's pinned `lib/time/update.bash` identifies its IANA code and data version as
2025c. The archive contains 598 stored ZIP entries. The IANA timezone database
is public domain, as described in the upstream `lib/time/README`:
https://github.com/golang/go/blob/go1.26.5/lib/time/README

Transition selection, pre-transition selection and POSIX recurrence follow
Go 1.26.5 `src/time/zoneinfo.go` and `src/time/zoneinfo_read.go`. The Go copyright
and BSD license are preserved in `LICENSE-GO`. These are independently
implemented parsers, without copied generated Rust timezone tables.

Pinned data supplies named-zone offsets and historical abbreviation definitions
without depending on OS tzdata.

`zoneinfo-oracle.tsv` is an independent Go `time.LoadLocationFromTZData` /
`Time.Zone` ledger for all 598 locations at eight instants: i64 Unix limits,
year zero, Unix epoch, 2024 winter/summer, and 2100 winter/summer. The far-future
instants exercise POSIX recurrence beyond stored transitions; the earliest
instants exercise initial-zone selection. It contains 4784 observations.
