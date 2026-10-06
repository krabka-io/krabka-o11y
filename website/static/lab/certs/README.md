# Browser lab trust roots

`ca.pem` contains all 121 complete X.509 root certificates from the locked
`webpki-root-certs` 1.0.9 dependency. The crate derives Mozilla's trusted root
store from the Common CA Database (CCADB). The lab mounts this bundle and sets
`SSL_CERT_FILE` so the production services can load real trust roots in WASIX.

Source: [rustls/webpki-roots, webpki-root-certs](https://github.com/rustls/webpki-roots/tree/0a553dbc8b3f18ea05c4f881cffa3f2d005d0d30/webpki-root-certs).
The original [crate archive](https://static.crates.io/crates/webpki-root-certs/webpki-root-certs-1.0.9.crate)
checksum matches the repository's native `Cargo.lock`.

| Item | Pinned value |
| --- | --- |
| Crate | `webpki-root-certs` 1.0.9 |
| Source revision | `0a553dbc8b3f18ea05c4f881cffa3f2d005d0d30` |
| Crate archive SHA-256 | `b96554aa2acc8ccdb7e1c9a58a7a68dd5d13bccc69cd124cb09406db612a1c9b` |
| Bundle SHA-256 | `a8e00c3793f619a1b7a6cd50211ec0081836f83cef4b237e8cb52876e72da9c2` |
| Certificate count | 121 |
| Bundle size | 181,603 bytes |
| License | CDLA-Permissive-2.0; upstream text in [LICENSE](LICENSE) |

The conversion reads every `CertificateDer::from_slice` literal in `src/lib.rs`,
requires each byte to use a `\xHH` escape, and rejects missing or duplicate
certificates. It preserves their order and encodes the original DER bytes as
64-column PEM with a trailing newline after each certificate.

Verified with OpenSSL 3.5.5: all 121 original DER certificates and generated PEM
certificates round-trip to identical DER; every root verifies against this
bundle with self-signed signature checking. That signature check excludes
certificate validity times so it tests the pinned source data independently
of the verification date. Services retain their normal TLS certificate
validation during connections.
