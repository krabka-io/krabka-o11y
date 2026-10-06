# krabka-o11y website

The product site, complete source documentation, and observability lab publish at
<https://krabka.io/krabka-o11y/> through GitHub Pages. Every local route uses the
same `/krabka-o11y/` base. The main krabka site links to this project site.

Use Node 22.12 or newer, npm, Python 3.11 or newer, Bash, Git, rustup, the native
Rust version in `../rust-toolchain.toml`, and native C/C++ build tools. Linux x86_64
is the current WASIX qualification platform. Run these commands from `website/`:

```sh
npm ci
npm run dev          # http://127.0.0.1:4321/krabka-o11y/
npm run build:site   # documentation and frontend only
npm run build:lab    # real WASIX services and browser runtime
npm run build        # lab, then documentation and frontend
npm run preview      # preview the existing dist/ build
npm test
npm run check        # validate the built site's routes, anchors, and assets
```

Preview the built lab with the ordinary static server:

```sh
node scripts/preview.mjs --port 4322
```

Open <http://127.0.0.1:4322/krabka-o11y/lab/>. The browser needs `SharedArrayBuffer`, WebAssembly threads, and cross-origin isolation. The lab's scoped worker supplies the isolation policy on localhost and GitHub Pages. Run the browser checks from another terminal with the preview server active. Use the ordinary preview without `--isolate` to check the real worker:

```sh
python3 -B scripts/prepare-wasix-test.py
node scripts/browser-check.mjs --url http://127.0.0.1:4322/krabka-o11y/
node scripts/network-check.mjs
node scripts/lab-check.mjs
```

Build the lab and site before the two runtime checks. `network-check.mjs` runs the compiled WASIX network guest. It checks real TCP port allocation and byte exchange, independent UDP ports, packet boundaries, zero-length datagrams, and timeouts. It also checks Tokio nonblocking reads, repeated framed exchanges, concurrent timers, contended locks, command deadlines, and cleanup with a blocked atomic waiter. `lab-check.mjs` starts the UI stack, sends telemetry, and checks actual metrics, logs, traces, and profile results. It also checks tenant isolation, API errors, shutdown, and a complete restart with empty data before it can pass.

Edit the original files under `../docs/`, crate READMEs, and `guides/`. Site
preparation imports the complete documents, updates their links, and copies HTTP
API inventories. Generated `content/`, `generated-static/`, `dist/`, SDK files,
WASM modules, and `.tools/` are ignored; they are build outputs.

The site's `/krabka-o11y/api/` route is the curated API landing page. The workspace rustdoc tree keeps its existing crate and source routes: `/krabka-o11y/krabka_<crate>/index.html` and `/krabka-o11y/src/...`. The combined Pages artifact includes the complete rustdoc tree at its root. Copy rustdoc first, then copy `dist/` onto it so the site supplies the homepage and API landing page. The crate pages, source pages, and rustdoc assets keep their original paths.

The existing CI `rustdoc` artifact supplies the complete workspace tree. To build and preview the combined artifact locally, first build the site, then run:

```sh
cd ..
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked
cp docs/rustdoc_index.html target/doc/index.html
mkdir -p website/.tools/site-combined
cp -R target/doc/. website/.tools/site-combined/
cp -R website/dist/. website/.tools/site-combined/
cd website
npm run check -- .tools/site-combined --rustdoc
node scripts/preview.mjs --dir .tools/site-combined --port 4322
```

Use a fresh combined output directory for each build. The site checker accepts the directory as a positional argument. Its `--rustdoc` flag checks the crate routes in the combined artifact. The default check skips those routes because `dist/` contains only the site.

The lab runs the real broker, storage formatter, WAL topic bootstrap, metrics
pipeline and Prometheus service, logs, traces, and profiles. HTTP requests travel
through a compiled guest client to the production listeners in the sandbox.
Telemetry samples enter those services through their ingestion APIs. Filesystem
state and virtual TCP connections belong to the browser sandbox.

The sandbox mounts the 121 Mozilla roots from the locked `webpki-root-certs`
1.0.9 dependency and sets `SSL_CERT_FILE`. This supplies the certificate store
that WASIX services need when they create their HTTP clients. The lab checks
the bundle's SHA-256 before mounting it. Its source, conversion, and license
are recorded in [`static/lab/certs/README.md`](static/lab/certs/README.md).

`scripts/build-lab.sh` first obtains locked native Cargo metadata, then stages
workspace, git, and selected registry sources in `.tools/wasi-source/`. The
native source tree and lockfile, including cached crate sources, are preserved. WASIX
dependency and platform adaptations apply to that independent copy; the
object_store 0.13 and 0.14 APIs remain distinct.
Staging preserves unchanged files and gives changed content a fresh modification
time, so incremental Cargo builds cannot reuse an older compiled guest.

The build restores the qualified service, guest, standard-library, and SDK
dependency locks from `wasi/` after their platform adaptations. Compiled graphs
use `--locked`. Publication checks that all four locks still match their
snapshots and records their hashes in the manifest.

The staged LZ4 binding uses the return type in its bundled C header. WASIX
builds reject linker warnings, including incompatible function signatures.
The staged WASIX Mio constructor sets its required nonblocking flag so an
empty async read returns `WouldBlock` and leaves Tokio timers runnable.
The staged parking_lot_core uses its existing atomic wait/notify backend on
WASIX, supporting contended locks and timed waits.

The build pins cargo-wasix 0.1.34, wasixcc 0.4.7, WASIX Rust `v2026-09-18.1+rust-1.97`, libc sysroot `v2026-10-02.1`, LLVM `21.1.206`, and Binaryen `version_133`. Installations and downloads stay under `.tools/`. Rustup's `wasix` alias points to the pinned compiler.

`scripts/prepare-wasix-std.py` creates the private compiler tree `.tools/wasix-rust/`. It imports Rust standard-library source from WASIX revision `706d63224a7ce7be61ceb9d0525fe942d99a93df`. It applies `wasi/wasix-std-network.patch` there so socket addresses reflect the actual bound ports. The build uses a private rustup alias and `-Zbuild-std=std,panic_abort`. The installed standard-library source stays unchanged. The native MSRV stays at Rust 1.98.1. Only the WASIX build uses `--ignore-rust-version`; the actual build and browser checks establish compiler and API compatibility.

`scripts/build-runtime.sh` pins Wasmer SDK 0.19.0 at `362e0db28fea57fb23af8d34cffefcb7333a883d` and Wasmer core at `5bd2b7d3182e816bd0ea330acc88e4e4733e167f`. The private SDK tree receives `wasi/wasmer-network.patch` for TCP port allocation, local UDP packets, and a dedicated async worker that keeps command deadlines runnable while guest workers block. Closing the SDK stops its workers, including workers blocked in native atomic waits. The private core tree receives `wasi/wasmer-address.patch` for the guest socket address ABI and `wasi/wasmer-udp-timeout.patch` for UDP timeouts. The runtime build records these revisions, patch hashes, and the compiled runtime hash. `network-check.mjs` checks these adapters with actual guest socket operations.

```sh
CARGO_BUILD_JOBS=4 npm run build:lab
LAB_OPT_LEVEL=s LAB_LTO=thin npm run build:lab  # qualify this size profile separately
```

The default release profile uses size optimization and retains the toolchain's
panic behavior. The manifest records the source revision, dirty state, dependency
lock and build-script hashes, pinned runtime, and each module's SHA-256 and byte
count, plus every platform patch hash. It is published only after all ten modules and the browser runtime exist;
a failed build removes the ready manifest. Cached tools do not replace builds.

Local Linux x86_64 qualification passed in Chromium. All ten WASIX modules
compiled with the qualified locks. Browser checks passed actual ingestion and
expected query results for every signal, tenant isolation, API error display,
canceled startup, shutdown, and a fresh restart with empty data. The recorded
evidence is local; CI and GitHub Pages deployment are pending.
