#!/usr/bin/env bash
set -euo pipefail

WEBSITE_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
TOOLS_DIR="$WEBSITE_DIR/.tools"
SDK_DIR="$TOOLS_DIR/wasmer-sdk"
SDK_REV=362e0db28fea57fb23af8d34cffefcb7333a883d
SDK_NIGHTLY=nightly-2026-08-14
WASMER_REV=5bd2b7d3182e816bd0ea330acc88e4e4733e167f
WASMER_DIR="$TOOLS_DIR/wasmer"
BINDGEN_VERSION=0.2.126
PATCH_FILE="$WEBSITE_DIR/wasi/wasmer-network.patch"
export TMPDIR="$TOOLS_DIR/tmp"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}"
mkdir -p "$TMPDIR" "$TOOLS_DIR/wasix"

if [[ ! -d "$SDK_DIR/.git" ]]; then
  git clone --filter=blob:none --no-checkout https://github.com/wasmerio/wasmer-sdk.git "$SDK_DIR"
  git -C "$SDK_DIR" checkout --detach "$SDK_REV"
fi
[[ $(git -C "$SDK_DIR" rev-parse HEAD) == "$SDK_REV" ]] || {
  echo 'The private SDK checkout has a different revision; use a fresh .tools/wasmer-sdk directory.' >&2
  exit 1
}
if git -C "$SDK_DIR" apply --check "$PATCH_FILE" 2>/dev/null; then
  git -C "$SDK_DIR" apply "$PATCH_FILE"
else
  git -C "$SDK_DIR" apply --reverse --check "$PATCH_FILE"
fi

if [[ ! -d "$WASMER_DIR/.git" ]]; then
  git clone --filter=blob:none --no-checkout https://github.com/wasmerio/wasmer.git "$WASMER_DIR"
  git -C "$WASMER_DIR" checkout --detach "$WASMER_REV"
fi
[[ $(git -C "$WASMER_DIR" rev-parse HEAD) == "$WASMER_REV" ]]
git -C "$WASMER_DIR" submodule update --init --depth 1 lib/napi
ADDRESS_PATCH="$WEBSITE_DIR/wasi/wasmer-address.patch"
UDP_PATCH="$WEBSITE_DIR/wasi/wasmer-udp-timeout.patch"
for patch in "$ADDRESS_PATCH" "$UDP_PATCH"; do
  if git -C "$WASMER_DIR" apply --check "$patch" 2>/dev/null; then
    git -C "$WASMER_DIR" apply "$patch"
  else
    git -C "$WASMER_DIR" apply --reverse --check "$patch"
  fi
done
export WASMER_REPO="$WASMER_DIR"

rustup toolchain install "$SDK_NIGHTLY" --profile minimal --component rust-src
BINDGEN="$TOOLS_DIR/wasix/bin/wasm-bindgen"
if [[ ! -x "$BINDGEN" ]] || [[ $("$BINDGEN" --version) != "wasm-bindgen $BINDGEN_VERSION" ]]; then
  cargo install --locked --force --root "$TOOLS_DIR/wasix" wasm-bindgen-cli --version "$BINDGEN_VERSION"
fi
export WASM_BINDGEN="$BINDGEN"

# Use the SDK's own bindgen, worker glue and optimizer steps. Pin its nightly
# and enforce its qualified lock in this private tree.
python3 - "$SDK_DIR/js/scripts/build-wasm.mjs" "$SDK_NIGHTLY" <<'PY'
from pathlib import Path
import sys
path, nightly = Path(sys.argv[1]), sys.argv[2]
text = path.read_text()
for old, new, label in [
    ('"+nightly"', '"+' + nightly + '"', 'nightly selection'),
    ('    ...(localWasmer ? [] : ["--locked"]),\n', '    "--locked",\n', 'dependency lock enforcement'),
]:
    if text.count(old) == 1:
        text = text.replace(old, new)
    elif text.count(new) != 1:
        raise SystemExit(f'The pinned SDK build script changed; review its {label}.')
path.write_text(text)
PY
cp "$WEBSITE_DIR/wasi/sdk.Cargo.lock" "$SDK_DIR/Cargo.lock"

cd "$SDK_DIR/js"
# The SDK's bindgen step reads this target directory directly.
export CARGO_TARGET_DIR="$SDK_DIR/target"
npm ci --ignore-scripts
npm run build
python3 - "$SDK_DIR" "$PATCH_FILE" "$SDK_REV" "$SDK_NIGHTLY" "$BINDGEN_VERSION" "$WASMER_REV" "$ADDRESS_PATCH" "$UDP_PATCH" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
sdk, patch = map(Path, sys.argv[1:3])
wasm = sdk / 'js/pkg/wasmer_sdk_js_bg.wasm'
record = {'version': '0.19.0', 'revision': sys.argv[3], 'nightly': sys.argv[4],
          'bindgen': sys.argv[5], 'networkPatch': hashlib.sha256(patch.read_bytes()).hexdigest(),
          'coreRevision': sys.argv[6], 'addressPatch': hashlib.sha256(Path(sys.argv[7]).read_bytes()).hexdigest(),
          'udpTimeoutPatch': hashlib.sha256(Path(sys.argv[8]).read_bytes()).hexdigest(),
          'wasmSha256': hashlib.sha256(wasm.read_bytes()).hexdigest()}
(sdk / '.krabka-runtime-build.json').write_text(json.dumps(record, indent=2) + '\n')
PY
mkdir -p "$WEBSITE_DIR/static/lab/sdk"
cp -R "$SDK_DIR/js/dist" "$SDK_DIR/js/pkg" "$WEBSITE_DIR/static/lab/sdk/"
cp "$SDK_DIR/LICENSE" "$WEBSITE_DIR/static/lab/sdk/LICENSE"
