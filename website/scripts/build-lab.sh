#!/usr/bin/env bash
# Build real services and publish a manifest only after every required artifact exists.
set -euo pipefail

site="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
repository="$(cd "$site/.." && pwd -P)"
cd "$repository"
tools="$site/.tools"
source_tree="$tools/wasi-source"
lab="$site/static/lab"
jobs="${CARGO_BUILD_JOBS:-4}"
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo 'CARGO_BUILD_JOBS must be a positive integer.' >&2; exit 1; }

cargo_wasix_version=0.1.34
wasixcc_version=0.4.7
rust_tag='v2026-09-18.1+rust-1.97'
sysroot_tag='v2026-10-02.1'
llvm_tag='21.1.206'
binaryen_tag='version_133'
sdk_version='0.19.0'
sdk_revision='362e0db28fea57fb23af8d34cffefcb7333a883d'
modules=(krabka-broker krabka-format krabka-o11y-bootstrap krabka-metrics krabka-metrics-service krabka-observability krabka-traces krabka-profiles krabka-lab-http krabka-lab-network)
mkdir -p "$tools" "$lab"
export TMPDIR="$tools/tmp"
mkdir -p "$TMPDIR"
artifact_directory=''

cleanup() {
  status=$?
  trap - EXIT
  if (( status != 0 )); then
    rm -f "$lab/manifest.json" "$site/dist/lab/manifest.json"
    for name in "${modules[@]}"; do rm -f "$lab/$name.wasm"; done
    echo 'Lab build failed; no ready manifest was published.' >&2
  fi
  if [[ -n "$artifact_directory" ]]; then rm -rf "$artifact_directory"; fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
rm -f "$lab/manifest.json" "$site/dist/lab/manifest.json"
for name in "${modules[@]}"; do rm -f "$lab/$name.wasm"; done

native_version="$(python3 - "$repository/rust-toolchain.toml" <<'PY'
import sys, tomllib
from pathlib import Path
print(tomllib.loads(Path(sys.argv[1]).read_text())["toolchain"]["channel"])
PY
)"
native_cargo=(rustup run "$native_version" cargo)
echo 'Resolving the locked native source graph.'
"${native_cargo[@]}" metadata --locked --format-version 1 --manifest-path "$repository/Cargo.toml" > "$tools/native-metadata.json.tmp"
mv "$tools/native-metadata.json.tmp" "$tools/native-metadata.json"

export PATH="$tools/wasix/bin:$PATH"
ensure_tool() {
  local crate="$1" version="$2" binary="$3"
  if [[ ! -x "$tools/wasix/bin/$binary" ]] || ! python3 - "$tools/wasix/.crates2.json" "$crate" "$version" <<'PY'
import json, sys
from pathlib import Path
path, crate, version = sys.argv[1:]
try:
    installed = json.loads(Path(path).read_text())["installs"]
except (FileNotFoundError, KeyError, json.JSONDecodeError):
    sys.exit(1)
sys.exit(0 if any(key.startswith(f"{crate} {version} (") for key in installed) else 1)
PY
  then
    "${native_cargo[@]}" install "$crate" --version "=$version" --locked --force --root "$tools/wasix" --target-dir "$tools/installer-target" -j "$jobs"
  fi
}
ensure_tool cargo-wasix "$cargo_wasix_version" cargo-wasix
ensure_tool wasixcc "$wasixcc_version" wasixccenv

export WASIX_DATA_DIR="$tools/cargo-wasix"
export WASIX_CACHE_DIR="$tools/cargo-wasix/cache"
export CARGO_WASIX_NO_REGISTRY_CONFIG=1
rust_marker="$tools/wasix/rust-toolchain.tag"
wasix_sysroot="$(rustup run wasix rustc --print sysroot 2>/dev/null || true)"
if [[ ! -f "$rust_marker" || "$(cat "$rust_marker")" != "$rust_tag" || "$wasix_sysroot" != *"$rust_tag"* || ! -d "$wasix_sysroot" ]]; then
  "${native_cargo[@]}" wasix download-toolchain "$rust_tag"
  wasix_sysroot="$(rustup run wasix rustc --print sysroot)"
  printf '%s\n' "$rust_tag" > "$rust_marker"
fi
# This release archive omits execute bits on several linker wrappers.
python3 - "$wasix_sysroot" <<'PY'
import sys
from pathlib import Path
root = Path(sys.argv[1])
for directory in [root / "bin", *root.glob("lib/rustlib/*/bin")]:
    for executable in directory.rglob("*"):
        if executable.is_file():
            executable.chmod(executable.stat().st_mode | 0o111)
PY

export WASIXCC_SYSROOT_PREFIX="$tools/wasixcc/sysroot"
export WASIXCC_LLVM_LOCATION="$tools/wasixcc/llvm"
export WASIXCC_BINARYEN_LOCATION="$tools/wasixcc/binaryen"
cc_marker="$tools/wasixcc/versions.txt"
cc_versions="$wasixcc_version $sysroot_tag $llvm_tag $binaryen_tag"
if [[ ! -f "$cc_marker" || "$(cat "$cc_marker")" != "$cc_versions" || ! -f "$WASIXCC_SYSROOT_PREFIX/sysroot-eh/lib/wasm32-wasi/libc.a" || ! -x "$WASIXCC_LLVM_LOCATION/bin/clang" || ! -x "$WASIXCC_BINARYEN_LOCATION/bin/wasm-opt" ]]; then
  wasixccenv download-all --sysroot-tag "$sysroot_tag" --llvm-tag "$llvm_tag" --binaryen-tag "$binaryen_tag"
  printf '%s\n' "$cc_versions" > "$cc_marker"
fi
wasixccenv install-executables "$tools/wasix/bin"
export WASM_OPT="$WASIXCC_BINARYEN_LOCATION/bin/wasm-opt"
export CARGO_WASIX_OFFLINE=1
wasix_toolchain="$(python3 -B "$site/scripts/prepare-wasix-std.py" --tools "$tools" --compiler "$wasix_sysroot" --patch "$site/wasi/wasix-std-network.patch")"

echo 'Staging source and applying the WASIX platform adaptations.'
python3 -B "$site/scripts/prepare-wasix.py" --metadata "$tools/native-metadata.json" --output "$source_tree"
(
  cd "$source_tree"
  # Only locate the exact Mio/parking sources needed by the platform overlay.
  "${native_cargo[@]}" metadata --format-version 1 > "$tools/wasix-metadata.json.tmp"
)
mv "$tools/wasix-metadata.json.tmp" "$tools/wasix-metadata.json"
python3 -B "$site/scripts/port-wasix.py" "$source_tree"
cp "$site/wasi/services.Cargo.lock" "$source_tree/Cargo.lock"
cp "$site/wasi/client.Cargo.lock" "$source_tree/wasi-client/Cargo.lock"
export CARGO_TARGET_DIR="$tools/wasi-target"
# cargo-wasix reuses optimized sidecars on cache hits, even after interruption.
rm -f "$CARGO_TARGET_DIR/wasm32-wasmer-wasi/release/"*.wasi.wasm

build_wasi() {
  (
    cd "$source_tree"
    export RUSTFLAGS='-C target-feature=+atomics,+simd128,+relaxed-simd,+extended-const -D linker-messages'
    export CARGO_PROFILE_RELEASE_OPT_LEVEL="${LAB_OPT_LEVEL:-s}"
    export CARGO_PROFILE_RELEASE_DEBUG=0
    export CARGO_PROFILE_RELEASE_STRIP=debuginfo
    # Enable LAB_LTO=thin once that profile passes the complete browser qualification.
    if [[ -n "${LAB_LTO:-}" ]]; then export CARGO_PROFILE_RELEASE_LTO="$LAB_LTO"; fi
    RUSTC_BOOTSTRAP=1 "${native_cargo[@]}" wasix "+$wasix_toolchain" build --locked -Zbuild-std=std,panic_abort --release --ignore-rust-version --target wasm32-wasmer-wasi -j "$jobs" "$@"
  )
}
echo 'Building the broker and all four production observability services.'
production_args=()
for package in krabka-broker krabka-format krabka-metrics krabka-metrics-service krabka-observability krabka-traces krabka-profiles; do
  production_args+=(-p "$package")
done
for name in "${modules[@]:0:8}"; do production_args+=(--bin "$name"); done
build_wasi --manifest-path "$source_tree/Cargo.toml" "${production_args[@]}"
echo 'Building the sandbox HTTP and network commands.'
build_wasi --manifest-path "$source_tree/wasi-client/Cargo.toml" --bin krabka-lab-http --bin krabka-lab-network

echo 'Building the pinned browser runtime.'
bash "$site/scripts/build-runtime.sh"
actual_sdk_revision="$(git -C "$tools/wasmer-sdk" rev-parse HEAD)"
[[ "$actual_sdk_revision" == "$sdk_revision" ]] || { echo 'Browser runtime source revision does not match its pin.' >&2; exit 1; }
[[ -s "$lab/sdk/dist/index.js" && -s "$lab/sdk/pkg/wasmer_sdk_js_bg.wasm" ]] || { echo 'Browser runtime artifacts are missing.' >&2; exit 1; }

artifact_directory="$(mktemp -d "$tools/lab-artifacts.XXXXXX")"
python3 - "$repository" "$source_tree" "$CARGO_TARGET_DIR/wasm32-wasmer-wasi/release" "$artifact_directory" "$lab" "$sdk_version" "$sdk_revision" "$rust_tag" "$cargo_wasix_version" "$wasixcc_version" "${modules[@]}" <<'PY'
import hashlib, json, shutil, subprocess, sys
from pathlib import Path
repo, source, release, output, lab = map(Path, sys.argv[1:6])
sdk_version, sdk_revision, rust_tag, cargo_wasix, wasixcc = sys.argv[6:11]
names = sys.argv[11:]

def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()

modules = []
for name in names:
    artifact = release / f"{name}.wasm"
    with artifact.open("rb") as stream:
        if stream.read(8) != b"\x00asm\x01\x00\x00\x00":
            raise ValueError(f"Invalid WASM artifact: {artifact}")
    item = {"name": name, "file": artifact.name, "sha256": digest(artifact), "bytes": artifact.stat().st_size}
    shutil.copy2(artifact, output / artifact.name)
    modules.append(item)
runtime = lab / "sdk/pkg/wasmer_sdk_js_bg.wasm"
runtime_build = json.loads((source.parent / 'wasmer-sdk/.krabka-runtime-build.json').read_text())
std_build = json.loads((source.parent / 'wasix-rust/.krabka-std.json').read_text())
std_lock = source.parent / 'wasix-rust/lib/rustlib/src/rust/library/Cargo.lock'
lock_paths = {'services': source / 'Cargo.lock', 'client': source / 'wasi-client/Cargo.lock',
              'std': std_lock, 'sdk': source.parent / 'wasmer-sdk/Cargo.lock'}
lock_hashes = {name: digest(path) for name, path in lock_paths.items()}
for name, lock_hash in lock_hashes.items():
    if lock_hash != digest(repo / 'website/wasi' / f'{name}.Cargo.lock'):
        raise ValueError(f'The qualified {name} dependency lock changed during the build')
_, sysroot, llvm, binaryen = (source.parent / 'wasixcc/versions.txt').read_text().split()
manifest = {
    "source": subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip(),
    "sourceDirty": bool(subprocess.check_output(["git", "-C", str(repo), "status", "--porcelain"], text=True).strip()),
    "runtime": {"name": "@wasmer/sdk", "version": sdk_version, "source": sdk_revision, "file": "sdk/pkg/wasmer_sdk_js_bg.wasm", "sha256": digest(runtime), "bytes": runtime.stat().st_size,
                **{key: runtime_build[key] for key in ['coreRevision', 'networkPatch', 'addressPatch', 'udpTimeoutPatch', 'nightly', 'bindgen']}},
    "toolchain": {"target": "wasm32-wasmer-wasi", "rustRelease": rust_tag, "cargoWasix": cargo_wasix, "wasixcc": wasixcc,
                  "sysrootRelease": sysroot, "llvm": llvm, "binaryen": binaryen, "stdSource": std_build['revision'], "stdNetworkPatch": std_build['patch']},
    "locks": {"native": digest(repo / "Cargo.lock"), **lock_hashes},
    "scripts": {name: digest(repo / "website/scripts" / name) for name in ["build-lab.sh", "prepare-wasix.py", "port-wasix.py", "build-runtime.sh", "prepare-wasix-std.py"]},
    "platformPatches": {path.name: digest(path) for path in sorted((repo / 'website/wasi').glob('*.patch'))},
    "modules": modules,
    "totalBytes": sum(module["bytes"] for module in modules),
}
(output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"Built {len(modules)} real modules: {manifest['totalBytes']:,} bytes.")
PY
for name in "${modules[@]}"; do cp "$artifact_directory/$name.wasm" "$lab/$name.wasm"; done
mv "$artifact_directory/manifest.json" "$lab/manifest.json"
echo "Lab artifacts ready: $lab/manifest.json"
