#!/usr/bin/env python3
"""Enable existing operating-system implementations for the WASIX target."""

import argparse
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import tomllib


def os_cfg(text, wasi=False):
    def rewrite(match):
        cfg = match.group(0)
        targets = [('target_arch', 'wasm32'), ('target_family', 'wasm')]
        if wasi:
            targets.append(('target_os', 'wasi'))
        for target, value in targets:
            condition = f'{target} = "{value}"'
            cfg = cfg.replace(f'not({condition})', '__KRABKA_NATIVE_TARGET__')
            cfg = cfg.replace(condition, f'all({condition}, not(target_vendor = "wasmer"))')
            cfg = cfg.replace('__KRABKA_NATIVE_TARGET__', f'any(not({condition}), target_vendor = "wasmer")')
        return cfg

    return re.sub(r'#\[cfg(?:_attr)?\([^\]]*\)\]|cfg!\([^)]*\)', rewrite, text)


def single(root, pattern):
    matches = list(root.glob(pattern))
    if len(matches) != 1:
        raise ValueError(f"Expected one pinned source for {pattern}, found {len(matches)}")
    return matches[0]


def ring_crypto(manifest, helper):
    data = tomllib.loads(manifest.read_text())
    for feature in ['aws', 'azure', 'gcp', 'http']:
        entries = data['features'][feature]
        if 'aws-lc-rs' not in entries:
            raise ValueError(f'Pinned object_store crypto feature changed: {feature}')
        data['features'][feature] = ['ring' if entry == 'aws-lc-rs' else entry for entry in entries]
    manifest.write_text(helper.toml_text(data))


def port(root):
    fusion = single(root, 'git/datafusion-*/*')
    client = single(root, 'git/krabka-client-rs-*/*')
    broker = single(root, 'git/krabka-broker-*/*')
    protocol = single(root, 'git/krabka-protocol-*/*')
    sspi = single(root, 'registry/krabka-sspi-0.23.*')
    newer_store = single(root, 'registry/object_store-0.14.*')
    otel_http = single(root, 'registry/opentelemetry-http-0.33.0')
    for directory in [newer_store / 'src', fusion / 'datafusion', broker / 'crates/object-store/src', broker / 'crates/broker/src', broker / 'crates/raft/src', broker / 'crates/telemetry/src', protocol / 'crates/security/src', otel_http / 'src']:
        for source in directory.rglob('*.rs'):
            if source == broker / 'crates/broker/src/host_port.rs':
                continue
            text = source.read_text()
            transformed = os_cfg(text, wasi=directory in [broker / 'crates/broker/src', broker / 'crates/raft/src'])
            if transformed != text:
                source.write_text(transformed)
    source = client / 'crates/client-core/src/transport.rs'
    text = source.read_text()
    native = '#[cfg_attr(not(target_os = "wasi"), path = "transport/native.rs")]'
    preview1 = '#[cfg_attr(target_os = "wasi", path = "transport/wasi.rs")]'
    if native not in text or preview1 not in text:
        raise ValueError('Pinned client transport changed; review its WASIX selection')
    source.write_text(text.replace(native, '#[cfg_attr(any(not(target_os = "wasi"), target_vendor = "wasmer"), path = "transport/native.rs")]')
                     .replace(preview1, '#[cfg_attr(all(target_os = "wasi", not(target_vendor = "wasmer")), path = "transport/wasi.rs")]'))

    spec = importlib.util.spec_from_file_location('prepare_wasix', Path(__file__).with_name('prepare-wasix.py'))
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    # Cargo build-std cannot select dev-only dependency binaries. Activate the
    # existing broker dependency from this selected service in the lab's copy,
    # retaining the same pinned graph and native workspace boundaries.
    manifest = root / 'crates/metrics/Cargo.toml'
    data = tomllib.loads(manifest.read_text())
    data['dependencies']['krabka-broker'] = data['dev-dependencies']['krabka-broker']
    manifest.write_text(helper.toml_text(data))
    # Select object_store's existing complete Ring provider, which also backs
    # the services' rustls stacks and is supported by the WASIX toolchain.
    ring_crypto(newer_store / 'Cargo.toml', helper)
    manifest = root / 'crates/observability/src/server_security/internal_client.rs'
    text = manifest.read_text()
    old = 'builder = builder.tls_certs_only(self.trusted_roots.iter().cloned());'
    if old not in text:
        raise ValueError('Pinned internal-client trust selection changed')
    manifest.write_text(text.replace(old, 'builder = builder.tls_built_in_root_certs(false);\n            for certificate in self.trusted_roots.iter().cloned() {\n                builder = builder.add_root_certificate(certificate);\n            }'))
    for manifest in [*(broker / 'crates' / package / 'Cargo.toml' for package in ['object-store', 'broker', 'telemetry']), protocol / 'crates/security/Cargo.toml', sspi / 'Cargo.toml']:
        data = tomllib.loads(manifest.read_text())
        targets = data.get('target', {})
        for old in list(targets):
            if 'target_family = "wasm"' in old or 'target_arch = "wasm32"' in old:
                targets[os_cfg('#[' + old + ']')[2:-1]] = targets.pop(old)
        manifest.write_text(helper.toml_text(data))
    # WASIX has no host name either. Keep the broker's WASM branch, which
    # advertises none, and leave the hostname crate, which has no WASIX
    # implementation, out of the lab build.
    source = broker / 'crates/broker/src/host_port.rs'
    if source.read_text().count('#[cfg(target_family = "wasm")]') != 1:
        raise ValueError('Pinned broker host-name selection changed')
    manifest = broker / 'crates/broker/Cargo.toml'
    data = tomllib.loads(manifest.read_text())
    targets = data['target']
    ported = targets[os_cfg('#[cfg(not(target_family = "wasm"))]')[2:-1]]['dependencies']
    native = targets.setdefault('cfg(not(target_family = "wasm"))', {}).setdefault('dependencies', {})
    native['hostname'] = ported.pop('hostname')
    manifest.write_text(helper.toml_text(data))

    metadata = json.loads((root.parent / 'native-metadata.json').read_text())
    wasix_metadata = json.loads((root.parent / 'wasix-metadata.json').read_text())
    mio_sources = [p for p in wasix_metadata['packages']
                   if p['name'] == 'mio' and p['version'] == '1.0.3+wasix.1']
    if len(mio_sources) != 1:
        raise ValueError('Expected the pinned WASIX Mio source')
    mio = root / 'registry/mio-1.0.3+wasix.1'
    shutil.copytree(Path(mio_sources[0]['manifest_path']).parent, mio, dirs_exist_ok=True, copy_function=helper.copy_source)
    manifest = mio / 'Cargo.toml'
    data = tomllib.loads(manifest.read_text())
    data['workspace'] = {}
    manifest.write_text(helper.toml_text(data))

    def registry_source(name, version, source_metadata=metadata):
        packages = [p for p in source_metadata['packages'] if p['name'] == name and p['version'] == version]
        if len(packages) != 1:
            raise ValueError(f'Expected the qualified {name} {version} pin')
        staged = root / 'registry' / f'{name}-{version}'
        shutil.copytree(Path(packages[0]['manifest_path']).parent, staged, dirs_exist_ok=True, copy_function=helper.copy_source)
        manifest = staged / 'Cargo.toml'
        data = tomllib.loads(manifest.read_text())
        data['workspace'] = {}
        manifest.write_text(helper.toml_text(data))
        return staged

    parking = registry_source('parking_lot_core', '0.9.12', wasix_metadata)
    # Preserve addr2line's complete ELF/DWARF loader, replacing read-only mmap
    # with real file reads only for WASIX, where mmap is unavailable.
    symbols = registry_source('addr2line', '0.27.1')
    # The bundled LZ4 header returns size_t here. A void declaration causes
    # the WASM linker to replace this call with a trap, even if C returns zero.
    lz4 = registry_source('lzzzz', '2.0.0')
    binding = lz4 / 'src/lz4f/binding.rs'
    text = binding.read_text()
    old = 'pub fn LZ4F_freeCompressionContext(ctx: *mut LZ4FCompressionCtx);'
    if text.count(old) != 1:
        raise ValueError('Pinned LZ4 compression-context ABI changed')
    binding.write_text(text.replace(old, old[:-1] + ' -> size_t;'))
    website = Path(__file__).resolve().parents[1]
    subprocess.run(['patch', '-p1', '--batch', '--fuzz=0', '-i', str(website / 'wasi/object-store-reqwest.patch')], cwd=newer_store, check=True)
    subprocess.run(['patch', '-p1', '--batch', '--fuzz=0', '-i', str(website / 'wasi/addr2line-loader.patch')], cwd=symbols, check=True)
    subprocess.run(['patch', '-p1', '--batch', '--fuzz=0', '-i', str(website / 'wasi/mio-nonblocking.patch')], cwd=mio, check=True)
    subprocess.run(['patch', '-p1', '--batch', '--fuzz=0', '-i', str(website / 'wasi/parking-lot-wasix.patch')], cwd=parking, check=True)
    manifest = root / 'Cargo.toml'
    data = tomllib.loads(manifest.read_text())
    data['patch']['crates-io']['addr2line'] = {'version': '=0.27.1', 'path': str(symbols)}
    data['patch']['crates-io']['lzzzz'] = {'version': '=2.0.0', 'path': str(lz4)}
    data['patch']['crates-io']['mio'] = {'version': '=1.0.3+wasix.1', 'path': str(mio)}
    data['patch']['crates-io']['parking_lot_core'] = {'version': '=0.9.12', 'path': str(parking)}
    manifest.write_text(helper.toml_text(data))

    http = root / 'wasi-client'
    shutil.copytree(website / 'wasi-client', http, dirs_exist_ok=True, copy_function=helper.copy_source, ignore=lambda _, names: [name for name in names if name in {'target', 'Cargo.lock'}])
    (http / 'Cargo.lock').unlink(missing_ok=True)
    manifest = http / 'Cargo.toml'
    data = tomllib.loads(manifest.read_text())
    data['dependencies']['tokio']['version'] = '=' + helper.WASIX_VERSIONS['tokio'] + '+wasix.1'
    root_manifest = tomllib.loads((root / 'Cargo.toml').read_text())
    http_patches = {alias: patch for alias, patch in root_manifest['patch']['crates-io'].items()
                    if patch.get('package', alias) in {'hyper', 'hyper-util', 'h2', 'mio', 'parking_lot_core'}}
    if {patch.get('package', alias) for alias, patch in http_patches.items()} != {'hyper', 'hyper-util', 'h2', 'mio', 'parking_lot_core'}:
        raise ValueError('Expected the complete HTTP/network/locking compatibility overlay')
    data['patch'] = {'crates-io': http_patches}
    manifest.write_text(helper.toml_text(data))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    args = parser.parse_args()
    port(args.source.resolve())
