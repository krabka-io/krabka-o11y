#!/usr/bin/env python3
"""Build std from the pinned WASIX sources without changing an installed sysroot."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
from urllib.request import urlopen

REVISION = '706d63224a7ce7be61ceb9d0525fe942d99a93df'


def archive(url, destination, prefix=''):
    with urlopen(url, timeout=120) as response, tarfile.open(fileobj=response, mode='r|gz') as contents:
        for entry in contents:
            relative = entry.name.partition('/')[2]
            if not relative or (prefix and not relative.startswith(prefix)):
                continue
            entry.name = relative
            contents.extract(entry, destination, filter='data')


def prepare(tools, compiler, patch):
    tools, compiler, patch = tools.resolve(), compiler.resolve(), patch.resolve()
    root = tools / 'wasix-rust'
    if root == compiler or root.is_relative_to(compiler) or compiler.is_relative_to(root):
        raise ValueError('The private compiler directory must be separate from the installed sysroot')
    patch_hash = hashlib.sha256(patch.read_bytes()).hexdigest()
    record = {'revision': REVISION, 'patch': patch_hash, 'compiler': str(compiler)}
    marker = root / '.krabka-std.json'
    if not root.exists():
        # Private directory entries and shared immutable compiler files. New std
        # sources go only into this tree; no installed source or binary is edited.
        shutil.copytree(compiler, root, copy_function=os.link, symlinks=True)
    source = root / 'lib/rustlib/src/rust'
    if not marker.exists() or json.loads(marker.read_text()) != record:
        if source.exists():
            shutil.rmtree(source)
        source.mkdir(parents=True)
        archive(f'https://codeload.github.com/wasix-org/rust/tar.gz/{REVISION}', source, 'library/')
        # The backtrace submodule is the only library subtree absent from a GitHub
        # source archive. Resolve its exact revision from this immutable tree.
        with urlopen(f'https://api.github.com/repos/wasix-org/rust/contents/library/backtrace?ref={REVISION}', timeout=30) as response:
            revision = json.load(response)['sha']
        archive(f'https://codeload.github.com/rust-lang/backtrace-rs/tar.gz/{revision}', source / 'library/backtrace')
        subprocess.run(['patch', '-p1', '--batch', '--fuzz=0', '-i', str(patch)], cwd=source, check=True, stdout=subprocess.DEVNULL)
        marker.write_text(json.dumps(record, indent=2) + '\n')
    alias = 'krabka-o11y-wasix-' + hashlib.sha256(str(root).encode()).hexdigest()[:12]
    subprocess.run(['rustup', 'toolchain', 'link', alias, str(root)], check=True)
    library = source / 'library'
    profiler = library / 'profiler_builtins/Cargo.toml'
    text = profiler.read_text()
    if 'cc = "=1.2.0"' in text:
        profiler.write_text(text.replace('cc = "=1.2.0"', 'cc = "=1.2.27+wasix.1"'))
    elif 'cc = "=1.2.27+wasix.1"' not in text:
        raise ValueError('Pinned std profiler compiler dependency changed')
    config = library / '.cargo/config.toml'
    config.parent.mkdir(exist_ok=True)
    config.write_text('[source.crates-io]\nreplace-with = "wasix"\n[source.wasix]\nregistry = "sparse+https://cargo-registry.wasix.org/"\n')
    # This qualified graph includes the registry forks and the exact libc Git
    # revision. Keep build-std independent of live registry/branch updates.
    shutil.copyfile(Path(__file__).resolve().parents[1] / 'wasi/std.Cargo.lock', library / 'Cargo.lock')
    return alias


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tools', required=True, type=Path)
    parser.add_argument('--compiler', required=True, type=Path)
    parser.add_argument('--patch', required=True, type=Path)
    args = parser.parse_args()
    print(prepare(args.tools, args.compiler, args.patch))
