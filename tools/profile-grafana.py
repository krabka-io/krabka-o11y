#!/usr/bin/env python3
"""Diagnostic profiles of the exact comparison image and steady workload.

Instrumentation affects timing and RSS. These results do not qualify a
performance comparison. MinIO profiles stay local (--airgap).
"""
import argparse
import concurrent.futures
import hashlib
import importlib.util
import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import time
import urllib.request
import zipfile

spec = importlib.util.spec_from_file_location('comparison', pathlib.Path(__file__).with_name('compare-grafana.py'))
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)
env = comparison.env


def cpu_profile(port, seconds, output):
    request = f'http://127.0.0.1:{port}/debug/pprof/profile?seconds={seconds}'
    with urllib.request.urlopen(request, timeout=seconds + 30) as response:
        output.write_bytes(response.read())
    summarize(output)


def summarize(profile, sample_index=None):
    options = ['-sample_index=' + sample_index] if sample_index else []
    for kind in ('top', 'cum'):
        flags = ['-top', '-nodecount=60'] + (['-cum'] if kind == 'cum' else [])
        result = subprocess.run(['go', 'tool', 'pprof', *flags, *options, str(profile)],
                                capture_output=True, text=True, check=True)
        suffix = '.' + (sample_index + '.' if sample_index else '') + kind + '.txt'
        profile.with_name(profile.name + suffix).write_text(result.stdout + result.stderr)
        if not sample_index and 'Total samples = 0' in result.stdout:
            raise RuntimeError(f'empty CPU profile: {profile}')


def capture(deployment, output, seconds, windows, cpu=True):
    def memory_snapshot(name):
        records = {}
        for role, pid in deployment.pids.items():
            records[role] = env.command('sudo', '-n', 'cat', f'/proc/{pid}/smaps_rollup')
        (output / ('memory-' + name + '.json')).write_text(json.dumps(records, indent=2))

    memory_snapshot('start')
    def role(name, port):
        for window in range(windows):
            print('CPU profile', name, window + 1, flush=True)
            cpu_profile(port, seconds, output / f'{name}.{window + 1}.cpu.pb.gz')

    def minio():
        archive = output / 'minio-profile.zip'
        # Call the local admin Profile API directly using curl's SigV4 signer.
        # The benchmark account is synthetic. No support service is contacted.
        env.command('curl', '--fail', '--silent', '--show-error', '--max-time', str(seconds + 30),
                    '--aws-sigv4', 'aws:amz:us-east-1:s3', '--user', 'krabka:krabka-secret',
                    '--request', 'POST', '--output', str(archive),
                    f'http://127.0.0.1:19000/minio/admin/v3/profile?duration={seconds}s&profilerType=cpu%2Cmem')
        with zipfile.ZipFile(archive) as profiles:
            for member in profiles.infolist():
                if member.is_dir():
                    continue
                # Flatten untrusted archive paths instead of extracting them.
                name = re.sub(r'[^A-Za-z0-9._-]', '_', pathlib.PurePosixPath(member.filename).name)
                path = output / ('minio-' + name)
                path.write_bytes(profiles.read(member))
                if 'mem' in path.name or 'heap' in path.name:
                    summarize(path, 'inuse_space')
                    summarize(path, 'alloc_space')
                elif 'cpu' in path.name:
                    summarize(path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(deployment.admin_ports) + 1) as pool:
        futures = [pool.submit(role, name, port) for name, port in deployment.admin_ports.items()] if cpu else []
        futures.append(pool.submit(minio))
        for future in futures:
            future.result()
    memory_snapshot('end')


def copy_profiler(binary, destination):
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copy2(binary, destination / binary.name)
    for line in env.command('ldd', str(binary)).splitlines():
        if '=>' not in line:
            continue
        path = pathlib.Path(line.split('=>', 1)[1].strip().split()[0])
        # Keep the container's libc and loader. Other profiler dependencies
        # are copied from the runner, so its absolute library paths need not
        # exist inside the application image.
        if path.is_file() and not path.name.startswith(('libc.so', 'libm.so', 'libpthread.so', 'libdl.so', 'librt.so', 'ld-linux')):
            shutil.copy2(path, destination / path.name)


def configure_allocations(deployment, output):
    """Intercept the release binary's existing allocator; do not rebuild it."""
    library = next(pathlib.Path('/usr/lib').rglob('libheaptrack_preload.so'))
    libraries = output / 'heaptrack-libraries'
    copy_profiler(library, libraries)
    traces = output / 'allocations'
    traces.mkdir()
    traces.chmod(0o777)  # Container runs as uid 65532, not the runner uid.
    data = json.loads(deployment.file.read_text())
    role = deployment.signal + ('-all' if deployment.target == 'all' else '-querier')
    service = data['services'][role]
    service.setdefault('environment', {}).update({
        'LD_PRELOAD': '/opt/heaptrack/libheaptrack_preload.so',
        'LD_LIBRARY_PATH': '/opt/heaptrack',
        'DUMP_HEAPTRACK_OUTPUT': '/profiles/' + role + '.raw'})
    service.setdefault('volumes', []).extend([
        {'type': 'bind', 'source': str(libraries), 'target': '/opt/heaptrack', 'read_only': True},
        {'type': 'bind', 'source': str(traces), 'target': '/profiles'}])
    deployment.file.write_text(json.dumps(data, indent=2))
    return role


def analyze_allocations(deployment, output, role):
    # Stop gracefully while the container is still available for symbol files.
    deployment.run('stop', '-t', '30', role)
    raw = output / 'allocations' / (role + '.raw')
    if not raw.exists() or raw.stat().st_size == 0:
        raise RuntimeError('heaptrack did not record allocations')
    interpreter = next(pathlib.Path('/usr/lib').rglob('heaptrack_interpret'))
    with tempfile.TemporaryDirectory(prefix='heaptrack-symbols-') as directory:
        archive = pathlib.Path(directory) / 'root.tar'
        root = pathlib.Path(directory) / 'root'
        root.mkdir()
        env.command('docker', 'export', '--output', str(archive), deployment.ids[role])
        env.command('tar', '-xf', str(archive), '-C', str(root), '--exclude=dev', '--exclude=./dev')
        copy_profiler(interpreter, root / 'opt/heaptrack-analysis')
        interpreted = raw.with_suffix('.interpreted')
        with raw.open('rb') as source, interpreted.open('wb') as destination:
            # Ubuntu's heaptrack 1.5 interpreter has no --sysroot option.
            # Resolve modules inside the exact exported image instead.
            subprocess.run(['sudo', '-n', 'env', 'LD_LIBRARY_PATH=/opt/heaptrack-analysis',
                            'chroot', str(root), '/opt/heaptrack-analysis/heaptrack_interpret'],
                           stdin=source, stdout=destination, check=True)
        result = env.command('heaptrack_print', '-f', str(interpreted))
        raw.with_suffix('.summary.txt').write_text(result)
    for path in (raw, interpreted):
        env.command('gzip', str(path))


def run(args):
    env.SIGNALS = (args.signal,)
    env.write_request, env.query_request, env.http = comparison.write_request, comparison.query_request, comparison.http
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {'diagnostic_only': True, 'comparison_qualified': False,
              'commit': env.command('git', 'rev-parse', 'HEAD'),
              'image_commit': args.image_commit, 'image_digest': args.image_digest,
              'image_identity': json.loads(env.command('docker', 'inspect', args.image))[0],
              'signal': args.signal, 'deployment_target': args.deployment_target,
              'profile_seconds': args.profile_seconds, 'windows': args.windows if args.mode == 'cpu' else 1,
              'mode': args.mode,
              'tool_versions': {'go': env.command('go', 'version'), 'curl': env.command('curl', '--version')},
              'host': {'cpu_count': os.cpu_count(), 'kernel': env.command('uname', '-r'),
                       'lscpu': json.loads(env.command('lscpu', '-J'))},
              'seed': env.SEED, 'writers': 2, 'write_interval_seconds': 1,
              'query_interval_seconds': 0.25, 'warmup_seconds': 15,
              'harness_sha256': {name: hashlib.sha256(pathlib.Path(__file__).with_name(name).read_bytes()).hexdigest()
                                 for name in ('profile-grafana.py', 'compare-grafana.py', 'deployment-envelope.py', 'fuzz-corpus.py')}}
    deployment = comparison.ComparisonDeployment(output / 'deployment', args.image, args.signal, False,
                                                 args.deployment_target, args.deployment_target)
    allocation_role = configure_allocations(deployment, output) if args.mode == 'allocations' else None
    if allocation_role:
        report['instrumented_role'] = allocation_role
        report['tool_versions']['heaptrack'] = env.command('heaptrack', '--version')
    try:
        deployment.start()
        cardinality = 1000 if args.signal == 'metrics' else 100
        deployment.seed(args.signal, 'soak', cardinality)
        deployment.wait_for_quiet_host(120)
        seconds = args.profile_seconds * (args.windows if args.mode == 'cpu' else 1) + 15
        report['started_unix'] = time.time()
        result, operations, samples = env.measure(deployment, args.signal, seconds, 15, 2,
            cardinality, interval=1, check_durability=False,
            on_measurement=lambda: capture(deployment, output, args.profile_seconds, args.windows, cpu=args.mode == 'cpu'))
        report['workload'] = result
        for name, records in [('operations', operations), ('telemetry', samples)]:
            (output / (name + '.jsonl')).write_text(''.join(json.dumps(record) + '\n' for record in records))
        if result['ingest']['error_rate'] or result['query']['error_rate'] or result['query']['empty_queries']:
            raise RuntimeError('profiling workload failed or returned empty queries')
        if allocation_role:
            analyze_allocations(deployment, output, allocation_role)
    finally:
        deployment.close()
        (output / 'profile-report.json').write_text(json.dumps(report, indent=2) + '\n')
        checksums = []
        for path in sorted(output.rglob('*')):
            if path.is_file() and path.name != 'SHA256SUMS':
                with path.open('rb') as source:
                    checksums.append(f'{hashlib.file_digest(source, "sha256").hexdigest()}  {path.relative_to(output)}\n')
        (output / 'SHA256SUMS').write_text(''.join(checksums))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--signal', choices=comparison.PRODUCTS, required=True)
    parser.add_argument('--mode', choices=['cpu', 'allocations'], default='cpu')
    parser.add_argument('--image', required=True)
    parser.add_argument('--image-commit', required=True)
    parser.add_argument('--image-digest', required=True)
    parser.add_argument('--deployment-target', choices=['split', 'all'], default='split')
    parser.add_argument('--profile-seconds', type=int, choices=range(1, 61), default=55)
    parser.add_argument('--windows', type=int, choices=range(1, 5), default=3)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    args = parser.parse_args()
    if args.signal == 'metrics' and args.deployment_target != 'split':
        parser.error('metrics requires split deployment')
    run(args)
