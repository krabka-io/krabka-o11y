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
import pathlib
import subprocess
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


def capture(deployment, output, seconds, windows):
    def role(name, port):
        for window in range(windows):
            print('CPU profile', name, window + 1, flush=True)
            cpu_profile(port, seconds, output / f'{name}.{window + 1}.cpu.pb.gz')

    def minio():
        identity = deployment.ids['minio']
        # The pinned dev image contains mc. This is the synthetic benchmark
        # account, and --airgap explicitly disables uploads to MinIO support.
        command = ('mc alias set profile http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && '
                   f'mc support profile --airgap --type cpu,mem --duration {seconds} profile')
        log = env.command('docker', 'exec', '--workdir', '/tmp', identity, '/bin/sh', '-c', command)
        (output / 'minio-client.txt').write_text(log)
        archive = output / 'minio-profile.zip'
        env.command('docker', 'cp', identity + ':/tmp/profile.zip', str(archive))
        with zipfile.ZipFile(archive) as profiles:
            for member in profiles.infolist():
                if member.is_dir():
                    continue
                # Flatten untrusted archive paths instead of extracting them.
                path = output / ('minio-' + pathlib.PurePosixPath(member.filename).name)
                path.write_bytes(profiles.read(member))
                if 'mem' in path.name or 'heap' in path.name:
                    summarize(path, 'inuse_space')
                    summarize(path, 'alloc_space')
                elif 'cpu' in path.name:
                    summarize(path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(deployment.admin_ports) + 1) as pool:
        futures = [pool.submit(role, name, port) for name, port in deployment.admin_ports.items()]
        futures.append(pool.submit(minio))
        for future in futures:
            future.result()


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
              'profile_seconds': args.profile_seconds, 'windows': args.windows,
              'tool_versions': {'go': env.command('go', 'version')},
              'seed': env.SEED, 'writers': 2, 'write_interval_seconds': 1,
              'query_interval_seconds': 0.25, 'warmup_seconds': 15,
              'harness_sha256': {name: hashlib.sha256(pathlib.Path(__file__).with_name(name).read_bytes()).hexdigest()
                                 for name in ('profile-grafana.py', 'compare-grafana.py', 'deployment-envelope.py', 'fuzz-corpus.py')}}
    deployment = comparison.ComparisonDeployment(output / 'deployment', args.image, args.signal, False,
                                                 args.deployment_target, args.deployment_target)
    try:
        deployment.start()
        cardinality = 1000 if args.signal == 'metrics' else 100
        deployment.seed(args.signal, 'soak', cardinality)
        deployment.wait_for_quiet_host(120)
        seconds = args.profile_seconds * args.windows + 15
        report['started_unix'] = time.time()
        result, operations, samples = env.measure(deployment, args.signal, seconds, 15, 2,
            cardinality, interval=1, check_durability=False,
            on_measurement=lambda: capture(deployment, output, args.profile_seconds, args.windows))
        report['workload'] = result
        for name, records in [('operations', operations), ('telemetry', samples)]:
            (output / (name + '.jsonl')).write_text(''.join(json.dumps(record) + '\n' for record in records))
        if result['ingest']['error_rate'] or result['query']['error_rate'] or result['query']['empty_queries']:
            raise RuntimeError('profiling workload failed or returned empty queries')
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
