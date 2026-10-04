#!/usr/bin/env python3
"""Compare native single-node Grafana APIs with Krabka on one controlled host.

API acknowledgements have different durability contracts. This experiment
reports accepted throughput, not equivalent durable throughput.
"""
import argparse
import functools
import hashlib
import importlib.util
import json
import math
import os
import pathlib
import re
import statistics
import struct
import threading
import time

_spec = importlib.util.spec_from_file_location('envelope', pathlib.Path(__file__).with_name('deployment-envelope.py'))
env = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(env)
PRODUCTS = {
    'metrics': ('mimir', 'mirror.gcr.io/grafana/mimir@sha256:d42bfba7a8ef82a14b883a4cf235406324aeecae94c85eba000bdaa21aa2f289', 9009),
    'logs': ('loki', 'mirror.gcr.io/grafana/loki@sha256:81a6802ec4bd1b88c564494f06376889ed022998a188826190d26d2754ac2aae', 3100),
    'traces': ('tempo', 'mirror.gcr.io/grafana/tempo@sha256:19dca9c0b1801209424a757cd5970d6ffd7cfc9a7f4966c2a6795fbe29b495a8', 3200),
    'profiles': ('pyroscope', 'mirror.gcr.io/grafana/pyroscope@sha256:718b585ea168a616ca737018dbd1676992db9e5f1084ec4e9cd18f669326f334', 4040),
}
_original_write = env.write_request
_original_query = env.query_request
_original_http = env.http
_timestamp_lock = threading.Lock()
_last_ms = {}


def write_request(signal, sequence, cardinality, tenant='soak', age_seconds=0):
    if signal != 'metrics':
        path, body, content, rows = _original_write(signal, sequence, cardinality, tenant, age_seconds)
        if signal == 'profiles':
            query = env.urllib.parse.parse_qs(env.urllib.parse.urlsplit(path).query)
            # Legacy Pyroscope labels use unquoted values. Quoted values are
            # ingested literally and would not match the PromQL selector.
            query['name'] = [query['name'][0].replace(chr(34), '')]
            query['units'] = ['nanoseconds']
            query['sampleRate'] = ['1000000000']
            query['from'] = query['until']
            path = '/ingest?' + env.urllib.parse.urlencode({key: value[0] for key, value in query.items()})
        return path, body, content, rows
    now = time.time_ns() // 1_000_000 - age_seconds * 1000
    with _timestamp_lock:
        stamp = max(now, _last_ms.get((tenant, age_seconds), 0) + 1)
        _last_ms[tenant, age_seconds] = stamp
    timestamp = env.wire.pb_int64_field(2, stamp)
    prefixes = metric_prefixes(cardinality, len(timestamp))
    start = sequence * 1000 % cardinality
    selected = prefixes[start:start + 1000] if cardinality % 1000 == 0 else tuple(prefixes[(start + i) % cardinality] for i in range(1000))
    body = b''.join(prefix + timestamp for prefix in selected)
    return '/api/v1/push', env.wire.snappy_literal_block(body), 'application/x-protobuf', 1000


@functools.lru_cache(maxsize=8)
def metric_prefixes(cardinality, timestamp_size):
    """Cache immutable labels and values; only the timestamp changes per RPC."""
    prefixes = []
    for label in range(cardinality):
        labels = env.wire.pb_bytes_field(1, env.wire.pb_label('__name__', 'envelope_samples'))
        labels += env.wire.pb_bytes_field(1, env.wire.pb_label('series', str(label)))
        sample = env.wire.pb_double_field(1, float((env.SEED + label) % 97)) + bytes(timestamp_size)
        series = env.wire.pb_bytes_field(1, labels + env.wire.pb_bytes_field(2, sample))
        prefixes.append(series[:-timestamp_size])
    return tuple(prefixes)


def http(port, path, tenant='soak', body=None, content_type='application/json'):
    if port == env.INGEST.get('metrics') and path == '/api/v1/push':
        # The existing helper supplies tenant/content headers. Mimir also
        # requires the Snappy encoding and remote-write version headers.
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError
        request = Request(f'http://127.0.0.1:{port}{path}', body, {
            'X-Scope-OrgID': tenant, 'Content-Type': content_type,
            'Content-Encoding': 'snappy', 'X-Prometheus-Remote-Write-Version': '0.1.0'})
        try:
            with urlopen(request, timeout=30) as response:
                return response.status, response.read()
        except HTTPError as error:
            return error.code, error.read()
        except (OSError, TimeoutError) as error:
            return 0, str(error).encode()
    return _original_http(port, path, tenant, body, content_type)


def query_request(signal, window_seconds=30):
    path, body = _original_query(signal, window_seconds)
    if signal == 'metrics' and env.QUERY[signal] == 9009:
        path = '/prometheus' + path
    return path, body


def resource_sample(deployment, sample):
    sample['cpu_usec'], sample['throttled_usec'] = {}, {}
    sample['load_generator_cpu_seconds'] = time.process_time()
    for role, pid in deployment.pids.items():
        try:
            group = pathlib.Path(f'/proc/{pid}/cgroup').read_text().split('0::', 1)[1].strip()
            stats = dict(line.split() for line in (pathlib.Path('/sys/fs/cgroup') / group.lstrip('/') / 'cpu.stat').read_text().splitlines())
            sample['cpu_usec'][role] = int(stats['usage_usec'])
            sample['throttled_usec'][role] = int(stats.get('throttled_usec', 0))
        except (OSError, IndexError, KeyError, ValueError):
            sample['scrape_errors'].append(role + '/cpu')
    code, body = _original_http(19000, '/minio/v2/metrics/cluster')
    if code == 200:
        sample['s3'] = env.prometheus(body.decode())
        for prefix in ('minio_s3_requests_total', 'minio_s3_traffic_sent_bytes', 'minio_s3_traffic_received_bytes'):
            if not any(key.startswith(prefix) for key in sample['s3']):
                sample['scrape_errors'].append('minio/missing-' + prefix)
    else:
        sample['s3'] = {}
        sample['scrape_errors'].append('minio/s3-metrics')
    return sample


def cost(samples):
    if len(samples) < 2:
        raise RuntimeError('insufficient resource samples; CPU and S3 deltas are unavailable')
    cpu, throttle = {}, {}
    requests = read_bytes = write_bytes = 0
    for first, following in zip(samples, samples[1:]):
        for field, output in (('cpu_usec', cpu), ('throttled_usec', throttle)):
            for role, value in following[field].items():
                before = first[field].get(role, value)
                output[role] = output.get(role, 0) + (value - before if value >= before else value)
        for name, value in following['s3'].items():
            before = first['s3'].get(name, 0)
            delta = value - before if value >= before else value
            if name.startswith('minio_s3_requests_total{'):
                requests += delta
            elif name.startswith('minio_s3_traffic_sent_bytes'):
                read_bytes += delta
            elif name.startswith('minio_s3_traffic_received_bytes'):
                write_bytes += delta
    return {'cpu_seconds_by_role': {role: value / 1e6 for role, value in cpu.items()},
            'cpu_seconds_total': sum(cpu.values()) / 1e6,
            'load_generator_cpu_seconds': samples[-1].get('load_generator_cpu_seconds', 0) - samples[0].get('load_generator_cpu_seconds', 0),
            'throttled_seconds_by_role': {role: value / 1e6 for role, value in throttle.items()},
            'rss_kib_simultaneous_peak': max(sum(s['rss_kib'].values()) for s in samples),
            's3_requests': requests, 's3_read_bytes': read_bytes, 's3_write_bytes': write_bytes}


class ComparisonDeployment(env.Deployment):
    def __init__(self, evidence, image, signal, native):
        super().__init__(evidence, image)
        self.signal, self.native = signal, native
        data = json.loads(self.file.read_text())
        services = data['services']
        for name in list(services):
            if any(name.startswith(s + '-') for s in PRODUCTS) and not name.startswith(signal + '-'):
                del services[name]
        services['minio']['environment']['MINIO_PROMETHEUS_AUTH_TYPE'] = 'public'
        services['minio']['ports'] = [{'target': 9000, 'published': '19000', 'host_ip': '127.0.0.1'}]
        self.service_cpu = sum(float(s['cpus']) for name, s in services.items() if name == 'broker' or name.startswith(signal + '-'))
        self.service_memory_gib = self.service_cpu
        if native:
            product, native_image, port = PRODUCTS[signal]
            services = {name: service for name, service in services.items() if name.startswith('minio')}
            data['services'] = services
            bucket_names = ['mimir-blocks', 'mimir-ruler', 'mimir-alertmanager'] if signal == 'metrics' else [product]
            services['minio-buckets']['command'] = ['mc alias set krabka http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && ' + ' && '.join('mc mb --ignore-existing krabka/' + bucket for bucket in bucket_names)]
            # Compose interpolates environment references; quote dollars so
            # the init container expands them from its own fixed environment.
            services['minio-buckets']['command'][0] = services['minio-buckets']['command'][0].replace('$MINIO_', '$$MINIO_')
            data['volumes']['native-data'] = {}
            services['minio-permissions']['volumes'].append({'type': 'volume', 'source': 'native-data', 'target': '/native-data'})
            services['minio-permissions']['command'] = ['-R', '65532:65532', '/data', '/native-data']
            config = pathlib.Path('deploy/compare', product + '.yaml').resolve()
            ports = [{'target': port, 'published': str(port), 'host_ip': '127.0.0.1'}]
            if signal == 'traces':
                ports.append({'target': 4318, 'published': '4318', 'host_ip': '127.0.0.1'})
            services[product] = {'image': native_image, 'user': '65532:65532',
                'command': ['-config.file=/etc/compare.yaml', '-config.expand-env=true'],
                'cpus': self.service_cpu, 'mem_limit': f'{self.service_memory_gib}g',
                'ulimits': {'nofile': {'soft': 65536, 'hard': 65536}},
                'volumes': [{'type': 'bind', 'source': str(config), 'target': '/etc/compare.yaml', 'read_only': True},
                            {'type': 'volume', 'source': 'native-data', 'target': '/data'}],
                'working_dir': '/data', 'ports': ports, 'environment': {'MINIO_ROOT_USER': 'krabka', 'MINIO_ROOT_PASSWORD': 'krabka-secret'},
                'depends_on': {'minio-buckets': {'condition': 'service_completed_successfully'}}}
            env.INGEST[signal] = 4318 if signal == 'traces' else port
            env.QUERY[signal] = port
            self.admin_ports = {product: port}
        else:
            env.INGEST[signal] = {'metrics': 4041, 'logs': 3100, 'traces': 4318, 'profiles': 4040}[signal]
            env.QUERY[signal] = {'metrics': 9090, 'logs': 3101, 'traces': 3201, 'profiles': 4042}[signal]
            self.admin_ports = {name: port for name, port in self.admin_ports.items() if name == 'broker' or name.startswith(signal + '-')}
        self.file.write_text(json.dumps(data, indent=2))

    def start(self):
        if not self.native:
            super().start()
            return
        self.run('up', '-d', '--wait', '--wait-timeout', '300')
        product, _, port = PRODUCTS[self.signal]
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if http(port, '/ready')[0] == 200:
                break
            time.sleep(1)
        else:
            raise RuntimeError(product + ' did not become ready')
        self.ids = {product: self.run('ps', '-q', product), 'minio': self.run('ps', '-q', 'minio')}
        self.refresh_pids()
        config_path = '/status/config' if self.signal == 'traces' else '/config'
        status, config = http(port, config_path)
        (self.evidence / 'effective-config.txt').write_bytes(config)
        (self.evidence / 'effective-config-status.json').write_text(json.dumps({'path': config_path, 'status': status}))
        (self.evidence / 'containers.json').write_text(json.dumps({name: json.loads(env.command('docker', 'inspect', identity))[0] for name, identity in self.ids.items()}, indent=2))

    def close(self):
        for name in json.loads(self.file.read_text())['services']:
            identity = self.run('ps', '--all', '--quiet', name)
            if identity:
                self.ids[name] = identity
        super().close()

    def sample(self, signal):
        if not self.native:
            return resource_sample(self, super().sample(signal))
        sample = {'time_unix': time.time(), 'rss_kib': {}, 'metrics': {}, 'recovery': {}, 'scrape_errors': []}
        for role, pid in self.pids.items():
            try:
                sample['rss_kib'][role] = int(re.search(r'^VmRSS:\s+(\d+)', pathlib.Path(f'/proc/{pid}/status').read_text(), re.M)[1])
            except (OSError, TypeError):
                sample['scrape_errors'].append(role + '/rss')
        product, _, port = PRODUCTS[signal]
        code, body = http(port, '/metrics')
        if code == 200:
            sample['metrics'][product] = env.prometheus(body.decode())
        else:
            sample['scrape_errors'].append(product)
        return resource_sample(self, sample)

    def seed(self, signal, tenant, cardinality, age_seconds=0):
        per_request = 1000 if signal == 'metrics' else 100 if signal == 'logs' else 10 if signal == 'traces' else 1
        records = []
        def write(_):
            sequence = next(self.sequence)
            path, body, content, rows = write_request(signal, sequence, cardinality, tenant, age_seconds)
            started = time.monotonic()
            status, response = http(env.INGEST[signal], path, tenant, body, content)
            records.append({'sequence': sequence, 'status': status, 'rows': rows, 'seconds': time.monotonic() - started, 'tenant': tenant, 'cardinality': cardinality, 'age_seconds': age_seconds})
            if not 200 <= status < 300:
                raise RuntimeError(f'seed failed: {status} {response[:200]!r}')
        try:
            with env.concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
                list(pool.map(write, range(math.ceil(cardinality / per_request))))
        finally:
            (self.evidence / f'{tenant}.seed.jsonl').write_text(''.join(json.dumps(record) + '\n' for record in records))
        if not self.native:
            recovery = self.drain(signal)
            (self.evidence / f'{tenant}.seed-recovery.json').write_text(json.dumps(recovery, indent=2))
            if not recovery['recovered']:
                raise RuntimeError('seed WAL did not drain')
        self.wait_query(signal, tenant, True)
        path, body = query_request(signal, 1800)
        status, response = http(env.QUERY[signal], path, tenant, body)
        (self.evidence / f'{tenant}.seed-query.json').write_bytes(response)
        if status != 200 or not env.has_data(signal, response):
            raise RuntimeError('seed query lost visibility')
        data = json.loads(response)
        if signal == 'metrics':
            observed = float(data['data']['result'][0]['value'][1])
            expected = sum((env.SEED + label) % 97 for label in range(cardinality))
        elif signal == 'profiles':
            observed = float(data['flamegraph']['total'])
            expected = len(records) * env.POINTS
        else:
            return
        if observed != expected:
            raise RuntimeError(f'seed value mismatch: expected {expected}, observed {observed}')
        if signal == 'profiles':
            # Drain guarantees publication, but the querier refreshes its
            # index every 15s. Recheck the immutable seed after that handoff.
            # Both backends get this untimed wait before warm-up.
            time.sleep(20)
            status, response = http(env.QUERY[signal], path, tenant, body)
            (self.evidence / f'{tenant}.seed-handoff-query.json').write_bytes(response)
            if status != 200 or not env.has_data(signal, response):
                raise RuntimeError('seed handoff query lost visibility')
            observed = float(json.loads(response)['flamegraph']['total'])
            if observed != expected:
                raise RuntimeError(f'seed value mismatch after cold handoff: expected {expected}, observed {observed}')



def run(args):
    env.SIGNALS = (args.signal,)
    env.write_request, env.query_request, env.http = write_request, query_request, http
    report = {'schema_version': 1, 'commit': env.command('git', 'rev-parse', 'HEAD'),
              'signal': args.signal, 'seed': env.SEED, 'phase_seconds': args.seconds,
              'acknowledgements': 'API accepted; native durability contracts differ',
              'write_interval_seconds': 1, 'query_interval_seconds': 0.25,
              'image_commit': args.image_commit, 'image_digest': args.image_digest,
              'image_identity': json.loads(env.command('docker', 'inspect', args.image))[0],
              'harness_sha256': {name: hashlib.sha256(pathlib.Path(__file__).with_name(name).read_bytes()).hexdigest()
                                 for name in ('compare-grafana.py', 'deployment-envelope.py', 'fuzz-corpus.py')},
              'dataset': {'metric_samples_per_request': 1000, 'metric_points_per_series': 1,
                          'other_signals': 'deployment-envelope dataset; profile units=nanoseconds, unquoted legacy labels, sampleRate=1e9', 'seed_concurrency': 64},
              'host': {'cpu_count': os.cpu_count(), 'kernel': env.command('uname', '-r'), 'lscpu': json.loads(env.command('lscpu', '-J'))},
              'phases': args.phases, 'backends': args.backends,
              'entries': [], 'workload': {'writers': [n for n in env.WRITERS if n <= args.max_writers],
                         'cardinalities': [n for n in (1000, 5000, 20000) if n <= args.max_cardinality]}}
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / "comparison-report.json").exists():
        raise RuntimeError("output already contains a comparison report; choose a fresh directory")
    for repetition in range(args.repetitions):
        # Alternate the first backend to avoid always warming the host for one.
        for native in ((False, True) if repetition % 2 == 0 else (True, False)):
            if ('native' if native else 'krabka') not in args.backends:
                continue
            backend = PRODUCTS[args.signal][0] if native else 'krabka'
            for phase in args.phases:
                output = args.output / f'{repetition + 1}-{backend}-{phase}'
                deployment = ComparisonDeployment(output, args.image, args.signal, native)
                try:
                    deployment.start()
                    levels = report['workload']['writers'] if phase == 'burst' else report['workload']['cardinalities'] if phase == 'high_cardinality' else [2]
                    for level in levels:
                        writers = level if phase == 'burst' else 2
                        cardinality = level if phase == 'high_cardinality' else 1000 if args.signal == 'metrics' else 100
                        tenant = f'cardinality-{level}' if phase == 'high_cardinality' else f'burst-{level}' if phase == 'burst' else 'soak'
                        try:
                            deployment.seed(args.signal, tenant, cardinality)
                            duration = args.seconds if phase == 'steady' else args.seconds / 2
                            result, operations, samples = env.measure(deployment, args.signal, duration, duration / 4,
                                writers, cardinality, cold=phase == 'high_cardinality', interval=1, tenant=tenant, check_durability=False)
                            name = f'{level}'
                            (output / f'{name}.operations.jsonl').write_text(''.join(json.dumps(record) + '\n' for record in operations))
                            (output / f'{name}.telemetry.jsonl').write_text(''.join(json.dumps(sample) + '\n' for sample in samples))
                            result['resources'] = cost(samples)
                            result['resources']['sample_count'] = len(samples)
                            result['resources']['sampled_seconds'] = samples[-1]['time_unix'] - samples[0]['time_unix']
                            result['resources']['coverage_fraction'] = result['resources']['sampled_seconds'] / result['duration_seconds']
                            result['objectives_met'] &= result['resources']['coverage_fraction'] >= 0.9
                            rows = result['ingest']['accepted_rows']
                            result['resources']['cpu_seconds_per_million_accepted_rows'] = result['resources']['cpu_seconds_total'] * 1e6 / rows if rows else None
                        except RuntimeError as error:
                            result = {'signal': args.signal, 'writers': writers, 'cardinality': cardinality,
                                      'objectives_met': False, 'seed_error': str(error)}
                        result.update(backend=backend, repetition=repetition + 1, phase=phase,
                                      service_cpu=deployment.service_cpu, service_memory_gib=deployment.service_memory_gib,
                                      minio_cpu=2, minio_memory_gib=2)
                        result['container_states'] = {name: json.loads(env.command('docker', 'inspect', '--format', '{{json .State}}', identity)) for name, identity in deployment.ids.items()}
                        report['entries'].append(result)
                        (args.output / 'comparison-report.json').write_text(json.dumps(report, indent=2) + '\n')
                        print(backend, repetition + 1, phase, level, result['objectives_met'], result.get('query', {}).get('latency_seconds'), flush=True)
                        if not result['objectives_met']:
                            break
                finally:
                    deployment.close()
    checksums = []
    for path in sorted(args.output.rglob('*')):
        if path.is_file() and path.name != 'SHA256SUMS':
            with path.open('rb') as source:
                digest = hashlib.file_digest(source, 'sha256').hexdigest()
            checksums.append(f'{digest}  {path.relative_to(args.output)}\n')
    (args.output / 'SHA256SUMS').write_text(''.join(checksums))


def self_test():
    assert write_request('metrics', 1, 1000)[3] == 1000
    query = env.urllib.parse.parse_qs(env.urllib.parse.urlsplit(write_request('profiles', 1, 100)[0]).query)
    assert query['units'] == ['nanoseconds'] and query['sampleRate'] == ['1000000000']
    assert chr(34) not in query['name'][0] and query['from'] == query['until']
    # The original uncached encoder is an independent byte-level oracle.
    stamp = env.wire.pb_int64_field(2, 1791120000000)
    for cardinality in (1000, 5000, 20000):
        for sequence in (0, 23):
            reference = []
            for i in range(1000):
                label = (sequence * 1000 + i) % cardinality
                labels = env.wire.pb_bytes_field(1, env.wire.pb_label('__name__', 'envelope_samples'))
                labels += env.wire.pb_bytes_field(1, env.wire.pb_label('series', str(label)))
                sample = env.wire.pb_double_field(1, float((env.SEED + label) % 97)) + stamp
                reference.append(env.wire.pb_bytes_field(1, labels + env.wire.pb_bytes_field(2, sample)))
            prefixes = metric_prefixes(cardinality, len(stamp))
            start = sequence * 1000 % cardinality
            assert b''.join(prefix + stamp for prefix in prefixes[start:start + 1000]) == b''.join(reference)
    sample = {'cpu_usec': {'a': 10}, 'throttled_usec': {'a': 1}, 'rss_kib': {'a': 100},
              's3': {'minio_s3_requests_total{api="GetObject"}': 2, 'minio_s3_traffic_sent_bytes': 10}}
    after = {'cpu_usec': {'a': 1000010}, 'throttled_usec': {'a': 2}, 'rss_kib': {'a': 200},
             's3': {'minio_s3_requests_total{api="GetObject"}': 5, 'minio_s3_traffic_sent_bytes': 30}}
    result = cost([sample, after])
    assert result['cpu_seconds_total'] == 1 and result['s3_requests'] == 3 and result['s3_read_bytes'] == 20
    assert result['rss_kib_simultaneous_peak'] == 200
    print('compare-grafana self-test passed')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--signal', choices=PRODUCTS)
    p.add_argument('--image')
    p.add_argument('--image-digest')
    p.add_argument('--image-commit')
    p.add_argument('--seconds', type=int, default=60)
    p.add_argument('--repetitions', type=int, default=3)
    p.add_argument('--max-writers', type=int, default=256)
    p.add_argument('--max-cardinality', type=int, default=20000)
    p.add_argument('--phases', nargs='+', choices=('steady', 'burst', 'high_cardinality'),
                   default=['steady', 'burst', 'high_cardinality'])
    p.add_argument('--backends', nargs='+', choices=('krabka', 'native'), default=['krabka', 'native'])
    p.add_argument('--output', type=pathlib.Path, default=pathlib.Path('qualification/evidence/comparison'))
    p.add_argument('--self-test', action='store_true')
    options = p.parse_args()
    if options.self_test:
        self_test()
    else:
        if (not options.signal or not options.image or not options.image_digest or not options.image_commit
                or min(options.seconds, options.repetitions, options.max_writers, options.max_cardinality) <= 0):
            p.error('signal, image, image digest, image commit and positive load/duration values are required')
        if len(set(options.phases)) != len(options.phases) or len(set(options.backends)) != len(options.backends):
            p.error('phases and backends must be unique')
        run(options)
