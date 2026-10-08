#!/usr/bin/env python3
"""Compare native single-node Grafana APIs with Krabka on one controlled host.

API acknowledgements have different durability contracts. This experiment
reports accepted throughput, not equivalent durable throughput.
"""
import argparse
import base64
from decimal import Decimal, InvalidOperation
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
import tempfile
import threading
import time

_spec = importlib.util.spec_from_file_location('envelope', pathlib.Path(__file__).with_name('deployment-envelope.py'))
env = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(env)
PRODUCTS = {
    'metrics': ('mimir', 'mirror.gcr.io/grafana/mimir@sha256:d42bfba7a8ef82a14b883a4cf235406324aeecae94c85eba000bdaa21aa2f289', 9009),
    'logs': ('loki', 'docker.io/grafana/loki@sha256:81a6802ec4bd1b88c564494f06376889ed022998a188826190d26d2754ac2aae', 3100),
    'traces': ('tempo', 'mirror.gcr.io/grafana/tempo@sha256:19dca9c0b1801209424a757cd5970d6ffd7cfc9a7f4966c2a6795fbe29b495a8', 3200),
    'profiles': ('pyroscope', 'mirror.gcr.io/grafana/pyroscope@sha256:718b585ea168a616ca737018dbd1676992db9e5f1084ec4e9cd18f669326f334', 4040),
}
RESOURCE_ACCOUNTING_SCOPE = 'application_and_broker_excluding_object_store'

_original_write = env.write_request
_original_query = env.query_request
_original_http = env.http
_timestamp_lock = threading.Lock()
_last_ms = {}


def write_request(signal, sequence, cardinality, tenant='soak', age_seconds=0, *, seed_record=None):
    if signal != 'metrics':
        if signal == 'traces' and seed_record is not None:
            path, body, content, rows = _original_write(signal, sequence, cardinality, tenant, age_seconds, seed_record=seed_record)
        else:
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
    if seed_record is not None:
        seed_record['timestamp_ms'] = stamp
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


def metrics_seed_ledger(records, response, evaluation_ms):
    ledger = {'verified': False, 'complete_seed': False}
    try:
        cardinality = records[0]['cardinality']
        if cardinality <= 0 or len(records) != math.ceil(cardinality / 1000):
            raise ValueError('wrong seed request count')
        sequences = sorted(record['sequence'] for record in records)
        if sequences != list(range(sequences[0], sequences[0] + len(records))):
            raise ValueError('seed sequences are not distinct and consecutive')
        expected, timestamps = {}, set()
        for record in records:
            if not (200 <= record['status'] < 300 and record['rows'] == 1000
                    and record['tenant'] == records[0]['tenant'] and record['cardinality'] == cardinality
                    and record['age_seconds'] == records[0]['age_seconds']):
                raise ValueError('wrong seed admission or dataset')
            timestamp = record['timestamp_ms']
            if not isinstance(timestamp, int) or isinstance(timestamp, bool) or timestamp in timestamps:
                raise ValueError('seed receipt timestamps are not distinct integer milliseconds')
            timestamps.add(timestamp)
            if not evaluation_ms - 1800000 < timestamp <= evaluation_ms:
                raise ValueError('seed timestamp is outside the matrix window')
            if record['age_seconds'] == 0 and timestamp <= evaluation_ms - 300000:
                raise ValueError('fresh seed is outside instant lookback')
            for index in range(1000):
                series = (record['sequence'] * 1000 + index) % cardinality
                key = (('__name__', 'envelope_samples'), ('series', str(series)))
                expected.setdefault(key, set()).add((timestamp, (env.SEED + series) % 97))
        expected = {key: sorted(values) for key, values in expected.items()}
        ledger.update(seed_request_rows=sum(record['rows'] for record in records),
                      seed_unique_rows=sum(len(values) for values in expected.values()),
                      expected_series=[{'metric': dict(key), 'values': values}
                                       for key, values in sorted(expected.items())],
                      expected_series_count=cardinality, evaluation_ms=evaluation_ms)
        if not (len(expected) == cardinality and response['status'] == 'success'
                and response['data']['resultType'] == 'matrix'
                and not any(key in response for key in ('warnings', 'infos', 'error', 'errorType'))):
            raise ValueError('incomplete seed identities or wrong response envelope')
        series = response['data']['result']
        if not isinstance(series, list):
            raise ValueError('matrix result is not a list')
        actual = {}
        for item in series:
            if set(item) != {'metric', 'values'} or not isinstance(item['metric'], dict):
                raise ValueError('wrong matrix series shape')
            key = tuple(sorted(item['metric'].items()))
            if key in actual or key not in expected:
                raise ValueError('duplicate or unexpected series labels')
            values = item['values']
            if not isinstance(values, list) or not values:
                raise ValueError('series has no sample rows')
            points = []
            for point in values:
                if not isinstance(point, list) or len(point) != 2:
                    raise ValueError('wrong matrix sample shape')
                timestamp, value = point
                if not (isinstance(timestamp, (int, float, Decimal)) and not isinstance(timestamp, bool)
                        and isinstance(value, str)):
                    raise ValueError('wrong timestamp or value type')
                millis = Decimal(str(timestamp)) * 1000
                value = Decimal(value)
                if not (millis.is_finite() and millis == millis.to_integral_value() and value.is_finite()):
                    raise ValueError('nonfinite sample or fractional millisecond')
                points.append((int(millis), value))
            if not all(right[0] > left[0] for left, right in zip(points, points[1:])):
                raise ValueError('sample timestamps are duplicated or unordered')
            actual[key] = points
        ledger.update(observed_series_count=len(actual),
                      observed_unique_rows=sum(len(points) for points in actual.values()))
        if actual != expected:
            raise ValueError('matrix identities, timestamps or values differ from seed')
        ledger.update(verified=True, complete_seed=True)
    except (KeyError, TypeError, ValueError, IndexError, InvalidOperation) as error:
        ledger['error'] = f'metrics seed matrix mismatch: {type(error).__name__}: {error}'
    return ledger


def trace_seed_expected(records):
    expected, sequences = {}, set()
    if not records:
        raise ValueError('no trace seed receipts')
    tenant, cardinality, age = (records[0][key] for key in ('tenant', 'cardinality', 'age_seconds'))
    if not isinstance(cardinality, int) or isinstance(cardinality, bool) or cardinality < 1:
        raise ValueError('invalid trace seed cardinality')
    if len(records) != math.ceil(cardinality / 10):
        raise ValueError('missing trace seed receipts')
    for record in records:
        sequence, stamp = record['sequence'], record['timestamp_ns']
        if (not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0
                or sequence in sequences or not isinstance(stamp, int) or isinstance(stamp, bool)
                or not 0 <= stamp <= 2**64 - 1 - 1000009
                or (record['tenant'], record['cardinality'], record['age_seconds']) != (tenant, cardinality, age)
                or type(record['status']) is not int or not 200 <= record['status'] < 300
                or type(record['rows']) is not int or record['rows'] != 100):
            raise ValueError('invalid or duplicate trace seed receipt')
        sequences.add(sequence)
        for index in range(10):
            series = (sequence * 10 + index) % cardinality
            trace_id = hashlib.sha256(f'{env.SEED}-{tenant}-{sequence}-{series}'.encode()).hexdigest()[:32]
            fact = {'timestamp_ns': stamp, 'series': str(series)}
            if trace_id in expected and expected[trace_id] != fact:
                raise ValueError('conflicting trace seed identity')
            expected[trace_id] = fact
    if sorted(sequences) != list(range(min(sequences), min(sequences) + len(records))):
        raise ValueError('seed sequences are not distinct and consecutive')
    return expected


def trace_seed_expected_rows(trace_id, fact):
    # Complete query-visible OTLP rows, independently specified from seed facts.
    return [{'resource': {'attributes': [
                {'key': 'series', 'value': {'stringValue': fact['series']}},
                {'key': 'service.name', 'value': {'stringValue': 'envelope'}}], 'droppedAttributesCount': 0},
             'scope': {'name': '', 'version': '', 'attributes': [], 'droppedAttributesCount': 0},
             'resourceSchemaUrl': '', 'scopeSchemaUrl': '',
             'span': {'traceId': trace_id, 'spanId': (index + 1).to_bytes(8, 'big').hex(),
                      'parentSpanId': '', 'traceState': '', 'flags': 0, 'name': 'envelope',
                      'kind': 'SPAN_KIND_UNSPECIFIED', 'startTimeUnixNano': str(fact['timestamp_ns'] + index),
                      'endTimeUnixNano': str(fact['timestamp_ns'] + index + 1000000),
                      'attributes': [], 'droppedAttributesCount': 0, 'events': [], 'droppedEventsCount': 0,
                      'links': [], 'droppedLinksCount': 0, 'status': {'code': 'STATUS_CODE_UNSET', 'message': ''}}}
            for index in range(env.POINTS)]


def trace_seed_rows(response):
    # Both by-ID APIs use OTLP JSON. Normalize omitted protobuf defaults and
    # grouping/order only; keep all other fields for the whole-row comparison.
    if (not isinstance(response, dict) or set(response) - {'trace', 'metrics', 'status', 'message'}
            or response.get('status', 'COMPLETE') != 'COMPLETE' or response.get('message', '') != ''):
        raise ValueError('partial or invalid trace response')
    trace = response['trace']
    if set(trace) != {'resourceSpans'} or not isinstance(trace['resourceSpans'], list):
        raise ValueError('invalid trace envelope')
    rows, identities = [], set()
    for group in trace['resourceSpans']:
        if (set(group) - {'resource', 'scopeSpans', 'schemaUrl'}
                or not isinstance(group['scopeSpans'], list) or not group['scopeSpans']):
            raise ValueError('unexpected resource group fields')
        resource = {'attributes': [], 'droppedAttributesCount': 0, **group.get('resource', {})}
        for scoped in group['scopeSpans']:
            if (set(scoped) - {'scope', 'spans', 'schemaUrl'}
                    or not isinstance(scoped['spans'], list) or not scoped['spans']):
                raise ValueError('unexpected scope group fields')
            scope = {'name': '', 'version': '', 'attributes': [], 'droppedAttributesCount': 0, **scoped.get('scope', {})}
            for owner in (resource, scope):
                attributes = owner['attributes']
                keys = [attribute['key'] for attribute in attributes]
                if len(keys) != len(set(keys)) or not all(isinstance(key, str) for key in keys):
                    raise ValueError('duplicate or invalid trace attributes')
                owner['attributes'] = sorted(attributes, key=lambda attribute: attribute['key'])
            for value in scoped['spans']:
                span = {'parentSpanId': '', 'traceState': '', 'flags': 0, 'kind': 'SPAN_KIND_UNSPECIFIED',
                        'attributes': [], 'droppedAttributesCount': 0, 'events': [], 'droppedEventsCount': 0,
                        'links': [], 'droppedLinksCount': 0, 'status': {}, **value}
                span['status'] = {'code': 'STATUS_CODE_UNSET', 'message': '', **span['status']}
                for key, size in (('traceId', 16), ('spanId', 8), ('parentSpanId', 8)):
                    if key == 'parentSpanId' and span[key] == '':
                        continue
                    decoded = base64.b64decode(span[key], validate=True)
                    if len(decoded) != size:
                        raise ValueError('invalid trace identity length')
                    span[key] = decoded.hex()
                identity = span['traceId'], span['spanId']
                if identity in identities:
                    raise ValueError('duplicate trace span')
                identities.add(identity)
                for key in ('flags', 'droppedAttributesCount', 'droppedEventsCount', 'droppedLinksCount'):
                    if type(span[key]) is not int:
                        raise ValueError('invalid span counter type')
                if type(resource['droppedAttributesCount']) is not int or type(scope['droppedAttributesCount']) is not int:
                    raise ValueError('invalid attribute counter type')
                rows.append({'resource': resource, 'scope': scope,
                             'resourceSchemaUrl': group.get('schemaUrl', ''),
                             'scopeSchemaUrl': scoped.get('schemaUrl', ''), 'span': span})
    return sorted(rows, key=lambda row: (row['span']['traceId'], row['span']['spanId']))


def log_seed_ledger(records, response, limit):
    # Both pinned backends discover these labels for the envelope log lines.
    expected = {
        (tuple(sorted({**stream['stream'], 'service_name': 'envelope',
                       'detected_level': 'unknown'}.items())), *entry)
        for record in records for stream in record['request']['streams']
        for entry in stream['values']}
    streams = response['data']['result']
    keys = [tuple(sorted(stream['stream'].items())) for stream in streams]
    actual = [(key, *entry) for key, stream in zip(keys, streams) for entry in stream['values']]
    unique = set(actual)
    complete = 0 < len(expected) <= limit
    verified = (response['status'] == 'success' and response['data']['resultType'] == 'streams'
                and complete and len(keys) == len(set(keys))
                and all(stream['values'] for stream in streams)
                and len(actual) == len(unique) and unique == expected
                and all([int(entry[0]) for entry in stream['values']]
                        == sorted((int(entry[0]) for entry in stream['values']), reverse=True)
                        for stream in streams))
    return {'seed_unique_rows': len(expected), 'query_limit': limit,
            'expected_query_rows': len(expected), 'observed_query_rows': len(actual),
            'complete_seed': complete, 'verified': verified}


def log_seed_queries(records, limit):
    # Keep each series in one query, including rows from repeated seed requests.
    series = {}
    for record in records:
        for stream in record['request']['streams']:
            series.setdefault(stream['stream']['series'], []).append(stream)
    groups, streams, rows = [], [], 0
    for _, seeded in sorted(series.items()):
        count = len({tuple(entry) for stream in seeded for entry in stream['values']})
        if count > limit:
            raise RuntimeError('seed series exceeds the bounded query limit')
        if rows + count > limit:
            groups.append(streams)
            streams, rows = [], 0
        streams.extend(seeded)
        rows += count
    if streams:
        groups.append(streams)
    for streams in groups:
        labels = sorted({stream['stream']['series'] for stream in streams})
        stamps = [int(entry[0]) for stream in streams for entry in stream['values']]
        selector = '{job="envelope",series=~' + json.dumps('^(' + '|'.join(re.escape(label) for label in labels) + ')$') + '}'
        query = env.urllib.parse.urlencode({'query': selector, 'limit': limit, 'direction': 'backward',
                                          'start': min(stamps), 'end': max(stamps) + 1})
        yield [{'request': {'streams': streams}}], '/loki/api/v1/query_range?' + query


def resource_sample(deployment, sample):
    sample['cpu_usec'], sample['throttled_usec'] = {}, {}
    sample['load_generator_cpu_seconds'] = time.process_time()
    ticks = [int(value) for value in pathlib.Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
    sample['host_cpu_seconds'] = (sum(ticks[:8]) - ticks[3] - ticks[4]) / os.sysconf('SC_CLK_TCK')
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
    result = {'cpu_seconds_by_role': {role: value / 1e6 for role, value in cpu.items()},
            'cpu_seconds_total': sum(value for role, value in cpu.items() if role != 'minio') / 1e6,
            'accounting_scope': RESOURCE_ACCOUNTING_SCOPE,
            'load_generator_cpu_seconds': samples[-1].get('load_generator_cpu_seconds', 0) - samples[0].get('load_generator_cpu_seconds', 0),
            'throttled_seconds_by_role': {role: value / 1e6 for role, value in throttle.items()},
            'rss_kib_simultaneous_peak': max(sum(value for role, value in s['rss_kib'].items()
                                                if role != 'minio') for s in samples),
            'object_store': {'role': 'minio', 'cpu_seconds': cpu.get('minio', 0) / 1e6,
                             'throttled_seconds': throttle.get('minio', 0) / 1e6,
                             'rss_kib_peak': max(s['rss_kib'].get('minio', 0) for s in samples)},
            's3_requests': requests, 's3_read_bytes': read_bytes, 's3_write_bytes': write_bytes}
    # MinIO remains a controlled process when checking unrelated host activity.
    if all('host_cpu_seconds' in sample for sample in samples):
        def external_cores(first, following):
            elapsed = following['time_unix'] - first['time_unix']
            application = sum(max(0, value - first['cpu_usec'].get(role, value))
                              for role, value in following['cpu_usec'].items()) / 1e6
            generator = following.get('load_generator_cpu_seconds', 0) - first.get('load_generator_cpu_seconds', 0)
            return max(0, (following['host_cpu_seconds'] - first['host_cpu_seconds'] - application - generator) / elapsed)
        average = external_cores(samples[0], samples[-1])
        windows = [external_cores(samples[n], samples[n + 10]) for n in range(len(samples) - 10)]
        peak = max(windows, default=average)
        result.update(external_cpu_cores_mean=average, external_cpu_cores_peak_window=peak,
                      host_activity_qualified=average <= 2 and peak <= 4)
    return result


def configure_all(services, roles, admin_ports, signal):
    """Reuse a shipped all target without changing the aggregate budget."""
    names = [name for name in services if name.startswith(signal + '-')]
    application_cpu = sum(float(services[name]['cpus']) for name in names)
    service = services[signal + '-distributor']
    port = admin_ports[signal + '-distributor']
    if signal == 'logs':
        service['volumes'].extend(volume for volume in services['logs-block-builder']['volumes']
                                  if volume['target'] == '/var/lib/krabka')
    for name in names:
        del services[name]
    binary = {'logs': 'krabka-observability', 'metrics': 'krabka-metrics-service'}.get(signal, 'krabka-' + signal)
    service.update(command=[binary, f'--config.file=/etc/krabka/{signal}-all.yaml'],
                   cpus=application_cpu, mem_limit=f'{application_cpu:g}g',
                   stop_grace_period='180s')
    query_port = {'metrics': 9090, 'logs': 3100, 'traces': 3200, 'profiles': 4040}[signal]
    if not any(p['target'] == query_port for p in service['ports']):
        service['ports'].append({'target': query_port, 'published': str(query_port),
                                 'host_ip': '127.0.0.1'})
    services[signal + '-all'] = service
    overrides = {
        'metrics': 'runtime-overrides: /etc/krabka/metrics-limits.yaml\n',
        'logs': 'compactor-retention-sweep-interval: 2s\n'
                'logs-limits-overrides-config: /etc/krabka/logs-limits.yaml\n',
        'traces': 'block-builder-window: 5s\nblock-builder-flush-max-records: 50000\n'
                  'block-builder-flush-max-age: 2s\ncompaction-interval: 2s\n'
                  'retention: 30s\ntraces-limits-overrides-config: /etc/krabka/traces-limits.yaml\n',
        'profiles': 'block-builder-flush-records: 4096\n'
                    'block-builder-flush-max-age: 2s\ncompactor-interval: 2s\n'
                    'hot-store-max-age: 30s\nquery-frontend-shard-width: 1h\n'
                    'profiles-limits-overrides-config: /etc/krabka/profiles-limits.yaml\n',
    }
    if signal == 'metrics':
        writer = roles / 'metrics-writer.yaml'
        writer.write_text(writer.read_text() + '\nblock-builder-flush-max-age: 2s\n'
                          'block-builder-retention-sweep-interval: 2s\ncompactor-interval: 2s\n'
                          'runtime-overrides: /etc/krabka/metrics-limits.yaml\n')
    config = roles / (signal + '-all.yaml')
    config.write_text(config.read_text() + '\n' + overrides[signal])
    return {'broker': admin_ports['broker'], signal + '-all': port}


class ComparisonDeployment(env.Deployment):
    def __init__(self, evidence, image, signal, native, profiles_target='split', deployment_target='split'):
        super().__init__(evidence, image)
        self.signal, self.native = signal, native
        self.target = profiles_target if signal == 'profiles' and deployment_target == 'split' else deployment_target
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
            if self.target == 'all':
                self.admin_ports = configure_all(services, self.evidence / 'roles', self.admin_ports, signal)
                self.role_locks = {name: threading.Lock() for name in self.admin_ports}
                env.QUERY[signal] = {'metrics': 9090, 'logs': 3100, 'traces': 3200, 'profiles': 4040}[signal]
        self.file.write_text(json.dumps(data, indent=2))

    def drain(self, signal, timeout=180):
        # Logs registers its block-builder recovery bundle before the read-only
        # querier bundle in build_service_dependencies_with_client_resource_policy.
        # Require that writer's committed frontier, even while it is still null;
        # selecting whichever consumers currently have commits could hide lag.
        writer = 0 if signal in ('logs', 'metrics') and self.target == 'all' else None
        return super().drain(signal, timeout, admin_port=self.admin_ports.get(f'{signal}-all'),
                             durable_consumer_index=writer)

    def wait_for_quiet_host(self, timeout):
        deadline = time.monotonic() + timeout
        samples = []
        while time.monotonic() < deadline:
            samples.append(resource_sample(self, {'time_unix': time.time(), 'rss_kib': {}, 'scrape_errors': []}))
            if len(samples) >= 11 and cost(samples[-11:])['host_activity_qualified']:
                (self.evidence / 'host-preflight.jsonl').write_text(''.join(json.dumps(s) + '\n' for s in samples))
                return
            time.sleep(1)
        (self.evidence / 'host-preflight.jsonl').write_text(''.join(json.dumps(s) + '\n' for s in samples))
        raise RuntimeError('host remained busy before warm-up')

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
            record = {'sequence': sequence, 'tenant': tenant, 'cardinality': cardinality, 'age_seconds': age_seconds}
            if signal in ('metrics', 'traces'):
                path, body, content, rows = write_request(signal, sequence, cardinality, tenant, age_seconds, seed_record=record)
            else:
                path, body, content, rows = write_request(signal, sequence, cardinality, tenant, age_seconds)
            started = time.monotonic()
            status, response = http(env.INGEST[signal], path, tenant, body, content)
            record.update(status=status, rows=rows, seconds=time.monotonic() - started)
            if signal == 'logs':
                record['request'] = body
            records.append(record)
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
        if signal == 'metrics':
            evaluation_ms = max(time.time_ns() // 1000000, max(record['timestamp_ms'] for record in records))
            path, _ = query_request(signal, 1800)
            path = path.split('?', 1)[0] + '?' + env.urllib.parse.urlencode({
                'query': 'envelope_samples[30m]', 'time': str(Decimal(evaluation_ms) / 1000)})
            status, response = http(env.QUERY[signal], path, tenant)
            response_file = f'{tenant}.seed-matrix.json'
            (self.evidence / response_file).write_bytes(response)
            try:
                data = json.loads(response, parse_float=Decimal)
                ledger = metrics_seed_ledger(records, data, evaluation_ms) if status == 200 else {'verified': False}
            except (ValueError, InvalidOperation):
                ledger = {'verified': False}
            ledger.update(path=path, status=status, response_file=response_file)
            (self.evidence / f'{tenant}.seed-ledger.json').write_text(json.dumps(ledger, indent=2) + '\n')
            if not ledger['verified']:
                raise RuntimeError(f'metrics seed matrix mismatch: {response_file}: {ledger.get("error", status)}')
        path, body = query_request(signal, 1800)
        status, response = http(env.QUERY[signal], path, tenant, body)
        (self.evidence / f'{tenant}.seed-query.json').write_bytes(response)
        if status != 200 or not env.has_data(signal, response):
            raise RuntimeError('seed query lost visibility')
        data = json.loads(response)
        if signal == 'metrics':
            observed = float(data['data']['result'][0]['value'][1])
            expected = sum((env.SEED + label) % 97 for label in range(cardinality))
        elif signal == 'logs':
            limit = int(env.urllib.parse.parse_qs(env.urllib.parse.urlsplit(path).query)['limit'][0])
            groups = list(log_seed_queries(records, limit))
            expected = len({(tuple(sorted(stream['stream'].items())), *entry)
                            for record in records for stream in record['request']['streams']
                            for entry in stream['values']})
            queries = []
            for index, (selected, path) in enumerate(groups):
                status, response = http(env.QUERY[signal], path, tenant)
                evidence = f'{tenant}.seed-query-{index}.json'
                (self.evidence / evidence).write_bytes(response)
                check = log_seed_ledger(selected, json.loads(response), limit) if status == 200 else {
                    'complete_seed': False, 'verified': False}
                queries.append({'path': path, 'status': status, 'response_file': evidence, **check})
                ledger = {'seed_unique_rows': expected, 'query_limit': limit,
                          'expected_query_rows': expected,
                          'observed_query_rows': sum(query.get('observed_query_rows', 0) for query in queries),
                          'query_count': len(queries), 'expected_query_count': len(groups), 'queries': queries,
                          'complete_seed': len(queries) == len(groups) and all(query['complete_seed'] for query in queries),
                          'verified': len(queries) == len(groups) and all(query['verified'] for query in queries)}
                (self.evidence / f'{tenant}.seed-ledger.json').write_text(json.dumps(ledger, indent=2))
                if not check['complete_seed'] or not check['verified']:
                    raise RuntimeError(f'seed log ledger mismatch: {ledger}')
            return
        elif signal == 'traces':
            expected = trace_seed_expected(records)
            limit = int(env.urllib.parse.parse_qs(env.urllib.parse.urlsplit(path).query)['limit'][0])
            ledger = {'verified': False, 'complete_seed': False, 'expected_traces': len(expected),
                      'expected_unique_spans': len(expected) * env.POINTS, 'by_id_verified_traces': 0,
                      'search_all_identities_verified': len(expected) <= limit,
                      'response_file': f'{tenant}.seed-traces.jsonl',
                      'native_benchmark_image': PRODUCTS['traces'][1] if self.native else None,
                      'payload_contract': 'OTLP JSON defaults verified against MODULE Tempo 3.0.3; benchmark image recorded separately.',
                      'scope': 'Untimed by-ID seed verification warms both backends; timed query/writer rates are unchanged.'}
            try:
                if not isinstance(data['traces'], list) or any(
                        not isinstance(trace['traceID'], str) or not re.fullmatch('[0-9a-f]{1,32}', trace['traceID'])
                        for trace in data['traces']):
                    raise ValueError('invalid trace seed search shape')
                ids = [trace['traceID'].zfill(32) for trace in data['traces']]
                if (len(ids) != len(set(ids)) or not set(ids) <= expected.keys()
                        or len(expected) <= limit and set(ids) != expected.keys()
                        or any(trace['rootServiceName'] != 'envelope' or trace['rootTraceName'] != 'envelope'
                               or trace['durationMs'] != 1 or len(trace['spanSets']) != 1
                               or trace['spanSets'][0]['matched'] != env.POINTS for trace in data['traces'])):
                    raise ValueError('trace seed search identities or matched counts differ')
                start = min(fact['timestamp_ns'] for fact in expected.values()) // 1000000000
                end = (max(fact['timestamp_ns'] for fact in expected.values()) + 1000009) // 1000000000 + 1
                with (self.evidence / ledger['response_file']).open('w') as output:
                    for trace_id, fact in sorted(expected.items()):
                        path = f'/api/v2/traces/{trace_id}?' + env.urllib.parse.urlencode({'start': start, 'end': end})
                        status, response = http(env.QUERY[signal], path, tenant)
                        output.write(json.dumps({'trace_id': trace_id, 'path': path, 'status': status,
                                                 'response_base64': base64.b64encode(response).decode()}) + '\n')
                        if status != 200 or trace_seed_rows(json.loads(response)) != trace_seed_expected_rows(trace_id, fact):
                            raise ValueError(f'trace seed payload differs: {trace_id} ({status})')
                        ledger['by_id_verified_traces'] += 1
                ledger.update(verified=True, complete_seed=True)
            except (ValueError, TypeError, KeyError, IndexError) as error:
                ledger['error'] = str(error)
                raise RuntimeError(f'trace seed mismatch: {error}') from error
            finally:
                (self.evidence / f'{tenant}.seed-ledger.json').write_text(json.dumps(ledger, indent=2) + '\n')
            return
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
    if args.signal == 'profiles':
        args.deployment_target = args.profiles_target if args.deployment_target == 'split' else args.deployment_target
        args.profiles_target = args.deployment_target
    env.SIGNALS = (args.signal,)
    env.write_request, env.query_request, env.http = write_request, query_request, http
    identity = json.loads(env.command('docker', 'inspect', args.image))[0]
    known_digests = {value.rsplit('@', 1)[-1] for value in (identity.get('RepoDigests') or [])}
    descriptor_digest = (identity.get('Descriptor') or {}).get('digest')
    if descriptor_digest:
        known_digests.add(descriptor_digest)
    if known_digests and args.image_digest not in known_digests:
        raise RuntimeError('supplied image manifest digest does not match the loaded image')
    report = {'schema_version': 1, 'commit': env.command('git', 'rev-parse', 'HEAD'),
              'signal': args.signal, 'seed': env.SEED, 'phase_seconds': args.seconds,
              'acknowledgements': 'API accepted; native durability contracts differ',
              'resource_accounting_scope': RESOURCE_ACCOUNTING_SCOPE,
              'object_store_budget': {'cpu': 2, 'memory_gib': 2, 'included_in_application_budget': False},
              'write_interval_seconds': 1, 'query_interval_seconds': 0.25,
              'image_commit': args.image_commit, 'image_digest': args.image_digest,
              'image_identity': identity,
              'profiles_target': args.profiles_target if args.signal == 'profiles' else None,
              'deployment_target': args.deployment_target,
              'host_activity': {'wait_seconds': args.host_wait_seconds,
                                'external_cpu_cores_mean_limit': 2, 'external_cpu_cores_window_limit': 4,
                                'window_sample_intervals': 10},
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
                deployment = ComparisonDeployment(output, args.image, args.signal, native, args.profiles_target, args.deployment_target)
                try:
                    deployment.start()
                    levels = report['workload']['writers'] if phase == 'burst' else report['workload']['cardinalities'] if phase == 'high_cardinality' else [2]
                    for level in levels:
                        writers = level if phase == 'burst' else 2
                        cardinality = level if phase == 'high_cardinality' else 1000 if args.signal == 'metrics' else 100
                        tenant = f'cardinality-{level}' if phase == 'high_cardinality' else f'burst-{level}' if phase == 'burst' else 'soak'
                        try:
                            deployment.seed(args.signal, tenant, cardinality)
                            if args.host_wait_seconds:
                                deployment.wait_for_quiet_host(args.host_wait_seconds)
                                if phase == 'high_cardinality':
                                    (output / f'{level}.host-preflight.jsonl').write_bytes(
                                        (output / 'host-preflight.jsonl').read_bytes())
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
                            result['objectives_met'] &= result['resources']['host_activity_qualified']
                            rows = result['ingest']['accepted_rows']
                            result['resources']['cpu_seconds_per_million_accepted_rows'] = result['resources']['cpu_seconds_total'] * 1e6 / rows if rows else None
                        except RuntimeError as error:
                            result = {'signal': args.signal, 'writers': writers, 'cardinality': cardinality,
                                      'objectives_met': False, 'seed_error': str(error)}
                        result.update(backend=backend, repetition=repetition + 1, phase=phase,
                                      deployment_target='native' if native else deployment.target,
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


def trace_seed_self_test():
    from unittest.mock import patch
    # These wire hashes come from the unchanged 3da encoder, at the fixed
    # clock below. The optional receipt must not alter the actual protobuf.
    goldens = [(37, 100, 0, '1fc68ccf36efca8b9c31fd5fa863b7072237eab41cc791b5f435582573bbf65f'),
               (38, 11, 30, '30dc85372798035ad92b0d5cb51782cc45d7b14a08d5db8d3712b56190c31552'),
               (0, 3, 0, '106c69bd362c791255c7e46bef0d0c32dd00ed4a766f03966197c6c8686a9a13')]
    with patch.object(time, 'time_ns', return_value=1700000000000000000):
        for sequence, cardinality, age, digest in goldens:
            receipt = {}
            actual = write_request('traces', sequence, cardinality, 'seed-test', age, seed_record=receipt)
            assert actual == write_request('traces', sequence, cardinality, 'seed-test', age)
            assert actual[0] == '/v1/traces' and actual[2:] == ('application/x-protobuf', 100)
            assert hashlib.sha256(actual[1]).hexdigest() == digest
            assert receipt == {'timestamp_ns': 1700000000000000000 - age * 1000000000}

    failures = ('missing_search_trace', 'duplicate_search_trace', 'matched_count', 'missing_span',
                'duplicate_span', 'substituted_span', 'wrong_trace', 'start', 'end', 'name', 'series', 'service',
                'extra_attribute', 'duplicate_attribute', 'parent', 'scope', 'events', 'kind',
                'boolean_counter', 'partial', 'http_error', 'malformed', 'wrong_shape')
    cases = [(100, None), (100, 'defaults'), (100, 'grouping'), (3, None), (11, None), (1100, None), (1100, 'older_missing')]
    cases += [(100, failure) for failure in failures]
    encoder = write_request
    for case_index, (cardinality, failure) in enumerate(cases):
        with tempfile.TemporaryDirectory() as directory:
            deployment = object.__new__(ComparisonDeployment)
            deployment.evidence = pathlib.Path(directory)
            deployment.native = case_index % 2 == 0
            deployment.sequence = iter(range(37, 37 + math.ceil(cardinality / 10)))
            deployment.wait_query = lambda *_: None
            events, emitted, replies = [], {}, []
            deployment.drain = lambda _signal: events.append('drain') or {'recovered': True}
            def receipt_request(signal, sequence, size, tenant, age, *, seed_record):
                request = encoder(signal, sequence, size, tenant, age, seed_record=seed_record)
                assert seed_record['timestamp_ns'] == 1700000000000000000
                for index in range(10):
                    series = (sequence * 10 + index) % size
                    trace = hashlib.sha256(f'267-{tenant}-{sequence}-{series}'.encode()).digest()[:16]
                    emitted[trace.hex()] = (trace, str(series))
                return request
            def traces_http(_port, path, _tenant, body=None, *_content):
                if path == '/v1/traces':
                    return 204, b''
                selected = sorted(emitted, reverse=True)[:1000]
                if path.startswith('/api/search?'):
                    events.append('search')
                    traces = [{'traceID': identity, 'rootServiceName': 'envelope', 'rootTraceName': 'envelope',
                               'durationMs': 1, 'spanSets': [{'matched': 10}]} for identity in selected]
                    if failure == 'missing_search_trace':
                        traces.pop()
                    elif failure == 'duplicate_search_trace':
                        traces[1] = traces[0]
                    elif failure == 'matched_count':
                        traces[0]['spanSets'][0]['matched'] = 9
                    return 200, json.dumps({'traces': traces}).encode()
                identity = path.split('/api/v2/traces/')[1].split('?')[0]
                trace, series = emitted[identity]
                bounds = env.urllib.parse.parse_qs(env.urllib.parse.urlsplit(path).query)
                assert bounds == {'start': ['1700000000'], 'end': ['1700000001']}
                replies.append(identity)
                spans = [{'traceId': base64.b64encode(trace).decode(),
                          'spanId': base64.b64encode((point + 1).to_bytes(8, 'big')).decode(),
                          'name': 'envelope', 'startTimeUnixNano': str(1700000000000000000 + point),
                          'endTimeUnixNano': str(1700000000001000000 + point)} for point in range(10)]
                # Group/span/attribute order is irrelevant, but every full row
                # must match the independent wire fixture, including resources.
                attributes = [{'key': 'service.name', 'value': {'stringValue': 'envelope'}},
                              {'key': 'series', 'value': {'stringValue': series}}]
                resource = {'attributes': attributes}
                scope = {}
                reply = {'trace': {'resourceSpans': [{'resource': resource,
                           'scopeSpans': [{'scope': scope, 'spans': spans[::-1]}]}]}}
                if failure == 'defaults':
                    reply.update(status='COMPLETE', message='')
                    for span in spans:
                        span.update(parentSpanId='', traceState='', flags=0, kind='SPAN_KIND_UNSPECIFIED',
                                    attributes=[], events=[], links=[], droppedAttributesCount=0,
                                    droppedEventsCount=0, droppedLinksCount=0,
                                    status={'code': 'STATUS_CODE_UNSET', 'message': ''})
                elif failure == 'grouping':
                    reply['trace']['resourceSpans'] = [
                        {'resource': resource, 'scopeSpans': [{'spans': [span]}]} for span in spans[::-1]]
                if failure == 'older_missing' and identity not in selected:
                    return 404, b'not found'
                if failure == 'missing_span':
                    reply['trace']['resourceSpans'][0]['scopeSpans'][0]['spans'].pop()
                elif failure == 'duplicate_span':
                    spans[1]['spanId'] = spans[0]['spanId']
                elif failure == 'substituted_span':
                    spans[0]['spanId'] = base64.b64encode((11).to_bytes(8, 'big')).decode()
                elif failure == 'wrong_trace':
                    spans[0]['traceId'] = base64.b64encode(bytes(16)).decode()
                elif failure in ('start', 'end'):
                    spans[0]['startTimeUnixNano' if failure == 'start' else 'endTimeUnixNano'] = '1700000000000000001'
                elif failure == 'name':
                    spans[0]['name'] = 'wrong'
                elif failure in ('series', 'service'):
                    attributes[1 if failure == 'series' else 0]['value']['stringValue'] = 'wrong'
                elif failure == 'extra_attribute':
                    attributes.append({'key': 'extra', 'value': {'stringValue': 'wrong'}})
                elif failure == 'duplicate_attribute':
                    attributes.append(attributes[0])
                elif failure == 'parent':
                    spans[0]['parentSpanId'] = base64.b64encode(bytes(8)).decode()
                elif failure == 'scope':
                    scope['name'] = 'wrong'
                elif failure == 'events':
                    spans[0]['events'] = [{}]
                elif failure == 'kind':
                    spans[0]['kind'] = 'SPAN_KIND_CLIENT'
                elif failure == 'boolean_counter':
                    spans[0]['flags'] = False
                elif failure == 'partial':
                    reply['status'] = 'PARTIAL'
                elif failure == 'malformed':
                    return 200, b'not-json'
                elif failure == 'wrong_shape':
                    reply['trace'] = []
                return (503 if failure == 'http_error' else 200), json.dumps(reply).encode()
            with patch.dict(globals(), {'write_request': receipt_request, 'http': traces_http}), \
                    patch.object(time, 'time_ns', return_value=1700000000000000000), \
                    patch.object(time, 'time', return_value=1700000000):
                try:
                    deployment.seed('traces', 'seed-test', cardinality)
                except RuntimeError as error:
                    assert failure not in (None, 'defaults', 'grouping') and 'trace seed mismatch' in str(error)
                else:
                    assert failure in (None, 'defaults', 'grouping')
            ledger = json.loads((deployment.evidence / 'seed-test.seed-ledger.json').read_text())
            assert ledger['verified'] == (failure in (None, 'defaults', 'grouping'))
            assert ledger['expected_traces'] == len(emitted)
            assert ledger['search_all_identities_verified'] == (len(emitted) <= 1000)
            assert events == (['search'] if deployment.native else ['drain', 'search'])
            if ledger['verified']:
                assert len(replies) == len(set(replies)) == ledger['by_id_verified_traces'] == len(emitted)
            elif failure == 'older_missing':
                assert replies[-1] not in sorted(emitted, reverse=True)[:1000]
            recorded = [json.loads(line) for line in (deployment.evidence / 'seed-test.seed.jsonl').read_text().splitlines()]
            assert all(record['timestamp_ns'] == 1700000000000000000 for record in recorded)
            if replies:
                raw = [json.loads(line) for line in (deployment.evidence / 'seed-test.seed-traces.jsonl').read_text().splitlines()]
                assert [entry['trace_id'] for entry in raw] == replies
                assert all(base64.b64decode(entry['response_base64'], validate=True) for entry in raw)
    records = [{'sequence': sequence, 'timestamp_ns': 1700000000000000000, 'tenant': 'seed-test',
                'cardinality': 20000, 'age_seconds': 0, 'status': 204, 'rows': 100} for sequence in range(2000)]
    assert len(trace_seed_expected(records)) == 20000
    for key, value in [('timestamp_ns', True), ('timestamp_ns', 1700000000000000000.0),
                       ('sequence', True), ('cardinality', True), ('status', 204.0), ('rows', 100.0)]:
        bad = [{'sequence': 0, 'timestamp_ns': 1700000000000000000, 'tenant': 'seed-test',
                'cardinality': 3, 'age_seconds': 0, 'status': 204, 'rows': 100, key: value}]
        try:
            trace_seed_expected(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f'invalid trace receipt accepted: {key}')
    nonconsecutive = [{'sequence': sequence, 'timestamp_ns': 1700000000000000000, 'tenant': 'seed-test',
                       'cardinality': 100, 'age_seconds': 0, 'status': 204, 'rows': 100}
                      for sequence in range(0, 100, 10)]
    try:
        trace_seed_expected(nonconsecutive)
    except ValueError as error:
        assert str(error) == 'seed sequences are not distinct and consecutive'
    else:
        raise AssertionError('nonconsecutive trace seed receipts accepted')
    print('trace seed controls passed: three wire goldens, complete and above-limit fixtures, 24 API negatives')


def self_test():
    trace_seed_self_test()
    from unittest.mock import patch

    records = [{'request': {'streams': [
        {'stream': {'job': 'envelope', 'series': series},
         'values': [['1', 'older'], ['2', 'newer']]} for series in ('a', 'b')]}}]
    response = {'status': 'success', 'data': {'resultType': 'streams', 'result': [
        {'stream': {'job': 'envelope', 'series': series, 'service_name': 'envelope', 'detected_level': 'unknown'},
         'values': [['2', 'newer'], ['1', 'older']]} for series in ('a', 'b')]}}
    assert log_seed_ledger(records, response, 1000) == {
        'seed_unique_rows': 4, 'query_limit': 1000, 'expected_query_rows': 4,
        'observed_query_rows': 4, 'complete_seed': True, 'verified': True}
    for corruption in ('missing', 'duplicate', 'stream', 'empty_stream', 'line', 'timestamp', 'labels', 'ordering'):
        bad = json.loads(json.dumps(response))
        streams = bad['data']['result']
        if corruption == 'missing':
            streams[0]['values'].pop()
        elif corruption == 'duplicate':
            streams[0]['values'].append(streams[0]['values'][0])
        elif corruption == 'stream':
            streams.append(streams[0])
        elif corruption == 'empty_stream':
            streams.append({'stream': {'job': 'envelope', 'series': 'unseeded'}, 'values': []})
        elif corruption == 'line':
            streams[0]['values'][0][1] = 'unseeded'
        elif corruption == 'timestamp':
            streams[0]['values'][0][0] = '3'
        elif corruption == 'labels':
            streams[0]['stream']['series'] = 'unseeded'
        else:
            streams[0]['values'].reverse()
        assert not log_seed_ledger(records, bad, 1000)['verified']
    limited = json.loads(json.dumps(response))
    for stream in limited['data']['result']:
        stream['values'] = stream['values'][:1]
    assert log_seed_ledger(records, limited, 2) == {
        'seed_unique_rows': 4, 'query_limit': 2, 'expected_query_rows': 4,
        'observed_query_rows': 2, 'complete_seed': False, 'verified': False}
    for selected in limited['data']['result']:
        tied = {'status': 'success', 'data': {'resultType': 'streams', 'result': [selected]}}
        assert not log_seed_ledger(records, tied, 1)['verified']
    wrong_boundary = json.loads(json.dumps(limited))
    wrong_boundary['data']['result'][0]['values'] = [['1', 'older']]
    assert not log_seed_ledger(records, wrong_boundary, 2)['verified']
    missing_newer = json.loads(json.dumps(response))
    missing_newer['data']['result'][1]['values'].pop(0)
    assert not log_seed_ledger(records, missing_newer, 3)['verified']

    # Exercise seed's real guard, including 200,000 rows and missing older rows.
    for cardinality, incomplete in ((100, False), (100, True), (151, False),
                                    (20000, False), (20000, True)):
        with tempfile.TemporaryDirectory() as directory:
            deployment = object.__new__(ComparisonDeployment)
            deployment.evidence = pathlib.Path(directory)
            deployment.native = True
            deployment.sequence = iter(range(math.ceil(cardinality / 100)))
            deployment.wait_query = lambda *_: None
            pushed, requests, available, observed = [], [], [], []
            def seed_http(_port, path, _tenant, body=None, *_content):
                if path == '/loki/api/v1/push':
                    pushed.append(body)
                    return 204, b''
                query = env.urllib.parse.parse_qs(env.urllib.parse.urlsplit(path).query)
                requests.append(query)
                limit = int(query['limit'][0])
                assert limit <= 1000
                if not available:
                    available.extend((stream['stream'], entry) for request in pushed
                                     for stream in request['streams'] for entry in stream['values'])
                    available.sort(key=lambda row: int(row[1][0]), reverse=True)
                    if incomplete:
                        # This still satisfies the old global latest-1,000 check.
                        del available[500 if cardinality == 100 else 1000:]
                selector = query['query'][0]
                pattern = re.compile(json.loads(selector.split('series=~', 1)[1][:-1])) if 'series=~' in selector else None
                selected = [(labels, entry) for labels, entry in available
                            if (pattern is None or pattern.fullmatch(labels['series']))
                            and int(query['start'][0]) <= int(entry[0]) < int(query['end'][0])][:limit]
                observed.append(len(selected))
                streams = {}
                for labels, entry in selected:
                    stream = streams.setdefault(labels['series'], {
                        'stream': {**labels, 'service_name': 'envelope', 'detected_level': 'unknown'}, 'values': []})
                    stream['values'].append(entry)
                return 200, json.dumps({'status': 'success', 'data': {
                    'resultType': 'streams', 'result': list(streams.values())}}).encode()
            with patch.dict(globals(), {'http': seed_http}), patch.object(env.time, 'time_ns', return_value=time.time_ns() - 1000000):
                try:
                    deployment.seed('logs', 'soak', cardinality)
                except RuntimeError as error:
                    assert incomplete and 'seed log ledger mismatch' in str(error)
                else:
                    assert not incomplete
            ledger = json.loads((deployment.evidence / 'soak.seed-ledger.json').read_text())
            assert ledger['seed_unique_rows'] == math.ceil(cardinality / 100) * 1000
            assert ledger['verified'] == (not incomplete)
            assert len(requests) == ledger['query_count'] + 1
            if cardinality == 20000:
                assert observed[0] == 1000
            assert ledger['complete_seed'] == (ledger['query_count'] == ledger['expected_query_count'])
            if not incomplete:
                assert ledger['observed_query_rows'] == ledger['expected_query_rows'] == ledger['seed_unique_rows']
                assert ledger['query_count'] == ledger['expected_query_count']
                if cardinality == 20000:
                    assert ledger['query_count'] == 200
            for query in ledger['queries']:
                raw = json.loads((deployment.evidence / query['response_file']).read_text())
                assert raw['status'] == 'success' and query['path'].startswith('/loki/api/v1/query_range?')
            recorded = [json.loads(line)['request'] for line in
                        (deployment.evidence / 'soak.seed.jsonl').read_text().splitlines()]
            assert sorted(json.dumps(request, sort_keys=True) for request in recorded) == sorted(
                json.dumps(request, sort_keys=True) for request in pushed)

    # Check the real metrics seed path against independent outgoing receipts.
    seed_cases = [(n, None) for n in (100, 1000, 1501, 5000, 20000)] + [
        (1501 if failure in ('missing_older', 'older_value', 'ordering', 'duplicate_point') else 1000, failure)
        for failure in ('zero_missing', 'canceling_values', 'missing_older', 'older_value', 'duplicate_series',
                        'extra_series', 'renamed', 'extra_label', 'timestamp', 'fractional_ms',
                        'ordering', 'duplicate_point', 'histogram', 'wrong_shape', 'warning', 'info', 'error', 'http_error')]
    encoder = write_request
    for case_index, (cardinality, failure) in enumerate(seed_cases):
        with tempfile.TemporaryDirectory() as directory:
            deployment = object.__new__(ComparisonDeployment)
            deployment.evidence = pathlib.Path(directory)
            deployment.native = case_index % 2 == 0
            deployment.sequence = iter(range(37, 37 + math.ceil(cardinality / 1000)))
            deployment.wait_query = lambda *_: None
            events, receipts = [], []
            def drain(_signal):
                events.append('drain')
                return {'recovered': True}
            deployment.drain = drain
            def receipt_request(signal, sequence, size, tenant, age, *, seed_record):
                request = encoder(signal, sequence, size, tenant, age, seed_record=seed_record)
                receipts.append((sequence, seed_record['timestamp_ms']))
                return request
            def metrics_http(_port, path, tenant, body=None, *_content):
                if path == '/api/v1/push':
                    return 204, b''
                query = env.urllib.parse.parse_qs(env.urllib.parse.urlsplit(path).query)
                if query['query'] != ['envelope_samples[30m]']:
                    return 200, json.dumps({'status': 'success', 'data': {'resultType': 'vector',
                        'result': [{'metric': {}, 'value': [1791120000, str(sum((267 + i) % 97 for i in range(cardinality)))]}]}}).encode()
                events.append('matrix')
                assert path.startswith('/prometheus/api/v1/query?' if deployment.native else '/api/v1/query?')
                assert Decimal(query['time'][0]) * 1000 >= max(stamp for _, stamp in receipts)
                expected = {}
                for sequence, stamp in reversed(receipts):
                    for index in range(1000):
                        series = (sequence * 1000 + index) % cardinality
                        expected.setdefault(series, set()).add((stamp, (267 + series) % 97))
                matrix = [{'metric': {'__name__': 'envelope_samples', 'series': str(series)},
                           'values': [[float(Decimal(stamp) / 1000), str(value)] for stamp, value in sorted(points)]}
                          for series, points in sorted(expected.items(), reverse=True)]
                historical = next((item for item in matrix if len(item['values']) > 1), None)
                if failure == 'zero_missing':
                    matrix = [item for item in matrix if item['values'][-1][1] != '0']
                elif failure == 'canceling_values':
                    matrix[0]['values'][0][1] = str(int(matrix[0]['values'][0][1]) + 1)
                    matrix[1]['values'][0][1] = str(int(matrix[1]['values'][0][1]) - 1)
                elif failure == 'missing_older':
                    historical['values'].pop(0)
                elif failure == 'older_value':
                    historical['values'][0][1] = '999'
                elif failure == 'duplicate_series':
                    matrix.append(matrix[0])
                elif failure == 'extra_series':
                    matrix.append({'metric': {'__name__': 'envelope_samples', 'series': 'unexpected'}, 'values': matrix[0]['values']})
                elif failure == 'renamed':
                    matrix[0]['metric']['__name__'] = 'renamed'
                elif failure == 'extra_label':
                    matrix[0]['metric']['extra'] = 'unexpected'
                elif failure == 'timestamp':
                    matrix[0]['values'][0][0] += 0.001
                elif failure == 'fractional_ms':
                    matrix[0]['values'][0][0] += 0.0001
                elif failure == 'ordering':
                    historical['values'].reverse()
                elif failure == 'duplicate_point':
                    historical['values'].append(historical['values'][-1])
                elif failure == 'histogram':
                    matrix[0]['histograms'] = []
                reply = {'status': 'success', 'data': {'resultType': 'matrix', 'result': matrix}}
                if failure == 'wrong_shape':
                    reply['data']['resultType'] = 'vector'
                elif failure == 'warning':
                    reply['warnings'] = ['short result']
                elif failure == 'info':
                    reply['infos'] = []
                elif failure == 'error':
                    reply = {'status': 'error', 'error': 'query failed'}
                return (500 if failure == 'http_error' else 200), json.dumps(reply).encode()
            with patch.dict(globals(), {'write_request': receipt_request, 'http': metrics_http}), \
                    patch.dict(env.QUERY, {'metrics': 9009 if deployment.native else 9090}), \
                    patch.dict(_last_ms, {}, clear=True), patch.object(time, 'time_ns', return_value=1791120000000000000):
                try:
                    deployment.seed('metrics', 'seed-test', cardinality)
                except RuntimeError as error:
                    assert failure is not None and 'metrics seed matrix mismatch' in str(error)
                else:
                    assert failure is None
            ledger = json.loads((deployment.evidence / 'seed-test.seed-ledger.json').read_text())
            assert ledger['verified'] == (failure is None)
            assert events == (['matrix'] if deployment.native else ['drain', 'matrix'])
            if failure is None:
                assert ledger['complete_seed'] and ledger['expected_series_count'] == cardinality
                assert ledger['observed_series_count'] == cardinality
                assert ledger['seed_request_rows'] == math.ceil(cardinality / 1000) * 1000
                assert ledger['observed_unique_rows'] == ledger['seed_unique_rows'] == min(1000, cardinality) * math.ceil(cardinality / 1000)
            recorded = [json.loads(line) for line in (deployment.evidence / 'seed-test.seed.jsonl').read_text().splitlines()]
            assert sorted((record['sequence'], record['timestamp_ms']) for record in recorded) == sorted(receipts)

    # Even a matching API response cannot validate a corrupt emission receipt.
    for timestamps in ([True], [1791120000000.0], [1791120000000, 1791120000000]):
        records = [{'sequence': i, 'timestamp_ms': stamp, 'cardinality': len(timestamps) * 1000,
                    'status': 204, 'rows': 1000, 'tenant': 'receipt-test', 'age_seconds': 0}
                   for i, stamp in enumerate(timestamps)]
        reply = {'status': 'success', 'data': {'resultType': 'matrix', 'result': [
            {'metric': {'__name__': 'envelope_samples', 'series': str(i)},
             'values': [[timestamps[i // 1000] / 1000, str((267 + i) % 97)]]}
            for i in range(len(timestamps) * 1000)]}}
        ledger = metrics_seed_ledger(records, reply, max(timestamps))
        assert not ledger['verified'] and 'distinct integer milliseconds' in ledger['error']

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
    sample.update(time_unix=0, host_cpu_seconds=0, load_generator_cpu_seconds=0)
    after.update(time_unix=1, host_cpu_seconds=10, load_generator_cpu_seconds=0)
    assert cost([sample, after])['host_activity_qualified'] is False
    after.update(host_cpu_seconds=1.5, load_generator_cpu_seconds=0.5)
    assert cost([sample, after])['external_cpu_cores_mean'] == 0
    assert cost([sample, after])['host_activity_qualified'] is True
    # Application and object-store peaks occur at different times. Subtracting
    # independently measured peaks would incorrectly report 100 KiB, not 300.
    samples = [
        {'cpu_usec': {'broker': 0, 'metrics-querier': 0, 'minio': 0},
         'throttled_usec': {'minio': 0},
         'rss_kib': {'broker': 80, 'metrics-querier': 20, 'minio': 900}, 's3': {}},
        {'cpu_usec': {'broker': 2_000_000, 'metrics-querier': 3_000_000, 'minio': 7_000_000},
         'throttled_usec': {'minio': 1_000_000},
         'rss_kib': {'broker': 50, 'metrics-querier': 250, 'minio': 10}, 's3': {}},
        {'cpu_usec': {'broker': 4_000_000, 'metrics-querier': 6_000_000, 'minio': 14_000_000},
         'throttled_usec': {'minio': 2_000_000},
         'rss_kib': {'broker': 40, 'metrics-querier': 160, 'minio': 600}, 's3': {}},
    ]
    result = cost(samples)
    assert result['accounting_scope'] == RESOURCE_ACCOUNTING_SCOPE
    assert result['cpu_seconds_total'] == 10
    assert result['cpu_seconds_by_role'] == {'broker': 4, 'metrics-querier': 6, 'minio': 14}
    assert result['rss_kib_simultaneous_peak'] == 300
    assert result['object_store'] == {'role': 'minio', 'cpu_seconds': 14,
                                      'throttled_seconds': 2, 'rss_kib_peak': 900}
    # Native monoliths have no broker, and the same exclusion applies.
    for sample in samples:
        del sample['cpu_usec']['broker']
        del sample['rss_kib']['broker']
        sample['cpu_usec']['mimir'] = sample['cpu_usec'].pop('metrics-querier')
        sample['rss_kib']['mimir'] = sample['rss_kib'].pop('metrics-querier')
    assert cost(samples)['cpu_seconds_total'] == 6
    assert cost(samples)['rss_kib_simultaneous_peak'] == 250
    # Counter resets still count new MinIO work, separately from the application.
    samples[-1]['cpu_usec']['minio'] = 1_000_000
    assert cost(samples)['object_store']['cpu_seconds'] == 8
    assert cost(samples)['cpu_seconds_total'] == 6
    native = [
        {'time_unix': 0, 'cpu_usec': {'mimir': 0, 'minio': 0}, 'throttled_usec': {},
         'rss_kib': {}, 's3': {}, 'host_cpu_seconds': 0},
        {'time_unix': 1, 'cpu_usec': {'mimir': 1_000_000, 'minio': 1_000_000},
         'throttled_usec': {}, 'rss_kib': {}, 's3': {}, 'host_cpu_seconds': 2},
    ]
    assert cost(native)['external_cpu_cores_mean'] == 0
    with tempfile.TemporaryDirectory() as directory:
        roles = pathlib.Path(directory)
        (roles / 'profiles-all.yaml').write_text('target: all\n')
        services = {'broker': {'cpus': 2, 'mem_limit': '2g'},
                    **{f'profiles-{role}': {'cpus': 1, 'mem_limit': '1g', 'ports': [{'target': 4040}]}
                       for role in ('distributor', 'querier', 'block-builder', 'compactor')}}
        ports = {'broker': 15000, 'profiles-distributor': 15001}
        selected = configure_all(services, roles, ports, 'profiles')
        assert set(services) == set(selected) == {'broker', 'profiles-all'}
        assert sum(float(s['cpus']) for s in services.values()) == 6
        assert services['profiles-all']['mem_limit'] == '4g'
        assert selected['profiles-all'] == 15001
        assert 'query-frontend-shard-width: 1h' in (roles / 'profiles-all.yaml').read_text()
        for signal, query_port, role_count in [('logs', 3100, 3), ('traces', 3200, 5), ('metrics', 9090, 4)]:
            (roles / (signal + '-all.yaml')).write_text('target: all\n')
            services = {'broker': {'cpus': 2, 'mem_limit': '2g'},
                        **{f'{signal}-{n}': {'cpus': 1, 'mem_limit': '1g', 'ports': [], 'volumes': []}
                           for n in ['distributor', 'block-builder', 'querier']
                           + (['compactor', 'live-store'] if signal == 'traces' else [])}}
            if signal == 'metrics':
                services['metrics-compactor'] = {'cpus': 1, 'mem_limit': '1g', 'ports': [], 'volumes': []}
                (roles / 'metrics-writer.yaml').write_text('bootstrap: broker:9092\n')
            if signal == 'logs':
                services['logs-block-builder']['volumes'] = [
                    {'source': 'logs-data', 'target': '/var/lib/krabka'}]
            selected = configure_all(services, roles,
                                     {'broker': 15000, signal + '-distributor': 15001}, signal)
            assert set(services) == set(selected) == {'broker', signal + '-all'}
            assert sum(float(s['cpus']) for s in services.values()) == role_count + 2
            assert services[signal + '-all']['mem_limit'] == f'{role_count}g'
            assert services[signal + '-all']['ports'][0]['target'] == query_port
            if signal == 'metrics':
                assert services['metrics-all']['command'][0] == 'krabka-metrics-service'
                assert 'runtime-overrides: /etc/krabka/metrics-limits.yaml' in (roles / 'metrics-writer.yaml').read_text()
            if signal == 'logs':
                assert services['logs-all']['volumes'][0]['source'] == 'logs-data'
    # Run the real phase loop with external deployment and measurement stubbed.
    for phase, seconds, maximum, levels, duration in [
        ('steady', 60, 20000, [1000], 60),
        ('high_cardinality', 60, 20000, [1000, 5000, 20000], 30),
        ('high_cardinality', 120, 20000, [1000, 5000, 20000], 60),
        ('high_cardinality', 120, 5000, [1000, 5000], 60),
        ('high_cardinality', 120, 1000, [1000], 60),
    ]:
        with tempfile.TemporaryDirectory() as directory:
            calls, seeded = [], []
            deployment_type = ComparisonDeployment
            def deployment_for_phase(evidence, *_args):
                deployment = object.__new__(deployment_type)
                deployment.evidence = evidence
                deployment.target = 'all'
                deployment.service_cpu = deployment.service_memory_gib = 6
                deployment.ids = {}
                deployment.start = lambda: evidence.mkdir(parents=True)
                deployment.seed = lambda signal, tenant, cardinality: seeded.append((tenant, cardinality))
                def quiet_host(_timeout):
                    (evidence / 'host-preflight.jsonl').write_text(json.dumps(seeded[-1]) + '\n')
                deployment.wait_for_quiet_host = quiet_host
                deployment.close = lambda: None
                return deployment
            def phase_measure(_deployment, signal, measured, warmup, writers, cardinality, **options):
                calls.append((signal, measured, warmup, writers, cardinality, options))
                return ({'duration_seconds': measured, 'objectives_met': True,
                         'ingest': {'accepted_rows': 1000 * writers * measured}, 'query': {}},
                        [], [{'time_unix': 0}, {'time_unix': measured}])
            args = argparse.Namespace(signal='metrics', profiles_target='all', deployment_target='all',
                                      image='test-image', image_digest='sha256:' + '0' * 64,
                                      image_commit='test-source', seconds=seconds, repetitions=1,
                                      phases=[phase], backends=['krabka'], max_writers=256,
                                      max_cardinality=maximum, host_wait_seconds=120,
                                      output=pathlib.Path(directory))
            with patch.dict(globals(), {'ComparisonDeployment': deployment_for_phase,
                                        'cost': lambda _: {'cpu_seconds_total': 1, 'host_activity_qualified': True}}), \
                    patch.object(env, 'command', return_value='[{}]'), patch.object(env, 'measure', phase_measure):
                run(args)
            assert calls == [
                ('metrics', duration, duration / 4, 2, level,
                 {'cold': phase == 'high_cardinality', 'interval': 1,
                  'tenant': f'cardinality-{level}' if phase == 'high_cardinality' else 'soak',
                  'check_durability': False}) for level in levels]
            output = args.output / f'1-krabka-{phase}'
            if phase == 'high_cardinality':
                assert [json.loads((output / f'{level}.host-preflight.jsonl').read_text())
                        for level in levels] == [[f'cardinality-{level}', level] for level in levels]
                assert json.loads((output / 'host-preflight.jsonl').read_text()) == [f'cardinality-{levels[-1]}', levels[-1]]
            else:
                assert not list(output.glob('*.host-preflight.jsonl'))
    print('compare-grafana self-test passed')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--signal', choices=PRODUCTS)
    p.add_argument('--image')
    p.add_argument('--image-digest')
    p.add_argument('--image-commit')
    p.add_argument('--profiles-target', choices=('split', 'all'), default='split',
                   help='Separate profiles role containers or the existing all target; same aggregate budget')
    p.add_argument('--deployment-target', choices=('split', 'all'), default='split',
                   help='Separate roles or the all target for every signal; same aggregate budget')
    p.add_argument('--host-wait-seconds', type=int, default=0,
                   help='Wait for ten host sample intervals without excessive external CPU before warm-up')
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
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', options.image_digest):
            p.error('image digest must be sha256 followed by exactly 64 lowercase hexadecimal digits')
        if options.host_wait_seconds < 0:
            p.error('host wait must be nonnegative')
        if len(set(options.phases)) != len(options.phases) or len(set(options.backends)) != len(options.backends):
            p.error('phases and backends must be unique')
        run(options)
