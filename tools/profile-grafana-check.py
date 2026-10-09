#!/usr/bin/env python3
"""Check query-only diagnostics without starting containers or profilers."""
import argparse
import base64
import copy
import importlib.util
import json
import pathlib
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('profile', pathlib.Path(__file__).with_name('profile-grafana.py'))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


def check():
    graph = {'flamegraph': {'names': ['total', 'envelope'] + [f'frame_{i}' for i in range(10)],
                           'levels': [{'values': [0, 1000, 0, 0]}, {'values': [0, 1000, 0, 1]},
                                      {'values': [value for i in range(10) for value in (0, 100, 100, i + 2)]}],
                           'total': 1000, 'maxSelf': 100}}
    wrong_total = copy.deepcopy(graph)
    wrong_total['flamegraph'].update(total=900, maxSelf=90)
    for level in wrong_total['flamegraph']['levels'][:2]:
        level['values'][1] = 900
    wrong_total['flamegraph']['levels'][2]['values'] = [value for i in range(10) for value in (0, 90, 90, i + 2)]
    wrong_stacks = copy.deepcopy(graph)
    wrong_stacks['flamegraph']['levels'][2]['values'][:8] = [0, 99, 99, 2, 0, 101, 101, 3]
    wrong_stacks['flamegraph']['maxSelf'] = 101
    cases = [(False, graph, 2, False, False), (True, graph, 2, False, False),
             (True, wrong_total, 2, False, True), (True, wrong_stacks, 2, False, True),
             (True, graph, 0, False, True), (True, graph, 2, True, True)]
    for query_only, response, attempts, unexpected_write, fails in cases:
        with tempfile.TemporaryDirectory() as directory:
            calls, seeds, closed = [], [], []
            args = argparse.Namespace(signal='profiles', mode='cpu', query_only=query_only,
                                      image='test-image', image_commit='test-commit', image_digest='test-digest',
                                      deployment_target='all', cpu_profiler='perf', profile_seconds=1, windows=1,
                                      output=pathlib.Path(directory) / 'profile')
            deployment = SimpleNamespace(start=lambda: None, close=lambda: closed.append(True),
                                         seed=lambda *values: seeds.append(values), wait_for_quiet_host=lambda _: None,
                                         pids={}, admin_ports={})
            def command(*values):
                return '[{}]' if values[:2] == ('docker', 'inspect') else '{}' if values == ('lscpu', '-J') else 'test-version'
            def measure(_deployment, signal, seconds, warmup, writers, cardinality, **options):
                calls.append((signal, seconds, warmup, writers, cardinality, options))
                path, body = profile.env.query_request(signal, 1800 if options['cold'] else 30)
                assert body['end'] - body['start'] == (1801000 if query_only else 31000)
                # One warm-up query and two measured queries use the real wrapper.
                profile.env.http(profile.env.QUERY[signal], path, 'soak', body)
                options['on_measurement']()
                for _ in range(2):
                    profile.env.http(profile.env.QUERY[signal], path, 'soak', body)
                writes = 1 if writers or unexpected_write else 0
                operations = [{'kind': 'query'}] * attempts + ([{'kind': 'write'}] if writes else [])
                return ({'ingest': {'attempts': writes, 'error_rate': 0 if writes else None},
                         'query': {'attempts': attempts, 'error_rate': 0 if attempts else None, 'empty_queries': 0}},
                        operations, [{'time_unix': 0}, {'time_unix': 1}])
            with patch.object(profile.comparison, 'ComparisonDeployment', return_value=deployment), \
                    patch.object(profile.env, 'command', side_effect=command), \
                    patch.object(profile.env, 'measure', side_effect=measure), \
                    patch.object(profile.comparison, 'http', return_value=(200, json.dumps(response).encode())), \
                    patch.object(profile, 'capture') as capture:
                failed = False
                try:
                    profile.run(args)
                except (RuntimeError, ValueError):
                    failed = True
                assert failed == fails
                assert profile.env.http is profile.comparison.http
                assert seeds == [('profiles', 'soak', 100)] and closed == [True]
                assert len(calls) == 1 and calls[0][:5] == ('profiles', 16, 15, 0 if query_only else 2, 100)
                assert calls[0][5]['cold'] == query_only and calls[0][5]['interval'] == 1
                assert calls[0][5]['check_durability'] is False
                if not fails:
                    capture.assert_called_once_with(deployment, args.output, 1, 1, cpu=True, cpu_profiler='perf')
                report = json.loads((args.output / 'profile-report.json').read_text())
                assert report['diagnostic_only'] and not report['comparison_qualified']
                assert report['writers'] == (0 if query_only else 2)
                if query_only:
                    replies = [json.loads(line) for line in (args.output / 'query-replies.jsonl').read_text().splitlines()]
                    assert len(replies) == (1 if response != graph else 3)
                    assert all(json.loads(base64.b64decode(r['response_base64'])) == response for r in replies)
                    assert all(r['started_unix'] <= r['completed_unix'] and r['status'] == 200 for r in replies)
                    if not fails:
                        assert report['verified_queries'] == 3
                else:
                    assert not (args.output / 'query-replies.jsonl').exists()
                assert (args.output / 'SHA256SUMS').is_file()
    for mode, signal in [('allocations', 'profiles'), ('cpu', 'metrics')]:
        args = argparse.Namespace(query_only=True, mode=mode, signal=signal)
        with patch.object(profile.comparison, 'ComparisonDeployment') as deployment:
            try:
                profile.run(args)
            except ValueError:
                pass
            else:
                raise AssertionError('unsupported query-only mode was accepted')
            deployment.assert_not_called()
    print('profile-grafana check passed')


if __name__ == '__main__':
    check()
