#!/usr/bin/env python3
"""Check diagnostic perf capture and reports without external processes."""
import importlib.util
import json
import os
import pathlib
import subprocess
import tempfile
import types
import unittest
from unittest import mock
import zipfile

spec = importlib.util.spec_from_file_location('profiler', pathlib.Path(__file__).with_name('profile-grafana.py'))
profiler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profiler)


class PerfProfileTest(unittest.TestCase):
    def test_capture_modes_and_reports(self):
        for target, apps in [('all', ['metrics-all']),
                             ('split', ['metrics-distributor', 'metrics-block-builder', 'metrics-querier'])]:
            for mode in ('dwarf,16384', 'fp'):
                with self.subTest(target=target, mode=mode), tempfile.TemporaryDirectory() as directory:
                    output = pathlib.Path(directory) / 'profile'
                    roles = ['broker', *apps]
                    deployment = mock.Mock(admin_ports=dict.fromkeys(roles, 15000),
                                           pids={role: 100 + i for i, role in enumerate(roles)})
                    args = types.SimpleNamespace(signal='metrics', deployment_target=target,
                        phase='steady', cardinality=None,
                        mode='cpu', cpu_profiler='perf', app_call_graph=mode, output=output,
                        image='diagnostic-image', image_commit='source', image_digest='digest',
                        profile_seconds=1, windows=1)

                    def command(*argv):
                        if argv[:2] == ('docker', 'inspect'):
                            return '[{}]'
                        if argv[:2] == ('lscpu', '-J'):
                            return '{"lscpu": []}'
                        if argv[0] == 'curl' and '--output' in argv:
                            with zipfile.ZipFile(argv[argv.index('--output') + 1], 'w'):
                                pass
                        if 'report' in argv:
                            return '# Samples: 3\n'
                        return 'mock output'

                    def record(argv, **kwargs):
                        pathlib.Path(argv[argv.index('-o') + 1]).write_bytes(b'raw perf data')
                        return subprocess.CompletedProcess(argv, 0, 'record stdout', 'record stderr')

                    def measure(*positional, **kwargs):
                        kwargs['on_measurement']()
                        return {'ingest': {'error_rate': 0},
                                'query': {'error_rate': 0, 'empty_queries': 0}}, [], []

                    with mock.patch.object(profiler.comparison, 'ComparisonDeployment', return_value=deployment), \
                            mock.patch.object(profiler.env, 'command', side_effect=command) as commands, \
                            mock.patch.object(profiler.env, 'measure', side_effect=measure), \
                            mock.patch.object(profiler.subprocess, 'run', side_effect=record) as records:
                        profiler.run(args)

                    expected_modes = {'broker': 'dwarf,16384', **dict.fromkeys(apps, mode)}
                    report = json.loads((output / 'profile-report.json').read_text())
                    self.assertEqual(report['perf_call_graph_by_role'], expected_modes)
                    self.assertEqual((report['diagnostic_only'], report['comparison_qualified']), (True, False))
                    actual_modes = {call.args[0][call.args[0].index('-p') + 1]:
                                    call.args[0][call.args[0].index('--call-graph') + 1]
                                    for call in records.call_args_list}
                    self.assertEqual(len(records.call_args_list), len(roles))
                    self.assertEqual(actual_modes, {str(deployment.pids[role]): mode
                                                    for role, mode in expected_modes.items()})
                    actual_reports = [call.args for call in commands.call_args_list if 'report' in call.args]
                    expected_reports = []
                    for role, pid in deployment.pids.items():
                        raw = output / f'{role}.1.cpu.perf.data'
                        self.assertGreater(raw.stat().st_size, 0)
                        self.assertTrue(raw.with_name(raw.name + '.record.txt').is_file())
                        for kind, graph, children in [('top', 'none', '--no-children'),
                                                      ('cum', 'graph,0.5,caller', '--children')]:
                            expected_reports.append((f'/proc/{pid}/root', graph, (children,)))
                            self.assertGreater(raw.with_name(raw.name + '.' + kind + '.txt').stat().st_size, 0)
                    self.assertCountEqual([(argv[argv.index('--symfs') + 1],
                                            argv[argv.index('--call-graph') + 1],
                                            tuple(arg for arg in argv if arg in ('--children', '--no-children')))
                                           for argv in actual_reports], expected_reports)
                    deployment.close.assert_called_once_with()

        with tempfile.TemporaryDirectory() as directory:
            raw = pathlib.Path(directory) / 'empty.perf.data'
            for reports in [['# Samples: 0\n'], ['# Samples: 3\n', 'no sample header']]:
                with self.subTest(reports=reports), \
                        mock.patch.object(profiler.env, 'command', side_effect=reports), \
                        self.assertRaisesRegex(RuntimeError, 'empty CPU profile'):
                    profiler.report_perf(os.getpid(), raw)


    def test_workload_phase_and_cardinality(self):
        cases = [('metrics', 'steady', None, 1000, False, 'cpu'),
                 ('logs', 'steady', None, 100, False, 'cpu'),
                 ('traces', 'steady', None, 100, False, 'cpu'),
                 ('profiles', 'steady', None, 100, False, 'cpu'),
                 ('metrics', 'high_cardinality', 20000, 20000, True, 'cpu'),
                 ('metrics', 'steady', 5000, 5000, False, 'cpu'),
                 ('metrics', 'high_cardinality', 20000, 20000, True, 'allocations')]
        for signal, phase, requested, cardinality, cold, mode in cases:
            with self.subTest(signal=signal, phase=phase, cardinality=requested, mode=mode), \
                    tempfile.TemporaryDirectory() as directory:
                output = pathlib.Path(directory) / 'profile'
                deployment = mock.Mock()
                args = types.SimpleNamespace(signal=signal, phase=phase, cardinality=requested,
                    deployment_target='all', mode=mode, cpu_profiler='pprof', app_call_graph='fp',
                    output=output, image='diagnostic-image', image_commit='source', image_digest='digest',
                    profile_seconds=55, windows=3)

                def command(*argv):
                    if argv[:2] == ('docker', 'inspect'):
                        return '[{}]'
                    if argv[:2] == ('lscpu', '-J'):
                        return '{"lscpu": []}'
                    return 'mock output'

                result = {'ingest': {'error_rate': 0},
                          'query': {'error_rate': 0, 'empty_queries': 0}}
                with mock.patch.object(profiler.comparison, 'ComparisonDeployment', return_value=deployment), \
                        mock.patch.object(profiler.env, 'command', side_effect=command), \
                        mock.patch.object(profiler, 'configure_allocations', return_value=None), \
                        mock.patch.object(profiler, 'capture') as capture, \
                        mock.patch.object(profiler.env, 'measure', return_value=(result, [], [])) as measure:
                    profiler.run(args)
                    measure.call_args.kwargs['on_measurement']()
                    capture.assert_called_once_with(deployment, output, 55, 3,
                        cpu=mode == 'cpu', cpu_profiler='pprof', app_call_graph='fp')

                deployment.seed.assert_called_once_with(signal, 'soak', cardinality)
                deployment.wait_for_quiet_host.assert_called_once_with(120)
                self.assertEqual(measure.call_args.args,
                                 (deployment, signal, 180 if mode == 'cpu' else 70, 15, 2, cardinality))
                self.assertEqual({key: measure.call_args.kwargs[key]
                                  for key in ('cold', 'interval', 'check_durability')},
                                 {'cold': cold, 'interval': 1, 'check_durability': False})
                report = json.loads((output / 'profile-report.json').read_text())
                self.assertEqual((report['phase'], report['cardinality']), (phase, cardinality))
                deployment.close.assert_called_once_with()
                if signal == 'metrics':
                    path, body = profiler.comparison.query_request(signal, 1800 if cold else 30)
                    query = profiler.env.urllib.parse.parse_qs(profiler.env.urllib.parse.urlsplit(path).query)
                    self.assertEqual(query['query'], ['sum(last_over_time(envelope_samples[30m]))'
                                                     if cold else 'sum(envelope_samples)'])
                    self.assertIsNone(body)

        script = pathlib.Path(profiler.__file__)
        help_result = subprocess.run(['python3', str(script), '--help'],
                                     capture_output=True, text=True, check=True)
        self.assertLess(len(help_result.stdout), 8192)
        for cardinality in (0, 20001):
            with self.subTest(invalid_cardinality=cardinality), tempfile.TemporaryDirectory() as directory:
                output = pathlib.Path(directory) / 'profile'
                rejected = subprocess.run(['python3', str(script), '--signal', 'metrics',
                    '--phase', 'high_cardinality', '--cardinality', str(cardinality),
                    '--image', 'unused', '--image-commit', 'unused', '--image-digest', 'unused',
                    '--output', str(output)], capture_output=True, text=True, check=False)
                self.assertEqual(rejected.returncode, 2)
                self.assertIn('--cardinality must be between 1 and 20000', rejected.stderr)
                self.assertLess(len(rejected.stderr), 8192)
                self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
