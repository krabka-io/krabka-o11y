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
            for mode, phase, cardinality in [('dwarf,16384', 'steady', None),
                                              ('fp', 'steady', None),
                                              ('dwarf,16384', 'high_cardinality', 20000),
                                              ('fp', 'high_cardinality', 20000)]:
                with self.subTest(target=target, mode=mode, phase=phase), tempfile.TemporaryDirectory() as directory:
                    output = pathlib.Path(directory) / 'profile'
                    roles = ['broker', *apps]
                    deployment = mock.Mock(admin_ports=dict.fromkeys(roles, 15000),
                                           pids={role: 100 + i for i, role in enumerate(roles)})
                    args = types.SimpleNamespace(signal='metrics', deployment_target=target,
                        mode='cpu', cpu_profiler='perf', app_call_graph=mode, output=output,
                        phase=phase, cardinality=cardinality,
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
                            mock.patch.object(profiler.env, 'measure', side_effect=measure) as measurements, \
                            mock.patch.object(profiler.subprocess, 'run', side_effect=record) as records:
                        profiler.run(args)

                    expected_modes = {'broker': 'dwarf,16384', **dict.fromkeys(apps, mode)}
                    report = json.loads((output / 'profile-report.json').read_text())
                    self.assertEqual(report['perf_call_graph_by_role'], expected_modes)
                    expected_cardinality = cardinality or 1000
                    self.assertEqual((report['phase'], report['cardinality']), (phase, expected_cardinality))
                    deployment.seed.assert_called_once_with('metrics', 'soak', expected_cardinality)
                    self.assertEqual(measurements.call_args.args[5], expected_cardinality)
                    self.assertEqual(measurements.call_args.kwargs['cold'], phase == 'high_cardinality')
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


if __name__ == '__main__':
    unittest.main()
