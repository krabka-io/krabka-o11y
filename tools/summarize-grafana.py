#!/usr/bin/env python3
import argparse,hashlib,json,pathlib,statistics,zipfile
parser=argparse.ArgumentParser(description='Summarize three paired Grafana comparison artifacts after verifying their archive digests.')
parser.add_argument('--evidence',type=pathlib.Path,required=True,help='Directory with SIGNAL.zip files and provenance.json')
parser.add_argument('--output',type=pathlib.Path,required=True)
args=parser.parse_args()
ROOT=args.evidence
proof=json.loads((ROOT/'provenance.json').read_text())
PRODUCTS={'metrics':'mimir','logs':'loki','traces':'tempo','profiles':'pyroscope'}
def stats(values):
    return {'median':statistics.median(values),'min':min(values),'max':max(values),'values':values}
def at(data,*keys):
    for key in keys:data=data[key]
    return data
FIELDS={'rows_per_second':('ingest','accepted_rows_per_sec'),'accepted_rows':('ingest','accepted_rows'),'ingest_p99_seconds':('ingest','latency_seconds','p99'),'query_p99_seconds':('query','latency_seconds','p99'),'query_attempts':('query','attempts'),'cpu_seconds':('resources','cpu_seconds_total'),'rss_kib':('resources','rss_kib_simultaneous_peak'),'cpu_seconds_per_million_rows':('resources','cpu_seconds_per_million_accepted_rows'),'generator_cpu_seconds':('resources','load_generator_cpu_seconds'),'s3_requests':('resources','s3_requests'),'s3_read_bytes':('resources','s3_read_bytes'),'s3_write_bytes':('resources','s3_write_bytes'),'duration_seconds':('duration_seconds',)}
def distribution(points):
    result={field:stats([at(e,*keys) for e in points]) for field,keys in FIELDS.items()}
    result['query_attempts_per_second']=stats([e['query']['attempts']/e['duration_seconds'] for e in points])
    result['average_cpu_cores']=stats([e['resources']['cpu_seconds_total']/e['duration_seconds'] for e in points])
    result['driver_average_cpu_cores']=stats([e['resources']['load_generator_cpu_seconds']/e['duration_seconds'] for e in points])
    return result
summary={'schema_version':1,'scope':'Fixed deployment-shape, single-node API-accepted performance; different durability contracts','aggregation':'median/min/max and raw values of three repetitions, no confidence intervals','signals':{}}
reports=[]
for signal,product in PRODUCTS.items():
    archive_path=ROOT/(signal+'.zip')
    with archive_path.open('rb') as source: digest=hashlib.file_digest(source,'sha256').hexdigest()
    assert digest==proof['signals'][signal]['artifact_sha256'],(signal,'archive digest')
    with zipfile.ZipFile(archive_path) as archive:report=json.loads(archive.read('comparison-report.json'))
    assert report['commit']==proof['signals'][signal]['harness_commit'],(signal,'harness commit')
    assert proof['signals'][signal]['job_conclusion']=='success',signal
    reports.append(report)
    assert report['phase_seconds']==60 and report['signal']==signal
    assert report['write_interval_seconds']==1 and report['query_interval_seconds']==0.25
    coverage=[]
    with zipfile.ZipFile(ROOT/(signal+'.zip')) as archive:
        for e in report['entries']:
            if 'resources' not in e:continue
            level=e['writers'] if e['phase']=='burst' else e['cardinality'] if e['phase']=='high_cardinality' else 2
            name=f'{e["repetition"]}-{e["backend"]}-{e["phase"]}/{level}.telemetry.jsonl'
            samples=[json.loads(line) for line in archive.read(name).splitlines()]
            span=samples[-1]['time_unix']-samples[0]['time_unix'] if len(samples)>1 else 0
            valid=len(samples)>=2 and span/e['duration_seconds']>=0.9
            e['resource_coverage']={'sample_count':len(samples),'sampled_seconds':span,'coverage_fraction':span/e['duration_seconds'],'valid':valid}
            coverage.append({'repetition':e['repetition'],'backend':e['backend'],'phase':e['phase'],'level':level,**e['resource_coverage']})
    findings=[{k:e[k] for k in ('backend','repetition','phase','cardinality','seed_error')} for e in report['entries'] if e.get('seed_error','').startswith('seed value mismatch')]
    result={'host':report['host'],'harness_commit':report['commit'],'resource_coverage':coverage,'seed_value_mismatches':findings,'correctness_disqualified':bool(findings),'backends':{}}
    for backend in ('krabka',product):
        entries=[e for e in report['entries'] if e['backend']==backend]
        steady=[e for e in entries if e['phase']=='steady'];assert sorted(e['repetition'] for e in steady)==[1,2,3]
        assert all(e.get('resource_coverage',{}).get('valid') for e in steady),(signal,backend,'steady cost unavailable')
        row={'steady_objectives_met':all(e['objectives_met'] for e in steady),'steady':distribution(steady),'service_cpu':steady[0]['service_cpu'],'service_memory_gib':steady[0]['service_memory_gib']}
        for phase,key in [('burst','writers'),('high_cardinality','cardinality')]:
            steps=[e for e in entries if e['phase']==phase]
            passing=[level for level in sorted({e[key] for e in steps}) if len([e for e in steps if e[key]==level])==3 and all(e['objectives_met'] for e in steps if e[key]==level)]
            last=max(passing,default=None)
            failed=[{k:e[k] for k in ('repetition',key,'seed_error','ingest','query','telemetry','resources','resource_coverage') if k in e}|{'oom_killed_roles':[name for name,state in e.get('container_states',{}).items() if state.get('OOMKilled')]} for e in steps if not e['objectives_met']]
            row[phase]={'passing_levels':passing,'highest_consistently_passing_tested_level':last,'per_repetition':[], 'failed_points':failed}
            for rep in (1,2,3):
                group=[e for e in steps if e['repetition']==rep]
                passed=[e[key] for e in group if e['objectives_met']]
                row[phase]['per_repetition'].append({'repetition':rep,'passing_levels':passed,'highest_passing_tested_level':max(passed,default=None),'failed_levels':[e[key] for e in group if not e['objectives_met']]})
            if last is not None:
                points=[e for e in steps if e[key]==last]
                if all(e['resource_coverage']['valid'] for e in points):row[phase]['last_passing']=distribution(points)
        result['backends'][backend]=row
    common=set(result['backends']['krabka']['burst']['passing_levels'])&set(result['backends'][product]['burst']['passing_levels'])
    result['highest_common_burst_writers']=max(common,default=None)
    if common:
        level=max(common)
        result['common_burst']={backend:distribution([e for e in report['entries'] if e['backend']==backend and e['phase']=='burst' and e['writers']==level]) for backend in ('krabka',product) if all(e['resource_coverage']['valid'] for e in report['entries'] if e['backend']==backend and e['phase']=='burst' and e['writers']==level)}
    k=result['backends']['krabka']['steady'];g=result['backends'][product]['steady']
    result['steady_krabka_divided_by_native']={field:k[field]['median']/g[field]['median'] if g[field]['median'] else None for field in ('average_cpu_cores','rss_kib','query_p99_seconds','ingest_p99_seconds')}
    if findings:
        result['diagnostic_steady_krabka_divided_by_native']=result['steady_krabka_divided_by_native']
        result['steady_krabka_divided_by_native']=None
    summary['signals'][signal]=result
assert len(reports)==4
assert all(r['dataset']==reports[0]['dataset'] for r in reports)
assert len({r['image_commit'] for r in reports})==1 and len({r['image_digest'] for r in reports})==1
summary.update(harness_commits={r['signal']:r['commit'] for r in reports},dataset=reports[0]['dataset'],image_commit=reports[0]['image_commit'],image_digest=reports[0]['image_digest'])
args.output.write_text(json.dumps(summary,indent=2)+'\n')
for signal,result in summary['signals'].items():
    for backend,row in result['backends'].items():
        s=row['steady'];b=row['burst'];print(signal,backend,'steady',row['steady_objectives_met'],'p99 ms',round(s['query_p99_seconds']['median']*1000,2),'avg cores',round(s['average_cpu_cores']['median'],3),'rss MiB',round(s['rss_kib']['median']/1024,2),'burst',b['highest_consistently_passing_tested_level'],'rows/sec',round(b.get('last_passing',{}).get('rows_per_second',{}).get('median',0)), 'cardinality',row['high_cardinality']['highest_consistently_passing_tested_level'])
