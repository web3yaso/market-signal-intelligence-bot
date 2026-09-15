"""Evaluation keeps gold outside the API input path and reports real vs fixture honestly."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from collections import Counter
from pathlib import Path

from extract import APIExtractor, FixtureExtractor, PROMPT_ID, load_env
from ingest import replay
from main import Pipeline, ROOT, save_run
from schema import read_jsonl

FIELDS = ('resource', 'quantity', 'price', 'target_message', 'assertion_mode')
SIGNALS = {'supply', 'demand', 'set_remaining', 'reprice', 'retract'}


def leaves(value, prefix=''):
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            out.update(leaves(v, prefix + '.' + k if prefix else k))
        return out
    return {prefix: value}


def normalize_state(events):
    """IDs are implementation details; origin + side + resource define comparison identity."""
    normalized = []
    for event in events:
        event = {k:v for k,v in event.items() if k != 'event_id'}
        event['supporting_message_ids'] = sorted(event['supporting_message_ids'], key=lambda r:(r['channel_id'],r['message_id']))
        normalized.append(event)
    return sorted(normalized, key=lambda e: json.dumps([e['origin_message'],e['side'],e['resource']], sort_keys=True))


def score_observations(expected, actual):
    # Ignore explanatory records are optional; hallucinated non-ignore outputs remain errors.
    expected = [o for o in expected if o['action'] != 'ignore']
    actual = [o for o in actual if o['action'] != 'ignore']
    pairs = []
    remaining = set(range(len(actual)))
    # Maximum-weight one-to-one assignment, bounded by schema's <=8 observations.
    weights = [[100 * (e['action'] == a['action']) + 10 * (e.get('resource') == a.get('resource')) +
                sum(e.get(f) == a.get(f) for f in ('quantity','price','target_message')) for a in actual] for e in expected]
    best, best_pairs = -1, []
    n = min(len(expected),len(actual))
    if n:
        for eis in itertools.combinations(range(len(expected)), n):
            for ais in itertools.permutations(range(len(actual)), n):
                score = sum(weights[e][a] for e,a in zip(eis,ais))
                if score > best:
                    best, best_pairs = score, list(zip(eis,ais))
    pairs = best_pairs
    counts = Counter()
    fields = Counter()
    missing = set(range(len(expected)))
    errors = []
    for ei, ai in pairs:
        e, a = expected[ei], actual[ai]
        missing.remove(ei); remaining.remove(ai)
        counts['decisions_total'] += 1
        if e['action'] == a['action']:
            counts['decisions_correct'] += 1
            if e['action'] in SIGNALS:
                counts['true_positive_signals'] += 1
        else:
            if e['action'] in SIGNALS: counts['false_negative_signals'] += 1
            if a['action'] in SIGNALS: counts['false_positive_signals'] += 1
            errors.append({'expected_action':e['action'],'actual_action':a['action']})
        # Field totals include nulls, incorrect actions and abstentions. No denominator cherry-picking.
        for f in FIELDS:
            ee, aa = leaves(e.get(f),f), leaves(a.get(f),f)
            for path in ee:
                counts['fields_total'] += 1
                fields[path + ':total'] += 1
                if path in aa and aa[path] == ee[path]:
                    counts['fields_correct'] += 1
                    fields[path + ':correct'] += 1
                else:
                    errors.append({'field':path,'expected':ee[path],'actual':aa.get(path)})
    for ei in missing:
        e = expected[ei]
        counts['decisions_total'] += 1
        if e['action'] in SIGNALS: counts['false_negative_signals'] += 1
        for f in FIELDS:
            for path in leaves(e.get(f), f):
                counts['fields_total'] += 1
                fields[path + ':total'] += 1
        errors.append({'missing_action':e['action']})
    for ai in remaining:
        a = actual[ai]
        counts['extra_outputs'] += 1
        if a['action'] in SIGNALS: counts['false_positive_signals'] += 1
        errors.append({'extra_action':a['action']})
    # All-ignore / empty cases have one negative decision; extra outputs invalidate it.
    if not expected:
        counts['decisions_total'] += 1
        counts['decisions_correct'] += int(not actual)
    return counts, fields, errors


def evaluate_pipelines(pipelines, gold, expected_snapshots, mode):
    golden = {(r['message']['channel_id'],r['message']['message_id']):r for r in gold}
    expected = {(r['scenario_id'],r['step']):r for r in expected_snapshots}
    totals, field_counts, cases, failures, summaries = Counter(), Counter(), [], [], []
    all_scenarios_pass = 0
    for p in pipelines:
        summaries.append(p.summary())
        scenario_pass = True
        for snap in p.store.snapshots:
            wanted = expected[p.scenario_id,snap['step']]
            state_ok = normalize_state(snap['events']) == normalize_state(wanted['events'])
            metrics_ok = all(snap['metrics'].get(k) == v for k,v in wanted['metrics'].items())
            totals['snapshots_total'] += 1
            totals['states_correct'] += int(state_ok)
            totals['aggregates_correct'] += int(metrics_ok)
            scenario_pass &= state_ok and metrics_ok
            if not (state_ok and metrics_ok):
                failures.append({'scenario':p.scenario_id,'step':snap['step'], 'kind':'state_or_aggregate',
                                 'state_matches':state_ok,'metrics_match':metrics_ok,
                                 'expected_metrics':wanted['metrics'],'actual_metrics':snap['metrics']})
        all_scenarios_pass += int(scenario_pass)
        if mode == 'api':
            for record in p.records:
                if record['status'] == 'duplicate': continue
                m = record['message']
                wanted = golden[m['channel_id'],m['message_id']]['observations']
                actual = record['observations']
                counts, fields, errors = score_observations(wanted, actual)
                extraction_status = (record.get('extraction') or {}).get('status')
                guard_rejected = (extraction_status == 'ok' and record['status'] == 'unresolved'
                                  and not record['effects'])
                if guard_rejected:
                    counts['guard_rejections'] += 1
                if record['status'] == 'failed' or extraction_status == 'unresolved' or guard_rejected:
                    # Neither failed calls nor rejected candidates earn a correct negative decision.
                    counts['decisions_correct'] = 0
                    counts['processing_failures'] += 1
                    errors.append({'processing_failure': record['reason']})
                totals.update(counts); field_counts.update(fields)
                cases.append({'message': {'channel_id':m['channel_id'],'message_id':m['message_id']},
                              'status':record['status'],'reason':record['reason'],'errors':errors})
                if errors or record['status'] == 'failed':
                    failures.append({'scenario':p.scenario_id,'message_id':m['message_id'],
                                     'kind':'extraction','reason':record['reason'],'errors':errors})
    tp, fp, fn = (totals[k] for k in ('true_positive_signals','false_positive_signals','false_negative_signals'))
    extraction = None if mode != 'api' else {
        **{k:totals[k] for k in ('decisions_total','decisions_correct','fields_total','fields_correct',
                               'true_positive_signals','false_positive_signals','false_negative_signals','extra_outputs','processing_failures','guard_rejections')},
        'signal_precision':tp/(tp+fp) if tp+fp else None,
        'signal_recall':tp/(tp+fn) if tp+fn else None,
        'per_field_counts':dict(field_counts)}
    return {'mode':mode,'prompt_id':PROMPT_ID if mode=='api' else None,
            'extraction_metrics':extraction,
            'state_metrics':{k:totals[k] for k in ('snapshots_total','states_correct','aggregates_correct')},
            'scenarios_passed':all_scenarios_pass,'scenarios_total':len(pipelines),
            'runs':summaries,'failures':failures,'extraction_cases':cases,
            'limitations':'20 mock messages; fixture mode validates downstream only; API metrics measure normalized outputs and report guard failures separately.'}


def main():
    parser = argparse.ArgumentParser(description='Run per-scenario evaluation with explicit API/fixture mode.')
    parser.add_argument('--mode',choices=['api','fixture'],default='api')
    parser.add_argument('--split',choices=['dev','holdout','all'],default='dev')
    parser.add_argument('--out',type=Path,default=ROOT/'outputs/evaluation')
    args=parser.parse_args()
    load_env(ROOT/'.env')
    try:
        config=json.loads((ROOT/'dataset/config.json').read_text())
        manifest=json.loads((ROOT/'dataset/manifest.json').read_text())
        extractor=APIExtractor() if args.mode=='api' else FixtureExtractor(ROOT/'dataset/gold.jsonl')
        selected=[s for s in manifest['scenarios'] if args.split=='all' or s['split']==args.split]
        pipelines=[]
        for s in selected:
            p=Pipeline(extractor,config,s['scenario_id']).run(replay(ROOT/'dataset/messages.jsonl',s['channel_id']))
            save_run(p,args.out/s['scenario_id']); pipelines.append(p)
        result=evaluate_pipelines(pipelines,read_jsonl(ROOT/'dataset/gold.jsonl'),
                                  read_jsonl(ROOT/'dataset/expected_snapshots.jsonl'),args.mode)
        result['split']=args.split
        result['dataset_sha256']=hashlib.sha256((ROOT/'dataset/messages.jsonl').read_bytes()).hexdigest()
        args.out.mkdir(parents=True,exist_ok=True)
        (args.out/'evaluation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        lines=['# Evaluation',f'Mode: {args.mode}; split: {args.split}',
               f'Scenarios: {result["scenarios_passed"]}/{result["scenarios_total"]}',
               f'States: {result["state_metrics"]["states_correct"]}/{result["state_metrics"]["snapshots_total"]}',
               f'Aggregates: {result["state_metrics"]["aggregates_correct"]}/{result["state_metrics"]["snapshots_total"]}',
               'LLM metrics: not measured (fixture replay)' if args.mode=='fixture' else 'LLM metrics: see evaluation.json',
               f'Failures: {len(result["failures"])}',result['limitations']]
        (args.out/'evaluation.md').write_text('\n\n'.join(lines)+'\n')
        print('\n'.join(lines))
    except (ValueError,OSError,KeyError) as exc:
        parser.exit(2,f'Error: {exc}\n')


if __name__=='__main__':
    main()
