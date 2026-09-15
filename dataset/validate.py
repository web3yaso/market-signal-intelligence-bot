"""Validate dataset integrity and hand-authored expected states, not model accuracy."""
import json
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent


def read_jsonl(name):
    return [json.loads(line) for line in (ROOT / name).read_text().splitlines() if line.strip()]


def key(value):
    return value['channel_id'], value['message_id']


def require(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    messages = read_jsonl('messages.jsonl')
    gold = read_jsonl('gold.jsonl')
    snapshots = read_jsonl('expected_snapshots.jsonl')
    manifest = json.loads((ROOT / 'manifest.json').read_text())
    config = json.loads((ROOT / 'config.json').read_text())
    ids = [key(m) for m in messages]
    require(len(ids) == len(set(ids)) == manifest['message_count'] == 20, 'Message IDs/count')
    by_id = dict(zip(ids, messages))
    position = {mid: i for i, mid in enumerate(ids)}
    fields = {'channel_id', 'message_id', 'sender_id', 'timestamp', 'text', 'reply_to', 'forwarded_origin'}
    for m in messages:
        require(set(m) == fields, f'Unexpected input fields / possible label leakage: {key(m)}')
        require(datetime.fromisoformat(m['timestamp']).tzinfo is not None, 'Timezone missing')
        require(m['channel_id'] in config['channels'], 'Missing channel convention')
        for field in ('reply_to', 'forwarded_origin'):
            if m[field]:
                target = key(m[field])
                require(target in by_id and position[target] < position[key(m)], f'Invalid {field}')

    for rows, field in [(gold, 'message'), (snapshots, 'after_message'), (manifest['messages'], 'message')]:
        keys = [key(row[field]) for row in rows]
        require(len(keys) == len(set(keys)) and set(keys) == set(ids), f'Coverage/duplicates: {field}')
    require(sum(len(g['observations']) for g in gold) == manifest['observation_count'] == 21, 'Observation count')
    actions = {'supply', 'demand', 'set_remaining', 'reprice', 'retract', 'ignore', 'unresolved'}
    for g in gold:
        current = key(g['message'])
        visible = {current}
        m = by_id[current]
        for _ in range(config['context']['max_reply_ancestors']):
            if not m['reply_to']:
                break
            visible.add(key(m['reply_to']))
            m = by_id[key(m['reply_to'])]
        forwarded = by_id[current]['forwarded_origin']
        if forwarded:
            visible.add(key(forwarded))
        for o in g['observations']:
            require(o['action'] in actions, 'Unknown action')
            require(o['evidence_quote'] in by_id[current]['text'], f'Non-verbatim evidence: {current}')
            require(all(key(src) in visible for src in o['source_message_ids']), f'Hidden evidence: {current}')
            if o['target_message']:
                require(key(o['target_message']) in visible, f'Target unavailable in context: {current}')
            if o['action'] in ('set_remaining', 'reprice', 'retract'):
                target = by_id[key(o['target_message'])]
                require(target['sender_id'] == by_id[current]['sender_id'], 'Unauthorized gold mutation')
            require(bool(o['unresolved_reason']) == (o['action'] == 'unresolved'), 'Unresolved reason')

    source_splits = {}
    for m in manifest['messages']:
        for src in m['source_forum_message_ids']:
            source_splits.setdefault(src, set()).add(m['split'])
    require(all(len(splits) == 1 for splits in source_splits.values()), 'Source crosses splits')
    require(sum(s['split'] == 'holdout' for s in manifest['scenarios']) == 2, 'Holdout count')
    require(len(manifest['scenarios']) == 5, 'Scenario count')
    for s in manifest['scenarios']:
        expected_ids = [(s['channel_id'], mid) for mid in s['message_ids']]
        require(expected_ids == [mid for mid in ids if mid[0] == s['channel_id']], 'Scenario order')
        subset = [x for x in snapshots if x['scenario_id'] == s['scenario_id']]
        require([x['step'] for x in subset] == list(range(1, len(subset) + 1)), 'Snapshot steps')
        for x in subset:
            require(key(x['after_message']) == expected_ids[x['step'] - 1], 'Snapshot message order')
            require(x['as_of'] == by_id[key(x['after_message'])]['timestamp'], 'Snapshot timestamp')

    # Recompute metrics from saved gold event states, independently of construction code.
    for snap in snapshots:
        active = [e for e in snap['events'] if e['status'] == 'active']
        require(len({e['event_id'] for e in snap['events']}) == len(snap['events']), 'Duplicate events')
        sums = {'supply': 0, 'demand': 0}
        excluded = Counter()
        asks = []
        for e in snap['events']:
            origin = key(e['origin_message'])
            require(origin[0] == snap['after_message']['channel_id'], 'Cross-scenario event')
            require(by_id[origin]['sender_id'] == e['owner'], 'Event owner')
            support = [key(m) for m in e['supporting_message_ids']]
            require(len(support) == len(set(support)), 'Repeated evidence')
            require(all(mid in by_id and position[mid] <= position[key(snap['after_message'])] for mid in support), 'Future evidence')
        for e in active:
            q, p, side = e['quantity'], e['price'], e['side']
            if q is not None and q['kind'] == 'flow':
                excluded['flow_quantity'] += 1
            elif q is not None and q['unit'] == 'USD_FACE':
                sums[side] += q['value']
            else:
                excluded[side + '_quantity_missing_or_unit_unknown'] += 1
            if side == 'supply':
                if p and (p['operator'], p['currency'], p['unit']) == ('eq', 'CNY', 'CNY_PER_USD_FACE'):
                    asks.append(Decimal(str(p['value'])))
                else:
                    excluded['seller_price_missing_or_not_comparable'] += 1
        metric = snap['metrics']
        require(metric['observed_supply_usd_face'] == sums['supply'], 'Supply sum')
        require(metric['observed_demand_usd_face'] == sums['demand'], 'Demand sum')
        require(metric['active_event_count'] == len(active), 'Active event count')
        require(metric['seller_ask_sample_count'] == len(asks), 'Price sample count')
        expected_median = float(median(asks)) if asks else None
        require(metric['seller_ask_median_cny_per_usd_face'] == expected_median, 'Ask median')
        require(all(v == excluded[k] for k, v in metric['excluded'].items()), 'Exclusion counts')

    index = {(s['scenario_id'], s['step']): s for s in snapshots}
    early = next(e for e in index['S1', 6]['events'] if e['event_id'] == 'E1')
    late = next(e for e in index['S1', 8]['events'] if e['event_id'] == 'E1')
    require(early['status'] == 'active' and late['status'] == 'retracted', 'Historical state')
    require(early['quantity']['value'] == 60000 and early['price']['value'] == 3.6, 'Historical values')
    for spec in manifest['replay_checks']:
        if 'delivery_order' in spec:
            require(len(set(spec['delivery_order'])) == spec['expected_unique_message_count'], 'Replay unique count')
        if 'expected_metrics' in spec:
            metric = index[spec['scenario_id'], spec['read_step']]['metrics']
            require(all(metric[k] == v for k, v in spec['expected_metrics'].items()), 'Replay expected metrics')

    tags = {t for m in manifest['messages'] for t in m['coverage_tags']}
    required = {'clean_supply', 'clean_demand', 'follow_up_replies', 'missing_units_unresolved',
                'aliases', 'repeated_offers', 'forwarded_information', 'contradictory_quotes',
                'historical_references', 'irrelevant_chatter', 'prior_context'}
    require(required <= tags, 'Requested adversarial coverage missing')
    print('PASS: 20 messages, 21 observations, 20 snapshots, 5 scenarios (3 dev / 2 holdout).')
    print('PASS: references, visible evidence, authority, splits, coverage, aggregates, historical gold states.')
    print('Dataset consistency only; model accuracy and runtime replay behavior have not been tested.')


if __name__ == '__main__':
    main()
