"""CLI and platform-independent orchestration. No gold access in API mode."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from events import EventStore
from extract import APIExtractor, FixtureExtractor, Result, load_env
from ingest import MessageIndex, replay
from schema import Message, write_jsonl

ROOT = Path(__file__).resolve().parent


class Pipeline:
    def __init__(self, extractor, config: dict, scenario_id='custom'):
        self.extractor, self.config, self.scenario_id = extractor, config, scenario_id
        self.index = MessageIndex()
        self.store = EventStore(config)
        self.records = []
        self.last_time = None

    def process(self, message: Message):
        existing = self.index.seen.get(message.key())
        if existing:
            state = 'duplicate' if existing == message else 'unsupported_edit'
            record = {'message': message.model_dump(mode='json'), 'status': state,
                      'reason': None if state == 'duplicate' else 'same_key_different_content',
                      'effects': [], 'extraction': None, 'observations': []}
            self.records.append(record)
            return record  # No extra snapshot or evidence for a redelivery.
        if message.channel_id not in self.config['channels']:
            raise ValueError('Channel configuration missing')
        self.index.accept(message)
        if self.last_time is not None and message.timestamp < self.last_time:
            result = Result(status='unresolved', reason='out_of_order_not_supported')
            context = []
        else:
            self.last_time = message.timestamp
            context = self.index.context(message, min(3, self.config.get('context', {}).get('max_reply_ancestors', 3)))
            result = self.extractor.extract(message, context, self.config)
        status, reason, observations, effects = result.status, result.reason, [], []
        if result.status == 'ok':
            observations, effects, rejection = self.store.apply(message, result.observations, context)
            if rejection:
                status, reason = 'unresolved', rejection
            elif any(o.action == 'unresolved' for o in observations):
                status = 'unresolved'
                reason = ';'.join(o.unresolved_reason for o in observations if o.unresolved_reason)
        record = {'message': message.model_dump(mode='json'), 'status': status, 'reason': reason,
                  'observations': [o.model_dump(mode='json') for o in observations],
                  'effects': effects, 'extraction': result.record()}
        self.records.append(record)
        self.store.snapshot(message, self.scenario_id, status)
        return record

    def run(self, messages):
        for message in messages:
            self.process(message)
        return self

    def summary(self):
        from collections import Counter
        results = [r['extraction'] for r in self.records if r['extraction']]
        calls = [r for r in results if r['called']]
        usage_known = [r for r in calls if r['usage'] is not None]
        costs = [r['estimated_cost'] for r in calls if r['estimated_cost'] is not None]
        return {'scenario_id': self.scenario_id, 'mode': self.extractor.mode,
                'status_counts': dict(Counter(r['status'] for r in self.records)),
                'unique_messages': len(self.store.snapshots), 'api_calls': len(calls),
                'calls_with_usage': len(usage_known),
                'input_tokens': sum(r['usage'].get('prompt_tokens', 0) for r in usage_known) if usage_known else None,
                'output_tokens': sum(r['usage'].get('completion_tokens', 0) for r in usage_known) if usage_known else None,
                'elapsed_ms': round(sum(r['elapsed_ms'] for r in results), 2),
                'estimated_cost': sum(costs) if calls and len(costs) == len(calls) else None,
                'final_metrics': self.store.aggregate()}


def save_run(pipeline, directory: Path):
    from report import render_report
    directory.mkdir(parents=True, exist_ok=True)
    write_jsonl(directory / 'observations.jsonl', pipeline.records)
    write_jsonl(directory / 'snapshots.jsonl', pipeline.store.snapshots)
    summary = pipeline.summary()
    config_digest = hashlib.sha256(json.dumps(pipeline.config, sort_keys=True).encode()).hexdigest()
    summary['config_sha256'] = config_digest
    (directory / 'config.json').write_text(json.dumps(pipeline.config, ensure_ascii=False, indent=2) + '\n')
    (directory / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    render_report(pipeline, directory / 'report.html')
    return summary


def main():
    parser = argparse.ArgumentParser(description='Replay mock chat into market signals and an offline HTML report.')
    parser.add_argument('--mode', choices=['api', 'fixture'], default='api')
    parser.add_argument('--input', type=Path, default=ROOT / 'dataset/messages.jsonl')
    parser.add_argument('--config', type=Path, default=ROOT / 'dataset/config.json')
    parser.add_argument('--manifest', type=Path, default=ROOT / 'dataset/manifest.json')
    parser.add_argument('--fixture', type=Path, default=ROOT / 'dataset/gold.jsonl')
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument('--scenario', default='S1', help='S1..S5 or all')
    selector.add_argument('--channel', help='Custom channel in config; bypass manifest scenario selection')
    parser.add_argument('--out', type=Path, default=ROOT / 'outputs/demo')
    args = parser.parse_args()
    load_env(ROOT / '.env')
    try:
        config = json.loads(args.config.read_text())
        extractor = APIExtractor() if args.mode == 'api' else FixtureExtractor(args.fixture)
        if args.channel:
            scenarios = [{'scenario_id': args.channel, 'channel_id': args.channel}]
        else:
            scenarios = json.loads(args.manifest.read_text())['scenarios']
            scenarios = [s for s in scenarios if args.scenario == 'all' or s['scenario_id'] == args.scenario]
        if not scenarios:
            raise ValueError('No matching scenario')
        for scenario in scenarios:
            messages = replay(args.input, scenario['channel_id'])
            if not messages:
                raise ValueError('No messages for selected channel')
            pipeline = Pipeline(extractor, config, scenario['scenario_id']).run(messages)
            out = args.out / scenario['scenario_id']
            summary = save_run(pipeline, out)
            print(json.dumps(summary, ensure_ascii=False))
            print('Report:', out / 'report.html')
    except (ValueError, OSError, KeyError) as exc:
        parser.exit(2, f'Error: {exc}\n')


if __name__ == '__main__':
    main()
