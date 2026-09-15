"""CLI and platform-independent orchestration. No gold access in API mode."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path

from events import EventStore
from extract import APIExtractor, FixtureExtractor, Result, load_env
from ingest import MessageIndex, replay
from schema import Message, write_jsonl, read_jsonl
from reliability import is_transient

ROOT = Path(__file__).resolve().parent
LOGGER = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, extractor, config: dict, scenario_id="custom"):
        self.extractor, self.config, self.scenario_id = extractor, config, scenario_id
        self.index = MessageIndex()
        self.store = EventStore(config)
        self.records = []
        self.last_time = None
        self.metadata = {}

    def process(self, message: Message) -> dict:
        disposition = self.index.accept(message)
        if disposition in ("duplicate", "unsupported_edit"):
            record = {
                "message": message.model_dump(mode="json"),
                "status": disposition,
                "reason": (
                    None if disposition == "duplicate" else "same_key_different_content"
                ),
                "effects": [],
                "extraction": None,
                "observations": [],
            }
            self.records.append(record)
            return record
        context = []
        if message.channel_id not in self.config["channels"]:
            result = Result(status="failed", reason="channel_not_configured")
        elif self.last_time is not None and message.timestamp < self.last_time:
            result = Result(status="unresolved", reason="replay_required")
        else:
            self.last_time = message.timestamp
            options = self.config.get("context", {})
            context = self.index.context(
                message,
                min(3, options.get("max_reply_ancestors", 3)),
                options.get("same_sender_minutes", 0),
            )
            result = self.extractor.extract(message, context, self.config)
        status, reason, observations, effects = result.status, result.reason, [], []
        if result.status == "ok":
            observations, effects, rejection = self.store.apply(
                message, result.observations, context
            )
            if rejection:
                status, reason = "unresolved", rejection
            elif any(o.action == "unresolved" for o in observations):
                status = "unresolved"
                reason = ";".join(
                    o.unresolved_reason for o in observations if o.unresolved_reason
                )
        record = {
            "message": message.model_dump(mode="json"),
            "status": status,
            "reason": reason,
            "observations": [o.model_dump(mode="json") for o in observations],
            "effects": effects,
            "extraction": result.record(),
        }
        self.records.append(record)
        self.index.finish(message, status, reason)
        LOGGER.info(
            "message channel=%s id=%s status=%s reason=%s",
            message.channel_id,
            message.message_id,
            status,
            reason,
        )
        self.store.snapshot(message, self.scenario_id, status, materialize=False)
        return record

    def run(self, messages):
        for message in messages:
            self.process(message)
        return self

    def summary(self) -> dict:
        from collections import Counter

        results = [r["extraction"] for r in self.records if r["extraction"]]
        calls = [r for r in results if r["called"]]
        usage_known = [r for r in calls if r["usage"] is not None]
        costs = [r["estimated_cost"] for r in calls if r["estimated_cost"] is not None]
        return {
            "scenario_id": self.scenario_id,
            "mode": self.extractor.mode,
            "status_counts": dict(Counter(r["status"] for r in self.records)),
            "unique_messages": len(self.index.seen),
            "processing_steps": len(self.store.snapshots),
            "reused_extractions": sum(r.get("reused", False) for r in results),
            "api_calls": sum(r.get("attempt_count") or 1 for r in calls),
            "retry_count": sum(r.get("retry_count", 0) for r in calls),
            "calls_with_usage": sum(
                (
                    sum(a.get("usage") is not None for a in r["attempt_history"])
                    if r.get("attempt_history")
                    else int(r["usage"] is not None)
                )
                for r in calls
            ),
            "input_tokens": (
                sum(r["usage"].get("prompt_tokens", 0) for r in usage_known)
                if usage_known
                else None
            ),
            "output_tokens": (
                sum(r["usage"].get("completion_tokens", 0) for r in usage_known)
                if usage_known
                else None
            ),
            "elapsed_ms": round(sum(r["elapsed_ms"] for r in results), 2),
            "estimated_cost": (
                sum(costs) if calls and len(costs) == len(calls) else None
            ),
            "final_metrics": self.store.aggregate(),
        }


def save_run(pipeline, directory: Path):
    from report import render_report

    directory.mkdir(parents=True, exist_ok=True)
    write_jsonl(directory / "observations.jsonl", pipeline.records)
    write_jsonl(directory / "snapshots.jsonl", pipeline.store.snapshots.entries)
    summary = pipeline.summary()
    config_digest = hashlib.sha256(
        json.dumps(pipeline.config, sort_keys=True).encode()
    ).hexdigest()
    summary["config_sha256"] = config_digest
    summary["snapshot_format"] = "delta-v1"
    summary["run_metadata"] = pipeline.metadata
    (directory / "config.json").write_text(
        json.dumps(pipeline.config, ensure_ascii=False, indent=2) + "\n"
    )
    (directory / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    )
    render_report(pipeline, directory / "report.html")
    return summary


class RecoveryExtractor:
    mode = "api"

    def __init__(self, saved: dict, client=None):
        self.saved = saved
        self.client = client

    def extract(self, message, context, config):
        old = self.saved[message.key()]
        if is_transient(old.get("reason")) or old.get("reason") in (
            "replay_required",
            "out_of_order_not_supported",
        ):
            if self.client is None:
                self.client = APIExtractor()
            return self.client.extract(message, context, config)
        return Result.from_record(old, reused=True)


def resume_run(
    directory: Path, client=None, config_path: Path | None = None
) -> Pipeline:
    """Only failed transient extractions call the API; every downstream state is rebuilt."""
    previous = json.loads((directory / "summary.json").read_text())
    config = json.loads((config_path or directory / "config.json").read_text())
    digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    if digest != previous["config_sha256"]:
        raise ValueError(
            "Resume requires the original config; changed config needs a fresh replay"
        )
    messages, saved = {}, {}
    for row in read_jsonl(directory / "observations.jsonl"):
        if row["status"] in ("duplicate", "unsupported_edit"):
            continue
        message = Message.model_validate(row["message"])
        messages[message.key()] = message
        saved[message.key()] = row["extraction"]
    extractor = RecoveryExtractor(saved, client)
    extractor.mode = previous["mode"]
    pipeline = Pipeline(extractor, config, previous["scenario_id"])
    pipeline.metadata = {
        "recovery": True,
        "original_summary": previous,
        "source_observations_sha256": hashlib.sha256(
            (directory / "observations.jsonl").read_bytes()
        ).hexdigest(),
    }
    return pipeline.run(
        sorted(
            messages.values(), key=lambda m: (m.timestamp, m.channel_id, m.message_id)
        )
    )


def main():
    parser = argparse.ArgumentParser(
        description="Replay mock chat into market signals and an offline HTML report."
    )
    parser.add_argument("--mode", choices=["api", "fixture"], default="api")
    parser.add_argument("--input", type=Path, default=ROOT / "dataset/messages.jsonl")
    parser.add_argument("--config", type=Path)
    parser.add_argument(
        "--resume",
        type=Path,
        help="Recover transient failures and replay saved results in time order",
    )
    parser.add_argument(
        "--log-level", choices=["WARNING", "INFO", "DEBUG"], default="WARNING"
    )
    parser.add_argument("--manifest", type=Path, default=ROOT / "dataset/manifest.json")
    parser.add_argument("--fixture", type=Path, default=ROOT / "dataset/gold.jsonl")
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--scenario", default="S1", help="S1..S5 or all")
    selector.add_argument(
        "--channel", help="Custom channel in config; bypass manifest scenario selection"
    )
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    load_env(ROOT / ".env")
    try:
        if args.resume:
            output = args.out or args.resume.with_name(args.resume.name + "-resumed")
            if output.resolve() == args.resume.resolve():
                raise ValueError("Resume output must differ from its source directory")
            pipeline = resume_run(args.resume, config_path=args.config)
            print(json.dumps(save_run(pipeline, output), ensure_ascii=False))
            print("Report:", output / "report.html")
            return
        config = json.loads((args.config or ROOT / "dataset/config.json").read_text())
        extractor = (
            APIExtractor() if args.mode == "api" else FixtureExtractor(args.fixture)
        )
        if args.channel:
            scenarios = [{"scenario_id": args.channel, "channel_id": args.channel}]
        else:
            scenarios = json.loads(args.manifest.read_text())["scenarios"]
            scenarios = [
                s
                for s in scenarios
                if args.scenario == "all" or s["scenario_id"] == args.scenario
            ]
        if not scenarios:
            raise ValueError("No matching scenario")
        for scenario in scenarios:
            messages = replay(args.input, scenario["channel_id"])
            if not messages:
                raise ValueError("No messages for selected channel")
            pipeline = Pipeline(extractor, config, scenario["scenario_id"]).run(
                messages
            )
            out = (args.out or ROOT / "outputs/demo") / scenario["scenario_id"]
            summary = save_run(pipeline, out)
            print(json.dumps(summary, ensure_ascii=False))
            print("Report:", out / "report.html")
    except (ValueError, OSError, KeyError) as exc:
        parser.exit(2, f"Error: {exc}\n")


if __name__ == "__main__":
    main()
