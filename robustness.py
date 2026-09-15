"""Small injection and repeated-call probes; expected outputs never enter model input."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from extract import APIExtractor, load_env, PROMPT_ID
from ingest import replay
from main import Pipeline

ROOT = Path(__file__).parent


def run_probes(
    extractor, config: dict, messages: list, expectations: list, repeats: int
) -> dict:
    if not 2 <= repeats <= 5:
        raise ValueError("repeats must be between 2 and 5")
    results = []
    for message, expected in zip(messages, expectations, strict=True):
        if message.message_id != expected["message_id"]:
            raise ValueError("Expected data must align with input IDs")
        attempts = []
        signatures = set()
        for iteration in range(repeats if expected["group"] == "variance" else 1):
            pipeline = Pipeline(extractor, config, message.message_id).run([message])
            record = pipeline.records[0]
            observations = [
                o for o in record["observations"] if o["action"] != "ignore"
            ]
            actions = [o["action"] for o in observations]
            metrics = pipeline.store.aggregate()
            matches = (
                record["status"] == "ok"
                and actions == expected["actions"]
                and all(
                    metrics[key] == value for key, value in expected["metrics"].items()
                )
            )
            signature = [
                {
                    k: o[k]
                    for k in (
                        "action",
                        "resource",
                        "quantity",
                        "price",
                        "target_message",
                        "assertion_mode",
                    )
                }
                for o in observations
            ]
            signatures.add(
                json.dumps(
                    {"status": record["status"], "observations": signature},
                    sort_keys=True,
                )
            )
            attempts.append(
                {
                    "iteration": iteration + 1,
                    "matches_expected": matches,
                    "record": record,
                    "summary": pipeline.summary(),
                }
            )
        results.append(
            {
                "message_id": message.message_id,
                "group": expected["group"],
                "expected": expected,
                "distinct_outputs": len(signatures),
                "attempts": attempts,
            }
        )
    return {
        "prompt_id": PROMPT_ID,
        "cases": results,
        "limitation": "Three injection probes and three repeated inputs are development checks, not broad security or variance guarantees.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--input", type=Path, required=True, help="Probe messages JSONL"
    )
    parser.add_argument(
        "--expected", type=Path, required=True, help="Expected probe results JSON"
    )
    parser.add_argument(
        "--config", type=Path, required=True, help="Channel configuration JSON"
    )
    parser.add_argument("--out", type=Path, default=ROOT / "outputs/robustness.json")
    args = parser.parse_args()
    load_env(ROOT / ".env")
    result = run_probes(
        APIExtractor(),
        json.loads(args.config.read_text()),
        replay(args.input),
        json.loads(args.expected.read_text()),
        args.repeats,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    for case in result["cases"]:
        print(
            case["message_id"],
            sum(a["matches_expected"] for a in case["attempts"]),
            "/",
            len(case["attempts"]),
            "distinct outputs:",
            case["distinct_outputs"],
        )


if __name__ == "__main__":
    main()
