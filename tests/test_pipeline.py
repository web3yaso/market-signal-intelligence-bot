import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError
from evaluate import evaluate_pipelines, score_observations
from events import EventStore
from extract import APIExtractor, FixtureExtractor, Result, request_messages, load_env
from ingest import MessageIndex, replay
from main import Pipeline, ROOT
from report import render_report, asks_at, window_snapshots
from schema import Extraction, Message, Observation, Quantity, read_jsonl

DATA = ROOT / "dataset"
CONFIG = json.loads((DATA / "config.json").read_text())
GOLD = read_jsonl(DATA / "gold.jsonl")
GOLD_MAP = {(g["message"]["channel_id"], g["message"]["message_id"]): g for g in GOLD}


def fixture():
    return FixtureExtractor(DATA / "gold.jsonl")


def messages(channel="tg_demo"):
    return replay(DATA / "messages.jsonl", channel)


def obs(message):
    return Extraction.model_validate(
        {"observations": GOLD_MAP[message.key()]["observations"]}
    ).observations


class StubExtractor:
    mode = "api"

    def __init__(self, result):
        self.result, self.calls = result, 0

    def extract(self, *args):
        self.calls += 1
        return self.result


class PipelineTests(unittest.TestCase):
    def test_all_golden_transitions(self):
        manifest = json.loads((DATA / "manifest.json").read_text())
        runs = [
            Pipeline(fixture(), CONFIG, s["scenario_id"]).run(messages(s["channel_id"]))
            for s in manifest["scenarios"]
        ]
        result = evaluate_pipelines(
            runs, GOLD, read_jsonl(DATA / "expected_snapshots.jsonl"), "fixture"
        )
        self.assertEqual(result["scenarios_passed"], 5)
        self.assertEqual(result["state_metrics"]["aggregates_correct"], 20)
        self.assertIsNone(result["extraction_metrics"])

    def test_duplicate_delivery_and_evidence_idempotent(self):
        base = Pipeline(fixture(), CONFIG, "S1").run(messages())
        delivery = [messages()[i] for i in [0, 0, 1, 2, 3, 4, 4, 5, 6, 7]]
        replayed = Pipeline(fixture(), CONFIG, "S1").run(delivery)
        self.assertEqual(base.store.events, replayed.store.events)
        self.assertEqual(base.store.snapshots, replayed.store.snapshots)
        self.assertEqual(replayed.summary()["status_counts"]["duplicate"], 2)

    def test_permanent_failure_not_retried(self):
        extractor = StubExtractor(Result(status="failed", reason="http_400"))
        p = Pipeline(extractor, CONFIG).run([messages()[0], messages()[0]])
        self.assertEqual(extractor.calls, 1)
        self.assertEqual(len(p.store.events), 0)
        self.assertEqual(len(p.store.snapshots), 1)

    def test_same_key_edit_rejected(self):
        p = Pipeline(fixture(), CONFIG).run(messages()[:1])
        edited = messages()[0].model_copy(update={"text": "改成 2 元一刀"})
        self.assertEqual(p.process(edited)["status"], "unsupported_edit")
        self.assertEqual(p.store.aggregate()["seller_ask_median_cny_per_usd_face"], 3.8)

    def test_snapshot_isolation(self):
        p = Pipeline(fixture(), CONFIG).run(messages()[:6])
        saved = copy.deepcopy(p.store.snapshots[-1])
        p.run(messages()[6:])
        self.assertEqual(saved, p.store.snapshots[5])
        self.assertEqual(saved["metrics"]["observed_supply_usd_face"], 160000)
        returned = p.store.snapshot(messages()[-1], "S1", "ok")
        returned["events"][0]["status"] = "corrupted"
        self.assertNotEqual(p.store.snapshots[-1]["events"][0]["status"], "corrupted")

    def test_third_party_cannot_force_retraction(self):
        source = messages()[0]
        attack = messages()[7].model_copy(update={"sender_id": "attacker"})
        store = EventStore(CONFIG)
        store.apply(source, obs(source), [])
        _, _, reason = store.apply(attack, obs(messages()[7]), [source])
        self.assertEqual(reason, "owner_mismatch")
        self.assertEqual(store.aggregate()["observed_supply_usd_face"], 100000)

    def test_atomic_rollback_on_second_invalid_candidate(self):
        m = messages()[0]
        good = obs(m)[0]
        bad = good.model_copy(deep=True)
        bad.action = "retract"
        bad.target_message = m.ref()
        # A second candidate with another resource cannot select the first event.
        bad.resource.terms_id = "other"
        store = EventStore(CONFIG)
        _, effects, reason = store.apply(m, [good, bad], [])
        self.assertIsNotNone(reason)
        self.assertEqual(effects, [])
        self.assertFalse(store.events)

    def test_fabricated_evidence_rejected(self):
        m = messages()[0]
        o = obs(m)[0]
        o.evidence_quote = "unseen facts"
        store = EventStore(CONFIG)
        self.assertEqual(store.apply(m, [o], [])[2], "evidence_not_verbatim")
        self.assertFalse(store.events)

    def test_fabricated_number_rejected(self):
        m = messages()[0]
        o = obs(m)[0]
        o.quantity.value = 999999
        store = EventStore(CONFIG)
        self.assertEqual(store.apply(m, [o], [])[2], "number_not_in_current_message")

    def test_conflicting_mutations_rollback(self):
        root = messages()[0]
        update = messages()[5].model_copy(update={"text": "改成 3.6，或者 3.9。"})
        first = obs(messages()[5])[0]
        second = first.model_copy(deep=True)
        first.evidence_quote = update.text
        second.evidence_quote = update.text
        second.price.value = 3.9
        store = EventStore(CONFIG)
        store.apply(root, obs(root), [])
        _, _, reason = store.apply(update, [first, second], [root])
        self.assertEqual(reason, "multiple_mutations_of_same_field")
        self.assertEqual(store.aggregate()["seller_ask_median_cny_per_usd_face"], 3.8)

    def test_soft_seller_reprice_is_not_confirmed(self):
        root = messages()[0]
        update = messages()[5]
        candidate = obs(update)[0]
        candidate.price.is_soft = True
        store = EventStore(CONFIG)
        store.apply(root, obs(root), [])
        self.assertEqual(
            store.apply(update, [candidate], [root])[2],
            "seller_reprice_not_confirmed_point",
        )
        self.assertEqual(store.aggregate()["seller_ask_median_cny_per_usd_face"], 3.8)

    def test_missing_currency_unit_not_hallucinated(self):
        m = messages("wx_units")[0]
        o = obs(m)[0]
        o.price.currency = "CNY"
        o.price.unit = "CNY_PER_USD_FACE"
        o.quantity.unit = "USD_FACE"
        store = EventStore(CONFIG)
        normalized, _, reason = store.apply(m, [o], [])
        self.assertIsNone(reason)
        self.assertIsNone(normalized[0].quantity.unit)
        self.assertIsNone(normalized[0].price.currency)
        self.assertEqual(store.aggregate()["observed_supply_usd_face"], 0)

    def test_forward_does_not_trust_model_seller(self):
        store = EventStore(CONFIG)
        root, forward = messages()[:2]
        store.apply(root, obs(root), [])
        candidate = obs(forward)[0]
        candidate.action = "supply"
        candidate.assertion_mode = "direct"
        store.apply(forward, [candidate], [root])
        self.assertEqual(len(store.events), 1)
        self.assertEqual(next(iter(store.events.values())).owner, "alice")

    def test_forward_non_event_is_ignored(self):
        m = messages()[1]
        store = EventStore(CONFIG)
        _, _, reason = store.apply(m, obs(m), [messages()[0]])
        self.assertIsNone(reason)
        self.assertFalse(store.events)

    def test_units_and_flow_excluded(self):
        p = Pipeline(fixture(), CONFIG).run(messages("wx_units"))
        metric = p.store.aggregate()
        self.assertEqual(metric["active_event_count"], 2)
        self.assertEqual(metric["observed_supply_usd_face"], 0)
        self.assertEqual(metric["observed_demand_usd_face"], 0)
        self.assertEqual(metric["excluded"]["flow_quantity"], 1)
        self.assertIsNone(metric["seller_ask_median_cny_per_usd_face"])

    def test_unknown_terms_not_aggregated(self):
        m = messages()[0]
        o = obs(m)[0]
        o.resource.terms_id = "some_other_terms"
        store = EventStore(CONFIG)
        store.apply(m, [o], [])
        self.assertEqual(store.aggregate()["noncomparable_terms_count"], 1)
        self.assertIsNone(store.aggregate()["seller_ask_median_cny_per_usd_face"])

    def test_repeated_offer_by_same_owner_without_link_is_distinct(self):
        p = Pipeline(fixture(), CONFIG).run(messages()[:1])
        m = messages()[2].model_copy(update={"sender_id": "alice"})
        p.process(m)
        self.assertEqual(len(p.store.events), 2)

    def test_out_of_order_rejected_without_model_call(self):
        extractor = StubExtractor(Result(status="ok"))
        p = Pipeline(extractor, CONFIG).run([messages()[2], messages()[0]])
        self.assertEqual(extractor.calls, 1)
        self.assertEqual(p.records[-1]["reason"], "replay_required")

    def test_context_two_hops_and_no_nearest_neighbor(self):
        ms = messages("wx_reply")
        index = MessageIndex()
        for m in ms:
            index.accept(m)
        self.assertEqual([m.message_id for m in index.context(ms[-1])], ["m02", "m01"])
        isolated = messages("wx_mixed")[1]
        index.accept(isolated)
        self.assertEqual(index.context(isolated), [])

    def test_context_never_uses_future_or_loops(self):
        root, following = messages()[:2]
        index = MessageIndex()
        index.accept(following)
        m = root.model_copy(update={"reply_to": following.ref()})
        index.accept(m)
        self.assertEqual(index.context(m), [])

    def test_contract_rejects_naive_time_and_bad_numbers(self):
        data = messages()[0].model_dump(mode="json")
        data["timestamp"] = "2026-09-15T09:00:00"
        with self.assertRaises(ValidationError):
            Message.model_validate(data)
        for value in [float("nan"), float("inf"), -1, True]:
            with self.assertRaises(ValidationError):
                Quantity(value=value)

    def test_report_escapes_untrusted_html(self):
        m = messages()[0].model_copy(
            update={"text": "</script><script>alert(1)</script>"}
        )
        p = Pipeline(StubExtractor(Result(status="ok")), CONFIG).run([m])
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "report.html"
            render_report(p, path)
            html = path.read_text()
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn("<script src=", html)
        self.assertEqual(html.count('<script type="text/javascript">'), 1)

    def test_all_null_price_does_not_block_remaining_update(self):
        original, update = messages()[0], messages()[4]
        candidate = obs(update)[0].model_dump()
        candidate["price"] = dict(
            value=None, currency=None, unit=None, operator=None, is_soft=None
        )
        normalized = Observation.model_validate(candidate)
        self.assertIsNone(normalized.price)
        store = EventStore(CONFIG)
        store.apply(original, obs(original), [])
        _, _, rejection = store.apply(update, [normalized], [original])
        self.assertIsNone(rejection)
        event = next(iter(store.events.values()))
        self.assertEqual(event.quantity.value, 60000)
        self.assertEqual(event.price.value, 3.8)

    def test_partial_or_unknown_price_fields_are_not_discarded(self):
        for price in (
            {"value": None, "currency": "CNY"},
            {"value": None, "unexpected": None},
            {},
        ):
            candidate = obs(messages()[4])[0].model_dump()
            candidate["price"] = price
            with self.assertRaises(ValidationError):
                Observation.model_validate(candidate)

    def test_report_price_range_excludes_bids_and_retracted_sellers(self):
        p = Pipeline(fixture(), CONFIG, "S1").run(messages())
        terms = CONFIG["scope"]["aggregation_terms_id"]
        self.assertEqual(asks_at(p.store.snapshots[5], terms), [3.6, 3.8])
        self.assertEqual(asks_at(p.store.snapshots[-1], terms), [3.8])

    def test_report_windows_are_anchored_to_sample_end(self):
        snapshots = [
            {"as_of": "2020-01-01T00:00:00+00:00"},
            {"as_of": "2020-01-05T00:00:00+00:00"},
            {"as_of": "2020-01-07T00:00:00+00:00"},
        ]
        self.assertEqual(window_snapshots(snapshots, 24), snapshots[-1:])
        self.assertEqual(window_snapshots(snapshots, 168), snapshots)


class APITests(unittest.TestCase):
    def client(self, transport, count=100):
        return APIExtractor(
            model="test-model",
            key="test-key",
            base_url="https://api.example.invalid/v1",
            transport=transport,
            counter=lambda *_: (count, "test_counter"),
            sleeper=lambda _: None,
        )

    def test_api_response_is_validated_and_request_has_no_gold(self):
        m = messages()[0]
        payload = {"observations": GOLD_MAP[m.key()]["observations"]}
        requests = []

        def transport(body):
            requests.append(body)
            return {
                "choices": [
                    {
                        "message": {"content": json.dumps(payload)},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 40},
            }

        result = self.client(transport).extract(m, [], CONFIG)
        self.assertEqual(result.status, "ok")
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["max_completion_tokens"], 1000)
        input_payload = json.loads(requests[0]["messages"][1]["content"])
        self.assertEqual(set(input_payload), {"current", "context", "config"})
        self.assertNotIn("rationale", json.dumps(input_payload))
        self.assertNotIn("gold_event_id", json.dumps(input_payload))

    def test_budget_blocks_request(self):
        called = []
        result = self.client(lambda body: called.append(body), 3001).extract(
            messages()[0], [], CONFIG
        )
        self.assertEqual(result.status, "unresolved")
        self.assertFalse(result.called)
        self.assertFalse(called)

    def test_timeout_retries_are_bounded(self):
        calls = []

        def transport(body):
            calls.append(body)
            raise TimeoutError()

        result = self.client(transport).extract(messages()[0], [], CONFIG)
        self.assertEqual(result.status, "failed")
        self.assertEqual(len(calls), 3)
        self.assertEqual(result.retry_count, 2)
        self.assertEqual(result.reason, "timeout_or_network_error")

    def test_invalid_json_or_truncation_cannot_mutate(self):
        for text, finish in [("not json", "stop"), ('{"observations": []}', "length")]:
            client = self.client(
                lambda body: {
                    "choices": [{"message": {"content": text}, "finish_reason": finish}]
                }
            )
            p = Pipeline(client, CONFIG).run(messages()[:1])
            self.assertEqual(p.records[0]["status"], "failed")
            self.assertFalse(p.store.events)

    def test_cost_is_not_zero_when_unknown(self):
        with patch.dict(
            "os.environ",
            {"INPUT_PRICE_PER_MILLION": "", "OUTPUT_PRICE_PER_MILLION": ""},
        ):
            c = self.client(
                lambda body: {
                    "choices": [
                        {
                            "message": {"content": '{"observations": []}'},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 40},
                }
            )
            result = c.extract(messages()[0], [], CONFIG)
        self.assertEqual(result.status, "ok")
        self.assertIsNone(result.estimated_cost)

    def test_env_is_not_executed(self):
        import os

        with tempfile.TemporaryDirectory() as temp, patch.dict(
            os.environ, {}, clear=True
        ):
            p = Path(temp) / ".env"
            p.write_text("DEMO_VAR=$(touch NEVER)\n")
            load_env(p)
            self.assertEqual(os.environ["DEMO_VAR"], "$(touch NEVER)")

    def test_abstention_and_extra_signals_are_scored(self):
        expected = GOLD_MAP[messages()[0].key()]["observations"]
        counts, _, _ = score_observations(expected, [])
        self.assertEqual(counts["false_negative_signals"], 1)
        self.assertGreater(counts["fields_total"], 0)
        self.assertEqual(counts["fields_correct"], 0)
        counts, _, _ = score_observations([], expected)
        self.assertEqual(counts["false_positive_signals"], 1)
        self.assertEqual(counts["decisions_correct"], 0)

    def test_explanatory_hearsay_record_optional(self):
        expected = GOLD_MAP["tg_quotes", "m02"]["observations"]
        counts, _, _ = score_observations(expected, expected[1:])
        self.assertEqual(counts["decisions_correct"], 1)
        self.assertEqual(counts["extra_outputs"], 0)

    def test_failed_call_is_not_correct_negative_classification(self):
        p = Pipeline(
            StubExtractor(Result(status="failed", reason="timeout")), CONFIG, "S5"
        ).run(messages("wx_mixed"))
        result = evaluate_pipelines(
            [p], GOLD, read_jsonl(DATA / "expected_snapshots.jsonl"), "api"
        )
        self.assertEqual(result["extraction_metrics"]["decisions_correct"], 0)
        self.assertEqual(result["extraction_metrics"]["processing_failures"], 2)

    def test_guard_rejection_is_not_correct_negative_classification(self):
        message = messages("wx_mixed")[0]
        candidate = obs(messages()[0])[0]
        candidate.source_message_ids = [message.ref()]
        candidate.evidence_quote = message.text
        candidate.convention_id = None
        p = Pipeline(
            StubExtractor(Result(status="ok", observations=[candidate])), CONFIG, "S5"
        ).run([message])
        self.assertEqual(p.records[0]["reason"], "number_not_in_current_message")
        result = evaluate_pipelines(
            [p], GOLD, read_jsonl(DATA / "expected_snapshots.jsonl"), "api"
        )
        self.assertEqual(result["extraction_metrics"]["decisions_correct"], 0)
        self.assertEqual(result["extraction_metrics"]["guard_rejections"], 1)

    def test_valid_model_abstention_is_not_guard_failure(self):
        message = messages("wx_mixed")[1]
        p = Pipeline(
            StubExtractor(Result(status="ok", observations=obs(message))), CONFIG, "S5"
        ).run([message])
        # This message preserves the empty state expected at either step of S5.
        result = evaluate_pipelines(
            [p], GOLD, read_jsonl(DATA / "expected_snapshots.jsonl"), "api"
        )
        self.assertEqual(result["extraction_metrics"]["decisions_correct"], 1)
        self.assertEqual(result["extraction_metrics"]["guard_rejections"], 0)


if __name__ == "__main__":
    unittest.main()
