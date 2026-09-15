import copy
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from test_pipeline import CONFIG, messages, obs, fixture, StubExtractor
from schema import Extraction, Message
from extract import APIExtractor, Result, strict_schema
from main import Pipeline, save_run, resume_run
from events import EventStore
from ingest import MessageIndex
from snapshot_log import load_snapshots


class ReviewFixTests(unittest.TestCase):
    def test_transient_redelivery_recovers_and_then_deduplicates(self):
        message = messages()[0]
        client = StubExtractor(Result(status="failed", reason="http_503"))
        pipeline = Pipeline(client, CONFIG)
        pipeline.process(message)
        client.result = Result(status="ok", observations=obs(message))
        pipeline.process(message)
        pipeline.process(message)
        self.assertEqual(client.calls, 2)
        self.assertEqual(pipeline.summary()["unique_messages"], 1)
        self.assertEqual(len(pipeline.store.snapshots), 2)
        self.assertEqual(pipeline.store.snapshots[0]["events"], [])
        self.assertEqual(pipeline.store.aggregate()["observed_supply_usd_face"], 100000)

    def test_late_retry_requires_time_ordered_recovery(self):
        client = StubExtractor(Result(status="failed", reason="http_500"))
        pipeline = Pipeline(client, CONFIG)
        pipeline.process(messages()[0])
        pipeline.process(messages()[3])
        self.assertEqual(pipeline.process(messages()[0])["reason"], "replay_required")
        self.assertEqual(client.calls, 2)

    def test_forward_chatter_is_ok(self):
        original = messages()[0].model_copy(update={"text": "hello"})
        forward = messages()[1].model_copy(update={"text": "hello"})
        candidate = obs(forward)[0]
        candidate.action = "ignore"
        candidate.evidence_quote = "hello"
        store = EventStore(CONFIG)
        accepted, effects, reason = store.apply(forward, [candidate], [original])
        self.assertIsNone(reason)
        self.assertEqual(effects[0]["reason"], "forward_source_not_an_event")
        self.assertFalse(store.events)

    def test_two_offers_and_exact_duplicate(self):
        message = messages()[0].model_copy(update={"text": "出 100k 3.8，另外 50k 3.9"})
        first = obs(messages()[0])[0]
        first.evidence_quote = message.text
        second = first.model_copy(deep=True)
        second.quantity.value = 50000
        second.price.value = 3.9
        store = EventStore(CONFIG)
        self.assertIsNone(store.apply(message, [first, second], [])[2])
        self.assertEqual(len(store.events), 2)
        self.assertEqual(store.aggregate()["observed_supply_usd_face"], 150000)
        original = copy.deepcopy(store.events)
        self.assertEqual(
            store.apply(message, [first, first], [])[2],
            "duplicate_candidate_for_same_resource",
        )
        self.assertEqual(original, store.events)
        fresh = EventStore(CONFIG)
        self.assertEqual(
            fresh.apply(message, [first, first], [])[2], "duplicate_candidate"
        )
        self.assertFalse(fresh.events)

    def test_new_demand_can_reference_non_event(self):
        original = messages()[0].model_copy(update={"text": "hello"})
        message = messages()[3].model_copy(update={"reply_to": original.ref()})
        candidate = obs(messages()[3])[0]
        candidate.target_message = original.ref()
        store = EventStore(CONFIG)
        self.assertIsNone(store.apply(message, [candidate], [original])[2])
        self.assertEqual(store.aggregate()["observed_demand_usd_face"], 500000)

    def test_unknown_channel_does_not_stop_batch_or_report(self):
        bad = messages()[0].model_copy(update={"channel_id": "missing"})
        pipeline = Pipeline(fixture(), CONFIG).run([bad, messages()[0]])
        self.assertEqual(pipeline.records[0]["reason"], "channel_not_configured")
        self.assertEqual(pipeline.store.aggregate()["observed_supply_usd_face"], 100000)
        with tempfile.TemporaryDirectory() as directory:
            save_run(pipeline, Path(directory))

    def test_resume_retries_only_transient_and_reapplies_dependents(self):
        root, update = messages()[0], messages()[4]
        client = StubExtractor(
            Result(status="failed", reason="timeout_or_network_error")
        )
        pipeline = Pipeline(client, CONFIG, "S1")
        pipeline.process(root)
        client.result = Result(status="ok", observations=obs(update))
        pipeline.process(update)
        self.assertEqual(pipeline.records[-1]["reason"], "target_not_unique_or_missing")
        retry = StubExtractor(Result(status="ok", observations=obs(root)))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            save_run(pipeline, path)
            before = (path / "observations.jsonl").read_bytes()
            recovered = resume_run(path, retry)
            self.assertEqual((path / "observations.jsonl").read_bytes(), before)
        self.assertEqual(retry.calls, 1)
        self.assertEqual(recovered.store.aggregate()["observed_supply_usd_face"], 60000)
        self.assertEqual(recovered.summary()["reused_extractions"], 1)

    def test_delta_history_stores_only_changed_events(self):
        pipeline = Pipeline(fixture(), CONFIG, "S1").run(messages())
        log = pipeline.store.snapshots
        self.assertLess(
            sum(len(e["event_changes"]) for e in log.entries),
            sum(len(s["events"]) for s in log),
        )
        exposed = log[0]
        exposed["events"][0]["quantity"]["value"] = -1
        self.assertEqual(log[0]["events"][0]["quantity"]["value"], 100000)
        with tempfile.TemporaryDirectory() as directory:
            save_run(pipeline, Path(directory))
            self.assertEqual(
                list(load_snapshots(Path(directory) / "snapshots.jsonl")), list(log)
            )

    def test_optional_context_is_same_sender_and_same_channel(self):
        index = MessageIndex()
        original = messages()[0]
        index.accept(original)
        followup = messages()[4].model_copy(update={"reply_to": None})
        self.assertEqual(index.context(followup), [])
        self.assertEqual(index.context(followup, same_sender_minutes=10), [original])
        self.assertEqual(index.context(followup, same_sender_minutes=1), [])
        self.assertEqual(
            index.context(
                followup.model_copy(update={"sender_id": "other"}),
                same_sender_minutes=10,
            ),
            [],
        )

    def test_bounded_retry_success_and_permanent_error(self):
        attempts, sleeps = [], []

        def transport(body):
            attempts.append(body)
            if len(attempts) == 1:
                raise urllib.error.HTTPError(
                    "https://example.invalid", 429, "rate limited", {}, None
                )
            return {
                "choices": [
                    {
                        "message": {"content": '{"observations":[]}'},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 2},
            }

        client = APIExtractor(
            model="test",
            key="test",
            transport=transport,
            counter=lambda *_: (1, "test"),
            sleeper=sleeps.append,
        )
        result = client.extract(messages()[0], [], CONFIG)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.attempt_count, 2)
        self.assertEqual(result.retry_count, 1)
        self.assertEqual(sleeps, [0.25])
        self.assertIsNone(result.estimated_cost)

        def permanent(_):
            raise urllib.error.HTTPError(
                "https://example.invalid", 400, "bad request", {}, None
            )

        client.transport = permanent
        self.assertEqual(client.extract(messages()[0], [], CONFIG).attempt_count, 1)

    def test_strict_schema_is_opt_in_and_accounts_for_schema_budget(self):
        schema = strict_schema()
        for node in [schema, *schema["$defs"].values()]:
            if node.get("type") == "object":
                self.assertEqual(set(node["required"]), set(node["properties"]))
                self.assertFalse(node["additionalProperties"])
        requests = []

        def transport(body):
            requests.append(body)
            return {
                "choices": [
                    {
                        "message": {"content": '{"observations":[]}'},
                        "finish_reason": "stop",
                    }
                ]
            }

        client = APIExtractor(
            model="test",
            key="test",
            transport=transport,
            counter=lambda messages, _: (len(messages), "test"),
            output_format="json_schema",
        )
        result = client.extract(messages()[0], [], CONFIG)
        self.assertEqual(requests[0]["response_format"]["type"], "json_schema")
        self.assertEqual(result.input_token_estimate, 3)

    def test_window_activity_is_not_remaining_stock(self):
        from report_metrics import window_activity

        pipeline = Pipeline(fixture(), CONFIG, "S1").run(messages())
        all_time = window_activity(
            pipeline.store.snapshots, pipeline.records, "demo_prepaid_v1"
        )
        self.assertEqual(all_time["new_supply"], 200000)
        self.assertEqual(all_time["new_demand"], 500000)
        self.assertEqual(all_time["quote_median"], 3.7)
        recent = window_activity(
            pipeline.store.snapshots[-2:], pipeline.records, "demo_prepaid_v1"
        )
        self.assertEqual(recent["new_supply"], 0)
        self.assertIsNone(recent["quote_median"])
        self.assertEqual(pipeline.store.aggregate()["observed_supply_usd_face"], 100000)

    def test_resume_reorders_previously_deferred_messages(self):
        pipeline = Pipeline(fixture(), CONFIG, "S1").run([messages()[2], messages()[0]])
        client = StubExtractor(Result(status="ok", observations=obs(messages()[0])))
        with tempfile.TemporaryDirectory() as directory:
            save_run(pipeline, Path(directory))
            recovered = resume_run(Path(directory), client)
        self.assertEqual(client.calls, 1)
        self.assertEqual(
            recovered.store.aggregate()["observed_supply_usd_face"], 200000
        )
        self.assertEqual(
            [r["message"]["message_id"] for r in recovered.records], ["m01", "m03"]
        )

    def test_context_does_not_cross_channels(self):
        index = MessageIndex()
        index.accept(messages()[0])
        message = messages()[4].model_copy(
            update={"reply_to": None, "channel_id": "another_channel"}
        )
        self.assertEqual(index.context(message, same_sender_minutes=10), [])

    def test_unlinked_followup_uses_optional_context_to_update_event(self):
        config = copy.deepcopy(CONFIG)
        config["context"]["same_sender_minutes"] = 10
        root = messages()[0]
        followup = messages()[4].model_copy(update={"reply_to": None})
        client = StubExtractor(Result(status="ok", observations=obs(root)))
        pipeline = Pipeline(client, config)
        pipeline.process(root)
        client.result = Result(status="ok", observations=obs(followup))
        self.assertEqual(pipeline.process(followup)["status"], "ok")
        self.assertEqual(pipeline.store.aggregate()["observed_supply_usd_face"], 60000)

    def test_resume_does_not_retry_permanent_extraction_failures(self):
        for reason in ("http_400", "invalid_output_or_response:ValidationError"):
            with self.subTest(
                reason=reason
            ), tempfile.TemporaryDirectory() as directory:
                client = StubExtractor(Result(status="failed", reason=reason))
                pipeline = Pipeline(client, CONFIG).run([messages()[0], messages()[0]])
                self.assertEqual(client.calls, 1)
                save_run(pipeline, Path(directory))
                retry = StubExtractor(
                    Result(status="ok", observations=obs(messages()[0]))
                )
                recovered = resume_run(Path(directory), retry)
                self.assertEqual(retry.calls, 0)
                self.assertEqual(recovered.records[0]["reason"], reason)
                self.assertFalse(recovered.store.events)

    def test_untouched_event_not_copied_by_transaction(self):
        pipeline = Pipeline(fixture(), CONFIG, "S1").run(messages()[:4])
        bob = next(e for e in pipeline.store.events.values() if e.owner == "bob")
        pipeline.process(messages()[4])
        self.assertIs(pipeline.store.events[bob.event_id], bob)
