"""Deterministic candidate validation, owner-authorized event transitions, snapshots."""

from __future__ import annotations

import copy
import hashlib
import re
from decimal import Decimal
from statistics import median

from schema import Event, Message, Observation, Ref
from snapshot_log import SnapshotLog


class Rejected(ValueError):
    pass


def numbers(text):
    # Validation of model-proposed numbers, not an alternative extraction route.
    return {
        float(value) * {"": 1, "k": 1000, "w": 10000, "万": 10000}[suffix.lower()]
        for value, suffix in re.findall(
            r"(?<![\d.])(\d+(?:\.\d+)?)\s*([kKwW万]?)", text
        )
    }


def normalize(
    o: Observation, message: Message, context: list[Message], config: dict
) -> Observation:
    o = o.model_copy(deep=True)
    visible = {m.key(): m for m in [message, *context]}
    if any(src.key() not in visible for src in o.source_message_ids):
        raise Rejected("source_not_visible")
    if message.key() not in {src.key() for src in o.source_message_ids}:
        raise Rejected("current_source_missing")
    if o.evidence_quote not in message.text:
        raise Rejected("evidence_not_verbatim")
    if o.target_message and o.target_message.key() not in visible:
        raise Rejected("target_not_visible")
    aliases = {k.casefold(): v for k, v in config.get("aliases", {}).items()}
    if o.resource:
        o.resource.provider = aliases.get(
            o.resource.provider.casefold(), o.resource.provider.casefold()
        )
    channel = config["channels"][message.channel_id]
    if o.convention_id and o.convention_id != channel["convention_id"]:
        raise Rejected("wrong_convention")
    # Default units only after resource scope is established. Never infer quantity from price.
    scoped = (
        o.resource
        and o.resource.provider == config["scope"]["provider"]
        and o.resource.kind == config["scope"]["kind"]
    )
    if scoped:
        used = False
        if o.resource.terms_id is None and channel.get(
            "terms_id_for_openai_api_credits"
        ):
            o.resource.terms_id = channel["terms_id_for_openai_api_credits"]
            used = True
        if (
            o.quantity
            and o.quantity.unit is None
            and channel.get("quantity_unit_default")
        ):
            o.quantity.unit = channel["quantity_unit_default"]
            used = True
        if o.price:
            if o.price.currency is None and channel.get("price_currency_default"):
                o.price.currency = channel["price_currency_default"]
                used = True
            if o.price.unit is None and channel.get("price_unit_default"):
                o.price.unit = channel["price_unit_default"]
                used = True
        if used:
            o.convention_id = channel["convention_id"]
    # Proposals changing current events must ground their numbers in the current text.
    if o.action not in ("ignore", "unresolved"):
        values = numbers(message.text)
        for candidate in (o.quantity, o.price):
            if candidate and candidate.value not in values:
                raise Rejected("number_not_in_current_message")
        evidence_text = "\n".join(
            visible[src.key()].text for src in o.source_message_ids
        )
        # Guard the only normalized dimensions we aggregate; absent evidence => null.
        if (
            o.quantity
            and o.quantity.unit == "USD_FACE"
            and not channel.get("quantity_unit_default")
        ):
            if not re.search(r"美元|美金|USD|\$|dollar", evidence_text, re.I):
                o.quantity.unit = None
        if o.price:
            if o.price.currency == "CNY" and not channel.get("price_currency_default"):
                if not re.search(r"人民币|CNY|RMB|元", evidence_text, re.I):
                    o.price.currency = None
            if o.price.unit == "CNY_PER_USD_FACE" and not channel.get(
                "price_unit_default"
            ):
                if not re.search(
                    r"一刀|每.*(?:美元|美金)|per.*(?:USD|dollar)", evidence_text, re.I
                ):
                    o.price.unit = None
    return o


class EventStore:
    def __init__(self, config: dict):
        self.config = config
        self.events: dict[str, Event] = {}
        self.snapshots = SnapshotLog()
        self._dirty: set[str] = set()

    def target(self, ref: Ref, resource=None) -> Event:
        matches = [
            e
            for e in self.events.values()
            if ref in e.supporting_message_ids
            and (resource is None or e.resource == resource)
        ]
        if len(matches) != 1:
            raise Rejected("target_not_unique_or_missing")
        return matches[0]

    @staticmethod
    def link(event, message):
        if message.ref() not in event.supporting_message_ids:
            event.supporting_message_ids.append(message.ref())

    def apply(
        self, message: Message, candidates: list[Observation], context: list[Message]
    ):
        """Atomic per message: no partially applied output if any actionable candidate is invalid."""
        before = {}

        def edit(event):
            if event.event_id not in before:
                before[event.event_id] = self.events[event.event_id]
                self.events[event.event_id] = event.model_copy(deep=True)
            return self.events[event.event_id]

        accepted, effects = [], []
        mutations = set()
        created_counts = {}
        seen_candidates = []
        try:
            accepted = [normalize(o, message, context, self.config) for o in candidates]
            if message.forwarded_origin:
                # Explicit source metadata wins over any model claim about this forward's seller.
                try:
                    source = self.target(message.forwarded_origin)
                except Rejected:
                    return (
                        accepted,
                        [
                            {
                                "operation": "none",
                                "reason": "forward_source_not_an_event",
                            }
                        ],
                        None,
                    )
                source = edit(source)
                self.link(source, message)
                self._dirty.update(before)
                return (
                    accepted,
                    [{"operation": "link_evidence", "event_id": source.event_id}],
                    None,
                )
            for o in accepted:
                if o.model_dump() in seen_candidates:
                    raise Rejected("duplicate_candidate")
                seen_candidates.append(o.model_dump())
                if (
                    o.action in ("ignore", "unresolved")
                    or o.assertion_mode == "hearsay"
                ):
                    effects.append(
                        {
                            "operation": "none",
                            "reason": o.reason
                            or o.unresolved_reason
                            or o.assertion_mode,
                        }
                    )
                    continue
                if o.resource is None:
                    raise Rejected("resource_missing")
                scope = self.config["scope"]
                if (o.resource.provider, o.resource.kind) != (
                    scope["provider"],
                    scope["kind"],
                ):
                    effects.append({"operation": "none", "reason": "out_of_scope"})
                    continue
                # Unknown terms may be retained as events, but must not be mixed into aggregation.
                if o.action in ("supply", "demand"):
                    linked = None
                    if o.target_message:
                        try:
                            target = self.target(o.target_message, o.resource)
                        except Rejected:
                            target = None
                        if (
                            target
                            and target.owner == message.sender_id
                            and target.side == o.action
                        ):
                            if target.status != "active":
                                raise Rejected("target_inactive")
                            if (
                                o.quantity is not None and o.quantity != target.quantity
                            ) or (o.price is not None and o.price != target.price):
                                raise Rejected(
                                    "reaffirmation_conflicts_use_explicit_update"
                                )
                            linked = target
                    if linked:
                        linked = edit(linked)
                        self.link(linked, message)
                        effects.append(
                            {"operation": "link_evidence", "event_id": linked.event_id}
                        )
                    else:
                        identity = (o.action, repr(o.resource.model_dump()))
                        seq = created_counts.get(identity, 0)
                        created_counts[identity] = seq + 1
                        material = repr(
                            (message.key(), o.action, o.resource.model_dump())
                        )
                        if seq:
                            material += f":{seq}"
                        eid = hashlib.sha256(material.encode()).hexdigest()[:16]
                        if eid in self.events:
                            raise Rejected("duplicate_candidate_for_same_resource")
                        before[eid] = None
                        self.events[eid] = Event(
                            event_id=eid,
                            owner=message.sender_id,
                            origin_message=message.ref(),
                            side=o.action,
                            resource=o.resource,
                            quantity=o.quantity,
                            price=o.price,
                            supporting_message_ids=[message.ref()],
                        )
                        effects.append({"operation": "create", "event_id": eid})
                    continue
                if o.target_message is None:
                    raise Rejected("update_target_missing")
                target = self.target(o.target_message, o.resource)
                if target.owner != message.sender_id:
                    raise Rejected("owner_mismatch")
                if target.status != "active":
                    raise Rejected("target_inactive")
                mutation = (target.event_id, o.action)
                if mutation in mutations:
                    raise Rejected("multiple_mutations_of_same_field")
                mutations.add(mutation)
                target = edit(target)
                if o.action == "set_remaining":
                    if not o.quantity or not target.quantity or o.quantity.unit is None:
                        raise Rejected("remaining_quantity_or_unit_missing")
                    if (
                        o.quantity.kind != "stock"
                        or target.quantity.kind != "stock"
                        or o.quantity.unit != target.quantity.unit
                    ):
                        raise Rejected("remaining_unit_mismatch")
                    target.quantity = o.quantity
                elif o.action == "reprice":
                    if (
                        not o.price
                        or not target.price
                        or o.price.currency is None
                        or o.price.unit is None
                    ):
                        raise Rejected("reprice_unit_missing")
                    if (o.price.currency, o.price.unit) != (
                        target.price.currency,
                        target.price.unit,
                    ):
                        raise Rejected("reprice_unit_mismatch")
                    if target.side == "supply" and (
                        o.price.is_soft or o.price.operator != "eq"
                    ):
                        raise Rejected("seller_reprice_not_confirmed_point")
                    target.price = o.price
                elif o.action == "retract":
                    target.status = "retracted"
                self.link(target, message)
                effects.append({"operation": o.action, "event_id": target.event_id})
            self._dirty.update(before)
            return accepted, effects, None
        except Rejected as exc:
            for event_id, original in before.items():
                if original is None:
                    self.events.pop(event_id, None)
                else:
                    self.events[event_id] = original
            return accepted, [], str(exc)

    def aggregate(self):
        asks, amounts = [], {"supply": 0.0, "demand": 0.0}
        excluded = {
            "supply_quantity_missing_or_unit_unknown": 0,
            "demand_quantity_missing_or_unit_unknown": 0,
            "flow_quantity": 0,
            "seller_price_missing_or_not_comparable": 0,
        }
        active = [e for e in self.events.values() if e.status == "active"]
        noncomparable = 0
        for event in active:
            if event.resource.terms_id != self.config["scope"]["aggregation_terms_id"]:
                noncomparable += 1
                continue
            q, p = event.quantity, event.price
            if q and q.kind == "flow":
                excluded["flow_quantity"] += 1
            elif q and q.kind == "stock" and q.unit == "USD_FACE":
                amounts[event.side] += q.value
            else:
                excluded[event.side + "_quantity_missing_or_unit_unknown"] += 1
            if event.side == "supply":
                if (
                    p
                    and p.operator == "eq"
                    and not p.is_soft
                    and p.currency == "CNY"
                    and p.unit == "CNY_PER_USD_FACE"
                ):
                    asks.append(Decimal(str(p.value)))
                else:
                    excluded["seller_price_missing_or_not_comparable"] += 1
        return {
            "observed_supply_usd_face": amounts["supply"],
            "observed_demand_usd_face": amounts["demand"],
            "seller_ask_median_cny_per_usd_face": float(median(asks)) if asks else None,
            "seller_ask_sample_count": len(asks),
            "active_event_count": len(active),
            "excluded": excluded,
            "noncomparable_terms_count": noncomparable,
        }

    def snapshot(
        self, message: Message, scenario_id: str, status: str, materialize: bool = True
    ):
        metadata = {
            "scenario_id": scenario_id,
            "step": len(self.snapshots) + 1,
            "after_message": message.ref().model_dump(),
            "as_of": message.timestamp.isoformat(),
            "processing_status": status,
            "metrics": self.aggregate(),
        }
        changes = {
            key: self.events[key].model_dump(mode="json")
            for key in self.events
            if key in self._dirty
        }
        self.snapshots.append(metadata, changes)
        self._dirty.clear()
        return self.snapshots[-1] if materialize else None
