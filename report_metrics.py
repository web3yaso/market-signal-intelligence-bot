"""Window activity is distinct from the current outstanding stock."""

from statistics import median


def window_activity(snapshots, records: list[dict], terms: str) -> dict:
    by_step = {
        step: row
        for step, row in enumerate(
            (
                r
                for r in records
                if r["status"] not in ("duplicate", "unsupported_edit")
            ),
            1,
        )
    }
    amounts = {"supply": 0.0, "demand": 0.0}
    quotes = {}
    for snapshot in snapshots:
        row = by_step.get(snapshot["step"], {})
        events = {event["event_id"]: event for event in snapshot["events"]}
        for effect in row.get("effects", []):
            event = events.get(effect.get("event_id"))
            if event is None or event["resource"]["terms_id"] != terms:
                continue
            quantity, price = event["quantity"], event["price"]
            if (
                effect["operation"] == "create"
                and quantity
                and quantity["kind"] == "stock"
                and quantity["unit"] == "USD_FACE"
            ):
                amounts[event["side"]] += quantity["value"]
            if (
                effect["operation"] in ("create", "reprice")
                and event["side"] == "supply"
                and price
                and price["operator"] == "eq"
                and not price["is_soft"]
                and price["currency"] == "CNY"
                and price["unit"] == "CNY_PER_USD_FACE"
            ):
                quotes[event["event_id"]] = price["value"]
    return {
        "new_supply": amounts["supply"],
        "new_demand": amounts["demand"],
        "quote_median": median(quotes.values()) if quotes else None,
        "quote_events": len(quotes),
    }
