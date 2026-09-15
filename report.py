"""Offline market dashboard. Embedded controls; no network, CDN, or credentials."""

import html
import json
from datetime import datetime, timedelta
from pathlib import Path
from snapshot_log import load_snapshots
from report_metrics import window_activity
from string import Template


def esc(value):
    return html.escape(str(value), quote=True)


def fmt(value):
    if value is None:
        return "—"
    if isinstance(value, (int, float)):
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return esc(value)


def compact(value):
    if value is None:
        return "—"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}".rstrip("0").rstrip(".") + "M"
    if abs(value) >= 1000:
        return f"{value / 1000:.1f}".rstrip("0").rstrip(".") + "k"
    return fmt(value)


def asks_at(snapshot, terms):
    """Same comparability rules as the event engine, without buyer budgets."""
    values = []
    for e in snapshot["events"]:
        p = e.get("price")
        if (
            e["status"] == "active"
            and e["side"] == "supply"
            and e["resource"]["terms_id"] == terms
            and p
            and p["operator"] == "eq"
            and not p["is_soft"]
            and p["currency"] == "CNY"
            and p["unit"] == "CNY_PER_USD_FACE"
        ):
            values.append(p["value"])
    return values


def window_snapshots(snapshots, hours):
    if not snapshots or hours is None:
        return snapshots
    cutoff = datetime.fromisoformat(snapshots[-1]["as_of"]) - timedelta(hours=hours)
    return [s for s in snapshots if datetime.fromisoformat(s["as_of"]) >= cutoff]


def price_chart(snaps, terms):
    width, height, left, right, top, bottom = 1080, 275, 65, 25, 24, 44
    pw, ph = width - left - right, height - top - bottom
    groups = [asks_at(s, terms) for s in snaps]
    known = [v for group in groups for v in group]
    lo, hi = min(known, default=0), max(known, default=1)
    padding = max((hi - lo) * 0.3, 0.12)
    lo, hi = max(0, lo - padding), hi + padding
    x = lambda i: left + (i + 0.5) * pw / max(1, len(snaps))
    y = lambda v: top + ph * (hi - v) / (hi - lo)
    pieces = []
    for i in range(5):
        v = lo + (hi - lo) * i / 4
        pieces.append(
            f'<line x1="{left}" y1="{y(v)}" x2="{width-right}" y2="{y(v)}" stroke="#e9edf3" stroke-dasharray="4 5"/><text x="{left-14}" y="{y(v)+4}" text-anchor="end">{v:.2f}</text>'
        )
    for i, (s, group) in enumerate(zip(snaps, groups)):
        if not group:
            continue
        mid = s["metrics"]["seller_ask_median_cny_per_usd_face"]
        low, high = min(group), max(group)
        if i and groups[i - 1]:
            prev = snaps[i - 1]["metrics"]["seller_ask_median_cny_per_usd_face"]
            prevlow, prevhigh = min(groups[i - 1]), max(groups[i - 1])
            pieces.append(
                f'<path d="M{x(i-1)} {y(prevhigh)} H{x(i)} V{y(high)} V{y(low)} V{y(prevlow)} H{x(i-1)} Z" fill="#dfe7ff" opacity=".8"/>'
            )
            pieces.append(
                f'<path d="M{x(i-1)} {y(prev)} H{x(i)} V{y(mid)}" fill="none" stroke="#5965e8" stroke-width="2.5"/>'
            )
        tooltip = f'步骤 {s["step"]} · 中位价 {fmt(mid)} · 范围 {fmt(low)}–{fmt(high)} · {len(group)} 个报价'
        pieces.append(
            f'<line x1="{x(i)}" y1="{y(low)}" x2="{x(i)}" y2="{y(high)}" stroke="#a1acf7" stroke-width="2"/><circle cx="{x(i)}" cy="{y(mid)}" r="4.5" fill="#5965e8" stroke="white" stroke-width="2"><title>{esc(tooltip)}</title></circle>'
        )
    stride = max(1, (len(snaps) + 9) // 10)
    multiple_days = len({datetime.fromisoformat(s["as_of"]).date() for s in snaps}) > 1
    for i, s in enumerate(snaps):
        if i % stride == 0 or i == len(snaps) - 1:
            label = datetime.fromisoformat(s["as_of"]).strftime(
                "%m/%d %H:%M" if multiple_days else "%H:%M"
            )
            pieces.append(
                f'<text x="{x(i)}" y="{height-16}" text-anchor="middle">{label} · {s["step"]}</text>'
            )
    if not known:
        pieces.append(
            f'<text x="{width/2}" y="{height/2}" text-anchor="middle">暂无可比卖方报价</text>'
        )
    return f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="独立卖方报价中位数与最低最高范围">{"".join(pieces)}</svg>'


def change(current, previous):
    if current is None or previous is None:
        return "暂无可比基准"
    if previous == 0:
        return "无变化" if current == 0 else f"新增 {compact(current)}"
    pct = (current - previous) / abs(previous) * 100
    if abs(pct) < 0.05:
        return "— 0%"
    return f'{"↑" if pct>0 else "↓"} {abs(pct):.1f}%'


def render_report(pipeline, path: Path) -> None:
    snaps, records = pipeline.store.snapshots, pipeline.records
    summary = pipeline.summary()
    terms = pipeline.config["scope"]["aggregation_terms_id"]
    fixture = summary["mode"] == "fixture"
    mode_badge = (
        '<div class="live"><span class="dot slate"></span>固定标签回放 · 仅验证下游，不是模型评测</div>'
        if fixture
        else ""
    )
    models = sorted(
        {
            r["extraction"]["model"]
            for r in records
            if r.get("extraction") and r["extraction"].get("model")
        }
    )
    channels = {r["message"]["channel_id"] for r in records}
    platform_labels = {"wechat": "WeChat", "telegram": "Telegram"}
    platforms = sorted(
        {
            platform_labels.get(
                pipeline.config["channels"].get(c, {}).get("platform"), c
            )
            for c in channels
        }
    )
    source = " / ".join(platforms) or "无来源"
    failed_count = sum(r["status"] == "failed" for r in records)
    unresolved_count = sum(r["status"] == "unresolved" for r in records)
    latest = (
        datetime.fromisoformat(snaps[-1]["as_of"]).strftime("%Y-%m-%d %H:%M UTC%z")
        if snaps
        else "暂无数据"
    )
    refs = {
        (r["message"]["channel_id"], r["message"]["message_id"]): f"msg-{i}"
        for i, r in enumerate(records)
    }
    labels = {
        "supply": "供给",
        "demand": "求购",
        "set_remaining": "剩余量",
        "reprice": "改价",
        "retract": "撤回",
        "ignore": "忽略",
        "unresolved": "待确认",
    }
    panels = []
    for key, hours in [("24h", 24), ("7d", 168), ("all", None)]:
        selected = window_snapshots(snaps, hours)
        m = selected[-1]["metrics"] if selected else summary["final_metrics"]
        first = selected[0]["metrics"] if selected else m
        asks = asks_at(selected[-1], terms) if selected else []
        activity = window_activity(selected, records, terms)
        supply, demand = m["observed_supply_usd_face"], m["observed_demand_usd_face"]
        median = m["seller_ask_median_cny_per_usd_face"]
        ratio = f"{supply/demand:.2f}" if demand else "—"
        seller_count = (
            sum(
                e["status"] == "active" and e["side"] == "supply"
                for e in selected[-1]["events"]
            )
            if selected
            else 0
        )
        coverage = f"{len(asks)/seller_count:.0%}" if seller_count else "—"
        spread = fmt(max(asks) - min(asks)) if asks else "—"
        price = f"{median:.2f}" if median is not None else "—"
        bounds = f"{fmt(min(asks))}–{fmt(max(asks))}" if asks else "暂无报价"
        cards = Template(
            (Path(__file__).parent / "templates/cards.html").read_text()
        ).substitute(
            {
                "price": price,
                "change_median_first_seller_ask_median_cny_per_usd_face": change(
                    median, first["seller_ask_median_cny_per_usd_face"]
                ),
                "len_asks": len(asks),
                "compact_supply": compact(supply),
                "change_supply_first_observed_supply_usd_face": change(
                    supply, first["observed_supply_usd_face"]
                ),
                "compact_demand": compact(demand),
                "change_demand_first_observed_demand_usd_face": change(
                    demand, first["observed_demand_usd_face"]
                ),
                "m_active_event_count": m["active_event_count"],
                "esc_pipeline_scenario_id": esc(pipeline.scenario_id),
            }
        )
        max_amount = max(supply, demand, 1)
        selected_refs = {
            (s["after_message"]["channel_id"], s["after_message"]["message_id"])
            for s in selected
        }
        signal_rows = []
        for r in reversed(records):
            msg = r["message"]
            ref = (msg["channel_id"], msg["message_id"])
            if ref not in selected_refs:
                continue
            obs = r["observations"]
            actions = list(
                dict.fromkeys(labels.get(o["action"], o["action"]) for o in obs)
            )
            action = (
                "转发关联"
                if msg["forwarded_origin"]
                else " / ".join(actions)
                or {"failed": "提取失败", "unresolved": "待确认"}.get(
                    r["status"], "无信号"
                )
            )
            state = (
                "待确认"
                if r["status"] == "unresolved"
                else "失败" if r["status"] == "failed" else "已处理"
            )
            badge = "warn" if r["status"] in ("failed", "unresolved") else "good"
            stamp = datetime.fromisoformat(msg["timestamp"]).strftime("%H:%M")
            signal_rows.append(
                f'<tr><td class="mono">{stamp}</td><td><b>{esc(msg["sender_id"])}</b><span class="cell-sub">{esc(msg["channel_id"])}</span></td><td><span class="tag">{esc(action)}</span></td><td class="signal-text">{esc(msg["text"])}</td><td><span class="status {badge}">{state}</span></td><td><a href="#{refs[ref]}" aria-label="查看 {esc(msg["message_id"])} 的证据">证据 ↗</a></td></tr>'
            )
        exclusions = m["excluded"]
        excluded_count = sum(exclusions.values())
        exclusion_text = " · ".join(
            f"{name} {exclusions[k]}"
            for name, k in [
                ("供应单位缺失", "supply_quantity_missing_or_unit_unknown"),
                ("需求单位缺失", "demand_quantity_missing_or_unit_unknown"),
                ("流量", "flow_quantity"),
                ("卖价不可比", "seller_price_missing_or_not_comparable"),
            ]
        )
        panels.append(
            Template(
                (Path(__file__).parent / "templates/window.html").read_text()
            ).substitute(
                {
                    "key": key,
                    "hidden_if_key_24h_else": " hidden" if key != "24h" else "",
                    "cards": cards,
                    "price_chart_selected_terms": price_chart(selected, terms),
                    "bounds": bounds,
                    "compact_activity_new_supply": compact(activity["new_supply"]),
                    "compact_activity_new_demand": compact(activity["new_demand"]),
                    "fmt_activity_quote_median": fmt(activity["quote_median"]),
                    "activity_quote_events": activity["quote_events"],
                    "compact_supply": compact(supply),
                    "format_100_supply_max_amount_3f": format(
                        100 * supply / max_amount, ".3f"
                    ),
                    "compact_demand": compact(demand),
                    "format_100_demand_max_amount_3f": format(
                        100 * demand / max_amount, ".3f"
                    ),
                    "ratio": ratio,
                    "change_supply_first_observed_supply_usd_face": change(
                        supply, first["observed_supply_usd_face"]
                    ),
                    "change_demand_first_observed_demand_usd_face": change(
                        demand, first["observed_demand_usd_face"]
                    ),
                    "spread": spread,
                    "len_asks": len(asks),
                    "seller_count": seller_count,
                    "coverage": coverage,
                    "len_signal_rows": len(signal_rows),
                    "join_signal_rows_or_tr_td_colspan_6_td_tr": "".join(signal_rows)
                    or '<tr><td colspan="6">窗口内暂无消息</td></tr>',
                    "esc_terms": esc(terms),
                    "excluded_count": excluded_count,
                    "m_noncomparable_terms_count": m["noncomparable_terms_count"],
                    "esc_exclusion_text": esc(exclusion_text),
                    "failed_count": failed_count,
                    "unresolved_count": unresolved_count,
                }
            )
        )
    event_rows = []
    for event in snaps[-1]["events"] if snaps else []:
        q, p = event.get("quantity"), event.get("price")
        quantity = (
            "未知"
            if q is None
            else f'{fmt(q["value"])} {esc(q["unit"] or "单位未知")} · {esc(q["kind"])}'
        )
        price = (
            "未知"
            if p is None
            else f'{esc(p["operator"])} {fmt(p["value"])} {esc(p["currency"] or "币种未知")} / {esc(p["unit"] or "口径未知")}'
        )
        evidence = " ".join(
            f'<a href="#{refs.get((r["channel_id"],r["message_id"]),"messages")}">{esc(r["message_id"])}</a>'
            for r in event["supporting_message_ids"]
        )
        event_rows.append(
            f'<tr><td>{esc(event["owner"])}<span class="cell-sub">{esc(event["event_id"])}</span></td><td>{esc(event["side"])}</td><td>{quantity}</td><td>{price}</td><td>{esc(event["status"])}</td><td>{evidence}</td></tr>'
        )
    event_table = f'<section class="panel"><div class="section-head"><div><h2>Events &amp; evidence</h2><p>最新时点的事件状态 · 包含已撤回事件</p></div></div><div class="scroll"><table><thead><tr><th>发布者 / 事件</th><th>供需</th><th>数量</th><th>价格</th><th>状态</th><th>证据</th></tr></thead><tbody>{"".join(event_rows) or "<tr><td colspan=\"6\">暂无事件</td></tr>"}</tbody></table></div></section>'
    details = []
    for i, r in enumerate(records):
        msg = r["message"]
        payload = {k: r[k] for k in ("observations", "effects", "reason")}
        details.append(
            f'<details class="message-detail" id="msg-{i}"><summary><span class="mono">{esc(msg["message_id"])}</span> <b>{esc(msg["sender_id"])}</b> <span>{esc(msg["text"])}</span><em>{esc(r["status"])}</em></summary><p class="hint">{esc(msg["timestamp"])} · {esc(msg["channel_id"])}</p><pre>{esc(json.dumps(payload,ensure_ascii=False,indent=2))}</pre></details>'
        )
    time_rows = "".join(
        f'<tr><td>{s["step"]}</td><td>{esc(s["as_of"])}</td><td>{fmt(s["metrics"]["observed_supply_usd_face"])}</td><td>{fmt(s["metrics"]["observed_demand_usd_face"])}</td><td>{fmt(s["metrics"]["seller_ask_median_cny_per_usd_face"])}</td><td>{s["metrics"]["seller_ask_sample_count"]}</td></tr>'
        for s in snaps
    )
    css = (Path(__file__).parent / "templates/report.css").read_text()
    script = (Path(__file__).parent / "templates/report.js").read_text()
    content = Template(
        (Path(__file__).parent / "templates/report.html").read_text()
    ).substitute(
        scenario=esc(pipeline.scenario_id),
        css=css,
        mode_badge=mode_badge,
        source=esc(source),
        latest=esc(latest),
        panels="".join(panels),
        event_table=event_table,
        details="".join(details),
        snapshot_count=len(snaps),
        time_rows=time_rows,
        summary=esc(json.dumps(summary, ensure_ascii=False, indent=2)),
        models=esc(" / ".join(models)),
        calls=summary["api_calls"],
        script=script,
    )

    path.write_text(content, encoding="utf-8")


def render_saved_run(directory: Path, config_path: Path):
    """Rebuild presentation from saved results without any model request."""
    import hashlib
    from types import SimpleNamespace
    from schema import read_jsonl

    config = json.loads(config_path.read_text())
    summary = json.loads((directory / "summary.json").read_text())
    digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    if summary.get("config_sha256") != digest:
        raise ValueError(
            "Saved run config does not match; supply its original --config."
        )
    pipeline = SimpleNamespace(
        config=config,
        scenario_id=summary["scenario_id"],
        records=read_jsonl(directory / "observations.jsonl"),
        store=SimpleNamespace(snapshots=load_snapshots(directory / "snapshots.jsonl")),
        summary=lambda: summary,
    )
    render_report(pipeline, directory / "report.html")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Refresh a saved report without calling a model."
    )
    parser.add_argument(
        "--run",
        type=Path,
        required=True,
        help="Directory containing summary and JSONL outputs",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Defaults to saved run config, then dataset/config.json",
    )
    args = parser.parse_args()
    saved_config = args.run / "config.json"
    config_path = args.config or (
        saved_config
        if saved_config.exists()
        else Path(__file__).parent / "dataset/config.json"
    )
    render_saved_run(args.run, config_path)
    print(args.run / "report.html")
