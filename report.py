"""Offline market dashboard. Embedded controls; no network, CDN, or credentials."""
import html
import json
from datetime import datetime, timedelta
from pathlib import Path


def esc(value):
    return html.escape(str(value), quote=True)


def fmt(value):
    if value is None:
        return '—'
    if isinstance(value, (int, float)):
        return f'{value:,.4f}'.rstrip('0').rstrip('.')
    return esc(value)


def compact(value):
    if value is None:
        return '—'
    if abs(value) >= 1_000_000:
        return f'{value / 1_000_000:.2f}'.rstrip('0').rstrip('.') + 'M'
    if abs(value) >= 1000:
        return f'{value / 1000:.1f}'.rstrip('0').rstrip('.') + 'k'
    return fmt(value)


def asks_at(snapshot, terms):
    """Same comparability rules as the event engine, without buyer budgets."""
    values = []
    for e in snapshot['events']:
        p = e.get('price')
        if (e['status'] == 'active' and e['side'] == 'supply'
                and e['resource']['terms_id'] == terms and p
                and p['operator'] == 'eq' and not p['is_soft']
                and p['currency'] == 'CNY' and p['unit'] == 'CNY_PER_USD_FACE'):
            values.append(p['value'])
    return values


def window_snapshots(snapshots, hours):
    if not snapshots or hours is None:
        return snapshots
    cutoff = datetime.fromisoformat(snapshots[-1]['as_of']) - timedelta(hours=hours)
    return [s for s in snapshots if datetime.fromisoformat(s['as_of']) >= cutoff]


def price_chart(snaps, terms):
    width, height, left, right, top, bottom = 1080, 275, 65, 25, 24, 44
    pw, ph = width-left-right, height-top-bottom
    groups = [asks_at(s, terms) for s in snaps]
    known = [v for group in groups for v in group]
    lo, hi = min(known, default=0), max(known, default=1)
    padding = max((hi-lo)*.3, .12)
    lo, hi = max(0, lo-padding), hi+padding
    x = lambda i: left + (i+.5)*pw/max(1,len(snaps))
    y = lambda v: top + ph*(hi-v)/(hi-lo)
    pieces = []
    for i in range(5):
        v = lo+(hi-lo)*i/4
        pieces.append(f'<line x1="{left}" y1="{y(v)}" x2="{width-right}" y2="{y(v)}" stroke="#e9edf3" stroke-dasharray="4 5"/><text x="{left-14}" y="{y(v)+4}" text-anchor="end">{v:.2f}</text>')
    for i, (s, group) in enumerate(zip(snaps,groups)):
        if not group:
            continue
        mid = s['metrics']['seller_ask_median_cny_per_usd_face']
        low, high = min(group), max(group)
        if i and groups[i-1]:
            prev = snaps[i-1]['metrics']['seller_ask_median_cny_per_usd_face']
            prevlow, prevhigh = min(groups[i-1]),max(groups[i-1])
            pieces.append(f'<path d="M{x(i-1)} {y(prevhigh)} H{x(i)} V{y(high)} V{y(low)} V{y(prevlow)} H{x(i-1)} Z" fill="#dfe7ff" opacity=".8"/>')
            pieces.append(f'<path d="M{x(i-1)} {y(prev)} H{x(i)} V{y(mid)}" fill="none" stroke="#5965e8" stroke-width="2.5"/>')
        tooltip = f'步骤 {s["step"]} · 中位价 {fmt(mid)} · 范围 {fmt(low)}–{fmt(high)} · {len(group)} 个报价'
        pieces.append(f'<line x1="{x(i)}" y1="{y(low)}" x2="{x(i)}" y2="{y(high)}" stroke="#a1acf7" stroke-width="2"/><circle cx="{x(i)}" cy="{y(mid)}" r="4.5" fill="#5965e8" stroke="white" stroke-width="2"><title>{esc(tooltip)}</title></circle>')
    stride = max(1, (len(snaps)+9)//10)
    multiple_days = len({datetime.fromisoformat(s['as_of']).date() for s in snaps}) > 1
    for i,s in enumerate(snaps):
        if i % stride == 0 or i == len(snaps)-1:
            label = datetime.fromisoformat(s['as_of']).strftime('%m/%d %H:%M' if multiple_days else '%H:%M')
            pieces.append(f'<text x="{x(i)}" y="{height-16}" text-anchor="middle">{label} · {s["step"]}</text>')
    if not known:
        pieces.append(f'<text x="{width/2}" y="{height/2}" text-anchor="middle">暂无可比卖方报价</text>')
    return f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="独立卖方报价中位数与最低最高范围">{"".join(pieces)}</svg>'


def change(current, previous):
    if current is None or previous is None:
        return '暂无可比基准'
    if previous == 0:
        return '无变化' if current == 0 else f'新增 {compact(current)}'
    pct = (current-previous)/abs(previous)*100
    if abs(pct) < .05:
        return '— 0%'
    return f'{"↑" if pct>0 else "↓"} {abs(pct):.1f}%'


def render_report(pipeline, path: Path):
    snaps, records = pipeline.store.snapshots, pipeline.records
    summary = pipeline.summary()
    terms = pipeline.config['scope']['aggregation_terms_id']
    fixture = summary['mode'] == 'fixture'
    mode_badge = '<div class="live"><span class="dot slate"></span>固定标签回放 · 仅验证下游，不是模型评测</div>' if fixture else ''
    models = sorted({r['extraction']['model'] for r in records if r.get('extraction') and r['extraction'].get('model')})
    channels = {r['message']['channel_id'] for r in records}
    platform_labels = {'wechat': 'WeChat', 'telegram': 'Telegram'}
    platforms = sorted({platform_labels.get(pipeline.config['channels'][c].get('platform'), c) for c in channels})
    source = ' / '.join(platforms) or '无来源'
    failed_count = sum(r['status'] == 'failed' for r in records)
    unresolved_count = sum(r['status'] == 'unresolved' for r in records)
    latest = datetime.fromisoformat(snaps[-1]['as_of']).strftime('%Y-%m-%d %H:%M UTC%z') if snaps else '暂无数据'
    refs = {(r['message']['channel_id'],r['message']['message_id']):f'msg-{i}' for i,r in enumerate(records)}
    labels = {'supply':'供给','demand':'求购','set_remaining':'剩余量','reprice':'改价','retract':'撤回','ignore':'忽略','unresolved':'待确认'}
    panels = []
    for key, hours in [('24h',24),('7d',168),('all',None)]:
        selected = window_snapshots(snaps,hours)
        m = selected[-1]['metrics'] if selected else summary['final_metrics']
        first = selected[0]['metrics'] if selected else m
        asks = asks_at(selected[-1],terms) if selected else []
        supply, demand = m['observed_supply_usd_face'],m['observed_demand_usd_face']
        median = m['seller_ask_median_cny_per_usd_face']
        ratio = f'{supply/demand:.2f}' if demand else '—'
        seller_count = sum(e['status']=='active' and e['side']=='supply' for e in selected[-1]['events']) if selected else 0
        coverage = f'{len(asks)/seller_count:.0%}' if seller_count else '—'
        spread = fmt(max(asks)-min(asks)) if asks else '—'
        price = f'{median:.2f}' if median is not None else '—'
        bounds = f'{fmt(min(asks))}–{fmt(max(asks))}' if asks else '暂无报价'
        cards = f'''<div class="kpis">
<section class="kpi featured"><div class="eyebrow"><span class="resource-icon">O</span> OpenAI API <span class="pill">美元面值额度</span></div><div class="value"><small>¥</small> {price}</div><div class="card-bottom"><span>卖方报价中位数</span><span class="delta">{change(median,first['seller_ask_median_cny_per_usd_face'])}</span></div><div class="hint">CNY / USD 面值 · {len(asks)} 个独立报价</div></section>
<section class="kpi"><div class="eyebrow"><span class="dot green"></span> Observed supply</div><div class="value">{compact(supply)}<small> USD</small></div><div class="card-bottom"><span>已观察供应量</span><span class="delta">{change(supply,first['observed_supply_usd_face'])}</span></div><div class="hint">单位明确的未撤回库存</div></section>
<section class="kpi"><div class="eyebrow"><span class="dot purple"></span> Observed demand</div><div class="value">{compact(demand)}<small> USD</small></div><div class="card-bottom"><span>已观察需求量</span><span class="delta">{change(demand,first['observed_demand_usd_face'])}</span></div><div class="hint">买方意向 · 不代表成交</div></section>
<section class="kpi"><div class="eyebrow"><span class="dot slate"></span> Active signals</div><div class="value">{m['active_event_count']}<small> events</small></div><div class="card-bottom"><span>去重活跃事件</span><span class="pill">{esc(pipeline.scenario_id)}</span></div><div class="hint">已关联来源的转发不重复累计</div></section></div>'''
        max_amount = max(supply,demand,1)
        selected_refs = {(s['after_message']['channel_id'],s['after_message']['message_id']) for s in selected}
        signal_rows = []
        for r in reversed(records):
            msg=r['message']; ref=(msg['channel_id'],msg['message_id'])
            if ref not in selected_refs:
                continue
            obs=r['observations']
            actions=list(dict.fromkeys(labels.get(o['action'],o['action']) for o in obs))
            action='转发关联' if msg['forwarded_origin'] else ' / '.join(actions) or {'failed':'提取失败','unresolved':'待确认'}.get(r['status'],'无信号')
            state='待确认' if r['status']=='unresolved' else '失败' if r['status']=='failed' else '已处理'
            badge='warn' if r['status'] in ('failed','unresolved') else 'good'
            stamp=datetime.fromisoformat(msg['timestamp']).strftime('%H:%M')
            signal_rows.append(f'<tr><td class="mono">{stamp}</td><td><b>{esc(msg["sender_id"])}</b><span class="cell-sub">{esc(msg["channel_id"])}</span></td><td><span class="tag">{esc(action)}</span></td><td class="signal-text">{esc(msg["text"])}</td><td><span class="status {badge}">{state}</span></td><td><a href="#{refs[ref]}" aria-label="查看 {esc(msg["message_id"])} 的证据">证据 ↗</a></td></tr>')
        exclusions=m['excluded']
        excluded_count=sum(exclusions.values())
        exclusion_text=' · '.join(f'{name} {exclusions[k]}' for name,k in [('供应单位缺失','supply_quantity_missing_or_unit_unknown'),('需求单位缺失','demand_quantity_missing_or_unit_unknown'),('流量','flow_quantity'),('卖价不可比','seller_price_missing_or_not_comparable')])
        panels.append(f'''<div class="window-panel" data-window="{key}"{' hidden' if key!='24h' else ''}>{cards}
<section class="panel price-panel"><div class="section-head"><div><h2>Median price <span class="light">+ range</span></h2><p>独立卖方报价中位数与最低—最高范围 · CNY / USD 面值</p></div><div class="legend"><span><i class="line-key"></i> 中位数</span><span><i class="range-key"></i> 报价范围</span></div></div>{price_chart(selected,terms)}<div class="chart-footer"><span>模拟消息时间（UTC）· 数字为步骤；无报价时留空</span><span>当前范围 <b>{bounds}</b></span></div></section>
<div class="split"><section class="panel"><div class="section-head"><div><h2>Supply vs demand</h2><p>已观察供需 · USD 面值额度</p></div><span class="tiny-label">CURRENT SNAPSHOT</span></div><div class="bar-row"><span><i class="dot green"></i> Supply</span><b>{compact(supply)}</b></div><div class="bar-track"><div class="bar-fill supply" style="width:{100*supply/max_amount:.3f}%"></div></div><div class="bar-row"><span><i class="dot purple"></i> Demand</span><b>{compact(demand)}</b></div><div class="bar-track"><div class="bar-fill demand" style="width:{100*demand/max_amount:.3f}%"></div></div><div class="ratio"><span>Supply / Demand ratio</span><strong>{ratio}</strong></div><p class="hint">需求为零时比值留空。未知单位与日用量不进入数量统计。</p></section>
<section class="panel"><div class="section-head"><div><h2>Market health</h2><p>变化相对窗口首条快照</p></div><span class="pill">描述性指标</span></div><div class="health-row"><span>供应量变化</span><b>{change(supply,first['observed_supply_usd_face'])}</b></div><div class="health-row"><span>需求量变化</span><b>{change(demand,first['observed_demand_usd_face'])}</b></div><div class="health-row"><span>卖价极差 <small>CNY / USD</small></span><b>{spread}</b></div><div class="health-row"><span>可比报价覆盖 <small>{len(asks)}/{seller_count} 活跃卖方</small></span><b>{coverage}</b></div><p class="hint">覆盖率表示可比卖方报价占比，不是模型置信度。</p></section></div>
<section class="panel recent"><div class="section-head"><div><h2>Recent signals</h2><p>最近消息、状态变更与原文证据</p></div><span class="pill">{len(signal_rows)} 条消息</span></div><div class="scroll"><table><thead><tr><th>时间</th><th>来源 / 发布者</th><th>信号</th><th>内容</th><th>处理状态</th><th></th></tr></thead><tbody>{''.join(signal_rows) or '<tr><td colspan="6">窗口内暂无消息</td></tr>'}</tbody></table></div></section>
<div class="data-note"><span class="note-icon">i</span><div><b>统计口径</b>　固定条款 {esc(terms)}；未撤回不保证仍有货。排除项计数合计 {excluded_count}（可重叠），条款不可比 {m['noncomparable_terms_count']}。<br><span>{esc(exclusion_text)}。窗口筛选时间线和最近消息，卡片保留最新时点仍活跃的供需存量。本次回放处理失败 {failed_count} 条，待确认 {unresolved_count} 条。</span></div></div></div>''')
    event_rows=[]
    for event in snaps[-1]['events'] if snaps else []:
        q,p=event.get('quantity'),event.get('price')
        quantity='未知' if q is None else f'{fmt(q["value"])} {esc(q["unit"] or "单位未知")} · {esc(q["kind"])}'
        price='未知' if p is None else f'{esc(p["operator"])} {fmt(p["value"])} {esc(p["currency"] or "币种未知")} / {esc(p["unit"] or "口径未知")}'
        evidence=' '.join(f'<a href="#{refs.get((r["channel_id"],r["message_id"]),"messages")}">{esc(r["message_id"])}</a>' for r in event['supporting_message_ids'])
        event_rows.append(f'<tr><td>{esc(event["owner"])}<span class="cell-sub">{esc(event["event_id"])}</span></td><td>{esc(event["side"])}</td><td>{quantity}</td><td>{price}</td><td>{esc(event["status"])}</td><td>{evidence}</td></tr>')
    event_table=f'<section class="panel"><div class="section-head"><div><h2>Events &amp; evidence</h2><p>最新时点的事件状态 · 包含已撤回事件</p></div></div><div class="scroll"><table><thead><tr><th>发布者 / 事件</th><th>供需</th><th>数量</th><th>价格</th><th>状态</th><th>证据</th></tr></thead><tbody>{"".join(event_rows) or "<tr><td colspan=\"6\">暂无事件</td></tr>"}</tbody></table></div></section>'
    details=[]
    for i,r in enumerate(records):
        msg=r['message']
        payload={k:r[k] for k in ('observations','effects','reason')}
        details.append(f'<details class="message-detail" id="msg-{i}"><summary><span class="mono">{esc(msg["message_id"])}</span> <b>{esc(msg["sender_id"])}</b> <span>{esc(msg["text"])}</span><em>{esc(r["status"])}</em></summary><p class="hint">{esc(msg["timestamp"])} · {esc(msg["channel_id"])}</p><pre>{esc(json.dumps(payload,ensure_ascii=False,indent=2))}</pre></details>')
    time_rows=''.join(f'<tr><td>{s["step"]}</td><td>{esc(s["as_of"])}</td><td>{fmt(s["metrics"]["observed_supply_usd_face"])}</td><td>{fmt(s["metrics"]["observed_demand_usd_face"])}</td><td>{fmt(s["metrics"]["seller_ask_median_cny_per_usd_face"])}</td><td>{s["metrics"]["seller_ask_sample_count"]}</td></tr>' for s in snaps)
    css = '''
*{box-sizing:border-box}body{margin:0;background:#f5f6fa;color:#242c40;font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",sans-serif}main{max-width:1240px;margin:auto;padding:34px 36px 40px}h1,h2,p{margin:0}h1{font-size:26px;letter-spacing:-.7px;font-weight:650}h2{font-size:17px;font-weight:650;letter-spacing:-.25px}.header{display:flex;justify-content:space-between;align-items:center;gap:20px}.brand{display:flex;gap:13px;align-items:center}.brand-icon{background:#5864df;color:white;width:43px;height:43px;display:grid;place-items:center;border-radius:12px;font-size:24px}.subtitle{font-size:12px;color:#8490a6;margin-top:2px}.live{font-size:12px;color:#556178;white-space:nowrap}.live .dot{margin-right:6px}.toolbar{margin:25px 0 20px;display:flex;align-items:center;gap:12px;flex-wrap:wrap}.tabs{display:flex;background:#e9ecf3;padding:4px;border-radius:9px;gap:2px}button{font:inherit;border:0;cursor:pointer}.tabs button{background:transparent;color:#6c768d;border-radius:6px;padding:5px 19px;font-size:12px;font-weight:600}.tabs button[aria-pressed=true]{background:white;color:#4652cf;box-shadow:0 1px 4px #15243a12}select{font:inherit;font-size:12px;color:#4f5b72;padding:9px 32px 9px 12px;border:1px solid #dfe4ec;border-radius:8px;background:white}.filter-label{font-size:11px;color:#8892a6;margin-right:6px}.window-note{margin-left:auto;color:#8a94a8;font-size:11px}.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:15px;margin-bottom:20px}.kpi,.panel{background:white;border:1px solid #e3e7ef;border-radius:12px;box-shadow:0 2px 3px #24345002}.kpi{padding:20px 19px}.featured{border-top:3px solid #6570e7;padding-top:18px}.eyebrow{display:flex;gap:7px;align-items:center;font-size:12px;font-weight:600;color:#647087;white-space:nowrap}.resource-icon{width:21px;height:21px;border-radius:6px;background:#eceeff;color:#5b64d9;display:inline-grid;place-items:center;font-size:12px}.pill{background:#f1f3f8;color:#7b859b;font-size:10px;font-weight:500;padding:3px 7px;border-radius:5px;white-space:nowrap}.eyebrow .pill{margin-left:auto}.value{font-size:34px;font-weight:650;letter-spacing:-1.2px;margin:16px 0 9px;line-height:1.15;font-variant-numeric:tabular-nums}.value small{font-size:15px;font-weight:450;letter-spacing:0;color:#9aa3b4}.card-bottom{display:flex;justify-content:space-between;gap:6px;font-size:11px;color:#7b859a}.delta{color:#5965a8;font-weight:600}.hint{font-size:11px;color:#929bae;margin-top:8px}.panel{padding:23px;margin-bottom:20px}.section-head{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:20px}.section-head p{font-size:11px;color:#919bad;margin-top:3px}.light{font-weight:450;color:#8892a8}.legend{display:flex;gap:18px;font-size:11px;color:#8590a4}.legend span{display:flex;align-items:center;gap:6px}.line-key{width:16px;height:2px;background:#5965e8}.range-key{width:13px;height:9px;background:#dfe7ff;border-radius:2px}svg{width:100%;display:block;overflow:visible}svg text{font:10px -apple-system,sans-serif;fill:#8d97aa}.chart-footer{display:flex;justify-content:space-between;border-top:1px solid #f0f2f7;padding-top:13px;color:#909aac;font-size:10px}.chart-footer b{color:#65718a;font-weight:500}.split{display:grid;grid-template-columns:1fr 1fr;gap:20px}.tiny-label{font-size:9px;letter-spacing:1px;color:#a0a8b7}.dot{display:inline-block;width:7px;height:7px;border-radius:50%;flex-shrink:0}.green{background:#51b89b}.purple{background:#8580e8}.slate{background:#8495ad}.bar-row{display:flex;justify-content:space-between;margin:15px 0 7px;font-size:12px;color:#6c7890}.bar-row .dot{margin-right:7px}.bar-row b{color:#3b4660;font-weight:600}.bar-track{height:12px;background:#f2f4f8;border-radius:4px;overflow:hidden}.bar-fill{height:100%;border-radius:4px}.supply{background:linear-gradient(90deg,#64c3a9,#4fb598)}.demand{background:linear-gradient(90deg,#aaa4f4,#817be2)}.ratio{margin-top:23px;border-top:1px solid #edf0f5;padding-top:14px;display:flex;justify-content:space-between;align-items:center;font-size:12px;color:#758197}.ratio strong{font-size:22px;color:#3c4764}.health-row{display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #f0f2f7;padding:11px 0;font-size:12px;color:#738097}.health-row b{color:#505d7c;font-weight:600}.health-row small{font-size:10px;color:#a0a9b8;margin-left:5px}.recent{padding-bottom:6px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;text-align:left;font-size:12px}th{background:#f8f9fc;color:#98a1b1;font-size:10px;font-weight:500;padding:10px 12px;white-space:nowrap}td{padding:14px 12px;border-bottom:1px solid #f0f2f6;color:#65718a}tbody tr:last-child td{border-bottom:0}.mono{font-variant-numeric:tabular-nums;color:#8e98ad;font-size:11px}td b{font-weight:550;color:#4f5b74}.cell-sub{display:block;font-size:10px;color:#9da5b5}.tag{background:#eff1fa;color:#717baf;font-size:10px;padding:4px 7px;border-radius:5px;white-space:nowrap}.signal-text{max-width:420px;min-width:230px;color:#536079;font-size:12px}.status{font-size:10px;white-space:nowrap}.status:before{content:"";display:inline-block;width:5px;height:5px;border-radius:50%;background:currentColor;margin-right:5px}.good{color:#56a38d}.warn{color:#bd914e}a{color:#6873c8;text-decoration:none;white-space:nowrap}a:hover{text-decoration:underline}.data-note{display:flex;gap:10px;color:#8691a6;font-size:10px;line-height:1.9;margin-bottom:26px}.data-note b{font-weight:600;color:#68758e}.note-icon{display:inline-grid;place-items:center;border:1px solid #a9b2c2;width:15px;height:15px;border-radius:50%;flex-shrink:0;margin-top:3px}.audit{border-top:1px solid #e3e7ef;padding-top:20px}.audit h2{font-size:13px;color:#7f8aa0;margin-bottom:12px}.message-detail{scroll-margin-top:16px;background:white;border:1px solid #e5e9f0;border-radius:7px;margin:7px 0;padding:10px 13px}.message-detail:target{border-color:#7d87dd}.message-detail summary{display:flex;align-items:center;gap:13px;cursor:pointer;font-size:11px;color:#7b879e}.message-detail summary>span:nth-of-type(2){flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.message-detail em{font-style:normal;font-size:10px;color:#8b95aa}.message-detail b{font-weight:550;color:#5a6680}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:11px;color:#6d7990;max-height:480px;overflow:auto}details>summary{cursor:pointer}.raw{margin:15px 0;font-size:12px;color:#7c88a0}.footer{display:flex;justify-content:space-between;gap:12px;margin-top:26px;color:#a0a8b8;font-size:10px}.footer span{overflow-wrap:anywhere}[hidden]{display:none!important}:focus-visible{outline:2px solid #6b74e6;outline-offset:3px}@media(max-width:1000px){.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.window-note{width:100%;margin:0}.eyebrow .pill{margin-left:0}}@media(max-width:680px){main{padding:20px 15px}h1{font-size:21px}.header{align-items:flex-start;flex-direction:column;gap:12px}.split{grid-template-columns:1fr;gap:0}.kpis{gap:10px}.kpi{padding:15px}.featured{padding-top:13px}.value{font-size:29px}.eyebrow{font-size:10px;white-space:normal;flex-wrap:wrap}.card-bottom{flex-wrap:wrap}.panel{padding:16px}.section-head{align-items:flex-start;flex-wrap:wrap}.legend{font-size:10px}.chart-footer{gap:10px}.tiny-label{display:none}.footer{flex-direction:column}.message-detail summary>span:nth-of-type(2){max-width:40vw}}@media print{body{background:white}main{padding:0}.toolbar,.audit{display:none}.panel,.kpi{break-inside:avoid;box-shadow:none}}
'''
    script = '''document.querySelectorAll('[data-window-button]').forEach(button=>button.addEventListener('click',()=>{document.querySelectorAll('[data-window-button]').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));document.querySelectorAll('[data-window]').forEach(panel=>{panel.hidden=panel.dataset.window!==button.dataset.windowButton;});}));function showEvidence(){const element=document.getElementById(location.hash.slice(1));if(element && element.matches('details.message-detail'))element.open=true;}window.addEventListener('hashchange',showEvidence);showEvidence();'''
    content=f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Market Signal Intelligence · {esc(pipeline.scenario_id)}</title><style>{css}</style></head><body><main>
<header class="header"><div class="brand"><div class="brand-icon" aria-hidden="true">≋</div><div><h1>Market Signal Intelligence</h1><p class="subtitle">AI RESOURCE MONITOR · 群聊资源信号</p></div></div>{mode_badge}</header>
<div class="toolbar"><div class="tabs" role="group" aria-label="时间窗口"><button type="button" data-window-button="24h" aria-pressed="true">24h</button><button type="button" data-window-button="7d" aria-pressed="false">7d</button><button type="button" data-window-button="all" aria-pressed="false">全部</button></div><label><span class="filter-label">Resource</span><select aria-label="资源（当前场景只有一种）"><option>OpenAI API 额度</option></select></label><label><span class="filter-label">Source</span><select aria-label="来源（当前场景）"><option>{esc(source)} · {esc(pipeline.scenario_id)}</option></select></label><span class="window-note">以样例末条时间为锚点 · {esc(latest)}</span></div>
{''.join(panels)}{event_table}<section class="audit"><h2 id="messages">MESSAGE EVIDENCE · 输入消息与处理结果</h2>{''.join(details)}<details class="raw"><summary>逐步快照 · {len(snaps)} 个独立状态</summary><div class="scroll"><table><thead><tr><th>步骤</th><th>模拟时间</th><th>供应</th><th>需求</th><th>中位价</th><th>报价数</th></tr></thead><tbody>{time_rows}</tbody></table></div></details><details class="raw"><summary>运行信息与调用用量</summary><pre>{esc(json.dumps(summary,ensure_ascii=False,indent=2))}</pre></details></section>
<footer class="footer"><span>模拟群聊 · 单资源原型 · 不包含模型置信度评分</span><span>{esc(' / '.join(models))} · {esc(pipeline.scenario_id)} · {summary['api_calls']} model calls</span></footer><noscript><p class="hint">JavaScript 未启用：显示默认 24h 窗口；消息证据仍可展开。</p></noscript></main><script type="text/javascript">{script}</script></body></html>'''
    path.write_text(content,encoding='utf-8')


def render_saved_run(directory: Path, config_path: Path):
    """Rebuild presentation from saved results without any model request."""
    import hashlib
    from types import SimpleNamespace
    from schema import read_jsonl
    config = json.loads(config_path.read_text())
    summary = json.loads((directory / 'summary.json').read_text())
    digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    if summary.get('config_sha256') != digest:
        raise ValueError('Saved run config does not match; supply its original --config.')
    pipeline = SimpleNamespace(
        config=config, scenario_id=summary['scenario_id'],
        records=read_jsonl(directory / 'observations.jsonl'),
        store=SimpleNamespace(snapshots=read_jsonl(directory / 'snapshots.jsonl')),
        summary=lambda: summary,
    )
    render_report(pipeline, directory / 'report.html')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Refresh a saved report without calling a model.')
    parser.add_argument('--run', type=Path, required=True, help='Directory containing summary and JSONL outputs')
    parser.add_argument('--config', type=Path, help='Defaults to saved run config, then dataset/config.json')
    args = parser.parse_args()
    saved_config = args.run/'config.json'
    config_path = args.config or (saved_config if saved_config.exists() else Path(__file__).parent/'dataset/config.json')
    render_saved_run(args.run, config_path)
    print(args.run / 'report.html')
