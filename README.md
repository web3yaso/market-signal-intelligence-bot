# Market Signal Intelligence Bot

A local take-home prototype that turns noisy chat messages into auditable supply/demand events and market snapshots.

**总用时约 4 小时，使用 AI 辅助设计、编码、测试与文档整理。** 单一 LLM 提取候选；确定性代码处理证据校验、来源去重、事件更新及聚合。

## 1. 先看演示

克隆仓库后，用浏览器打开 **[demo/index.html](demo/index.html)**。无需密钥或服务器，即可查看保存的真实模型结果：

- **WeChat / 中文**：[D1 报告](demo/final/D1/report.html)，36 条消息、七个日期、10 位卖家与 6 位买家。
- **Telegram / 英文**：[D2 报告](demo/final/D2/report.html)，12 条消息，含独立报价、需求、转发与回复更新。

所有消息均为自行构造的 mock 数据。两个场景分别统计，不把相似的跨平台用户自动合并。当前聚合仅支持固定预付条款的 OpenAI API 美元面值额度；报价单位为 **CNY / USD 面值**，不是美元售价。

报告含卖方中位价与范围、供需数量、最近信号、事件证据和历史快照。24h / 7d 按样例末条消息时间筛选；未撤回的较早报价仍进入当前存量。Resource / Source 展示当前场景，尚不支持跨场景筛选。可比报价覆盖率不是模型置信度。

## 2. 安装与离线验收

建议 Python 3.14（本次验证环境）；代码需要 Python 3.12+。运行：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock
python -m unittest discover -s tests -v
python dataset/validate.py
python evaluate.py --mode fixture --split all --out outputs/fixture-check
```

fixture 模式显式使用人工标签，只验证事件引擎，不代表模型准确率。已有 HTML 报告则来自真实 API 调用；两者有明确模式标记。

## 3. 重新调用模型生成演示

```bash
cp -n .env.example .env
```

在本机填写 `.env`：

```dotenv
OPENAI_API_KEY=your_api_key
OPENAI_MODEL=gpt-5.6-luna
OPENAI_BASE_URL=https://api.openai.com/v1
INPUT_PRICE_PER_MILLION=
OUTPUT_PRICE_PER_MILLION=
```

模型必须是 API ID。客户端使用 Chat Completions JSON mode，要求服务支持 `response_format: json_object`、`max_completion_tokens` 和 `store: false`。价格字段可留空；费用未知会显示 null。环境变量优先于 `.env`。不会在缺少配置时自动退回 fixture。

```bash
python main.py --mode api --input dataset/demo/messages.jsonl --config dataset/demo/config.json --manifest dataset/demo/manifest.json --scenario all --out outputs/demo
```

输出为 `outputs/demo/D1/report.html` 和 `outputs/demo/D2/report.html`。只刷新已有结果的样式，不调用模型：

```bash
python report.py --run outputs/demo/D1
```

每个运行目录保存 observations.jsonl（含 raw 模型输出）、snapshots.jsonl、summary.json、config.json 和 report.html。重新生成时核对配置摘要。新模型调用可能产生不同结果；提交的报告保留本次实测，未手工替换预测。

## 4. 架构与业务边界

```text
Mock Telegram / WeChat → 统一 Message → 去重与回复上下文
  → 单次 LLM 提取 → 数据契约与证据校验 → 事件更新
  → 独立快照 → 供需 / 卖方报价聚合 → HTML 报告
```

| 文件 | 职责 |
| --- | --- |
| schema.py | Message、Observation、Event 数据契约 |
| ingest.py | JSONL 回放、消息索引、有界回复链 |
| extract.py | 模型输入、token 预算、HTTP 调用与结构校验 |
| events.py | 归一化、权限校验、去重、状态变更与聚合 |
| main.py | Pipeline.process(Message)、CLI、保存运行结果 |
| report.py | 离线看板与已有结果重绘 |
| evaluate.py | 标签匹配、字段与状态指标、逐例差异 |

平台接入与智能处理以 **Message** 分界。JSONL 只是回放载体；未来平台连接器只需输出相同对象。下游不依赖平台 SDK，也不按语言分叉。中文 WeChat / 英文 Telegram 是新演示的数据约定，不是生产平台检测规则。

关键规则：

- 同键重复投递不再调用模型；明确转发只增加来源证据；无来源关联的同文报价分别建事件。
- 只有原发布者引用唯一目标时，才能替换剩余量、确认改价或撤回。买方意向不表示成交，也不减少供应。
- 未知单位保留 null；默认单位只来自公开群配置。日用量是 flow，不作为 stock 累加。
- 买方预算和软报价不进入卖方中位价。价格只比较同条款、币种和分母的有效卖方报价。
- 每条消息原子更新；校验拒绝不改变事件。每步快照深复制，撤回不会改写旧状态。
- 每条唯一消息最多一次模型请求，最多三层回复上下文；输入预算 3000、输出上限 1000 tokens，无自动重试。20 秒为 HTTP 网络操作超时，不是整个程序的总期限。

## 5. 评测与限制

详见 **[EVALUATION.md](EVALUATION.md)**：分开报告 20 条原基准与 48 条扩展演示，包含真实字段错误、处理失败和最终聚合差异。原 holdout 已在开发过程中查看过，最终重复运行属于回归检查，不宣称新的独立盲测。提示词、原始模型输出及用量均随结果保存。

- 只有 20 条完整标签基准，不能据此声称生产泛化能力；新演示的最终状态核对也不是逐字段准确率。
- 无 TTL：active 表示尚未撤回，不保证仍可成交。更新失败会保留旧状态，可能使库存或价格过时。
- 无引用的指代、模糊数词、复杂多资源消息仍可能失败；证据字面校验不能证明全部语言语义。
- 没有真实平台连接器、数据库或重启恢复。quality score、双路径提取、复杂生命周期均后置。
- HTML 结构和交互脚本已检查；未完成浏览器截图级视觉验收。

设计见 [DESIGN.md](DESIGN.md)，其中包含范围、取舍、评分标准对照与耗时说明。
