# quant-sentiment-monitor

> 金融量化舆情监控 AI 系统：基于 NLP 与量化策略，实时监控市场舆情并生成交易信号。  
> 本文档是**完整 README 模板**，可直接用于项目初始化与团队协作。

---

## 目录

- [1. 项目概述](#1-项目概述)
- [2. 系统规划](#2-系统规划)
  - [2.1 目标与范围](#21-目标与范围)
  - [2.2 核心能力规划](#22-核心能力规划)
  - [2.3 非功能性指标](#23-非功能性指标)
  - [2.4 系统架构设计](#24-系统架构设计)
  - [2.5 模块拆分](#25-模块拆分)
  - [2.6 数据流与时序](#26-数据流与时序)
  - [2.7 迭代路线图](#27-迭代路线图)
  - [2.8 重点金融网站采集规划](#28-重点金融网站采集规划)
  - [2.9 事件影响判定引擎-多资产多空](#29-事件影响判定引擎-多资产多空)
- [3. 功能清单](#3-功能清单)
- [4. 技术栈建议](#4-技术栈建议)
- [5. 项目结构模板](#5-项目结构模板)
- [6. 快速开始](#6-快速开始)
- [7. 配置说明](#7-配置说明)
- [8. API 设计示例](#8-api-设计示例)
- [9. 模型与策略说明](#9-模型与策略说明)
- [10. 回测与评估](#10-回测与评估)
- [11. 可观测性与告警](#11-可观测性与告警)
- [12. 测试与质量保障](#12-测试与质量保障)
- [13. 部署方案](#13-部署方案)
- [14. 安全与合规](#14-安全与合规)
- [15. 贡献指南](#15-贡献指南)
- [16. 常见问题](#16-常见问题)
- [17. 许可证](#17-许可证)
- [18. 联系方式](#18-联系方式)

---

## 1. 项目概述

### 1.1 背景
在高频信息环境下，市场情绪变化会快速反映到价格波动。传统量化因子对新闻、社媒、公告等非结构化信息利用不足，导致信号滞后或遗漏。

### 1.2 愿景
构建一个可扩展、低延迟、可解释的舆情量化系统，实现：
- 自动采集多源金融文本数据
- 实时情绪与事件识别
- 生成可回测、可落地的交易信号
- 提供监控、告警与策略复盘能力

### 1.3 适用场景
- 股票/期货/加密资产舆情监控
- 行业板块情绪热度分析
- 风险事件预警（黑天鹅、政策冲击）
- 量化策略信号增强

---

## 2. 系统规划

### 2.1 目标与范围

#### 业务目标（示例）
- 将舆情到信号的处理链路控制在 **< 60 秒**
- 关键标的覆盖率达到 **> 95%**
- 事件预警召回率达到 **> 80%**

#### MVP 范围（建议）
- 数据源：覆盖重点金融网站（央行/监管/交易所/主流媒体/公告）+ 新闻 + 社媒
- 模型：情绪分类 + 实体识别 + 事件分类（基础版）
- 输出：分钟级情绪指标、事件得分、跨资产多空影响结果、基础交易信号
- 展示：Web 看板 + 告警消息（邮件/IM）

#### 非 MVP（后续迭代）
- 多语言舆情
- 图神经网络关系传播
- 强化学习信号融合
- 自动化策略参数搜索

### 2.2 核心能力规划

1. **数据采集能力**  
   支持重点金融网站的 RSS/API/网页抓取/流式消息接入，具备去重、清洗、重试能力。

2. **NLP 理解能力**  
   完成分词、实体识别（公司/行业/人物）、情绪打分、事件标签识别。

3. **信号生成能力**  
   将情绪因子、事件冲击因子、跨资产影响因子、市场微观结构因子融合，生成可执行信号。

4. **策略评估能力**  
   通过回测引擎评估收益、风险、换手、滑点敏感性。

5. **可观测与运维能力**  
   全链路日志、指标、告警、模型漂移监测与灰度发布。

### 2.3 非功能性指标

| 类别 | 指标 | 目标值（模板） |
|---|---|---|
| 性能 | 舆情入库延迟 | P95 < 10s |
| 性能 | 舆情到信号延迟 | P95 < 60s |
| 性能 | 重要事件提醒时延（P0） | P95 < 30s |
| 可用性 | 核心服务可用率 | > 99.9% |
| 可靠性 | 数据丢失率 | < 0.01% |
| 模型效果 | 多空方向判断准确率（离线） | > 65% |
| 安全性 | 密钥管理 | 使用密钥管理服务/环境变量注入 |
| 可维护性 | 代码覆盖率 | > 80% |

### 2.4 系统架构设计

```mermaid
flowchart LR
    A[外部数据源\n金融网站/API/社媒] --> B[采集层\nCrawler/Connector]
    B --> C[消息队列\nKafka/Redis Stream]
    C --> D[预处理层\n清洗 去重 标准化]
    D --> E[NLP引擎\n情绪 实体 事件抽取]
    E --> K[事件影响引擎\n多资产映射 多空判断]
    E --> F[特征与因子服务\nFeature Store]
    F --> G[信号引擎\nRule/ML]
    K --> G
    G --> H[执行与回测\nBroker/Backtest]
    G --> I[可视化看板\nDashboard]
    E --> I
    F --> I
    K --> I
    I --> J[告警系统\nEmail/IM/Webhook]
```

### 2.5 模块拆分

| 模块 | 职责 | 输入 | 输出 |
|---|---|---|---|
| ingestion-service | 数据采集与接入 | 新闻/社媒/API | 原始文本流 |
| preprocessing-service | 清洗、去重、标准化 | 原始文本 | 标准化文本 |
| nlp-service | 情绪、实体、事件识别 | 标准化文本 | 结构化 NLP 结果 |
| impact-service | 事件影响映射与方向判定 | NLP 结果 + 市场快照 | 资产级多空影响结果 |
| factor-service | 特征生成与存储 | NLP 结果 + 市场数据 | 因子矩阵 |
| signal-service | 信号计算与融合 | 因子矩阵 | 交易信号 |
| backtest-service | 回测评估 | 信号 + 历史行情 | 回测报告 |
| api-gateway | 对外 API 聚合 | 内部服务结果 | REST/WebSocket 输出 |
| monitor-service | 监控与告警 | 系统指标/日志 | 告警消息 |

### 2.6 数据流与时序

1. 采集层按计划任务/流式方式获取重点金融网站数据  
2. 预处理层进行清洗、去重、时间对齐、标的映射  
3. NLP 层输出情绪分数、事件类型、实体列表  
4. 事件影响引擎完成“事件 -> 资产池 -> 多空方向/置信度”判定  
5. 因子层聚合形成分钟/小时级情绪与影响因子  
6. 信号层进行多因子融合并给出交易建议  
7. 回测层验证策略有效性并沉淀评估报告  
8. 监控层持续跟踪延迟、错误率、信号稳定性

### 2.7 迭代路线图

| 阶段 | 时间（示例） | 目标 | 交付物 |
|---|---|---|---|
| Phase 0 | Week 1-2 | 项目初始化 | 仓库结构、CI、基础文档 |
| Phase 1 | Week 3-5 | MVP 打通 | 采集+NLP+信号最小闭环 |
| Phase 2 | Week 6-8 | 回测与监控 | 回测报告、告警与看板 |
| Phase 3 | Week 9-12 | 策略优化 | 因子增强、多模型融合 |
| Phase 4 | Week 13+ | 生产化 | 高可用部署、灰度发布 |

### 2.8 重点金融网站采集规划

> 建议采用“分层采集 + 优先级调度 + 合规接入”策略，确保既快又稳。

| 层级 | 数据源类别（示例） | 采集方式 | 频率/SLA（模板） |
|---|---|---|---|
| Tier-0 | 央行/监管/统计机构/交易所公告（FOMC、ECB、BoE、PBOC、BLS、SEC、CME、HKEX） | 官方 API/RSS/Webhook | 5-15s 轮询或实时推送 |
| Tier-1 | 主流财经媒体（Reuters、Bloomberg、WSJ、FT、CNBC、财新等） | 授权 API + RSS | 30-60s |
| Tier-2 | 券商观点/行业媒体/高质量社媒账号 | API/抓取 | 3-5min |

采集原则：
- 优先使用官方 API/RSS 与授权数据源，避免纯网页抓取依赖。
- 网页抓取需遵守 robots.txt、服务条款与版权要求。
- 建立统一去重指纹（标题 + 时间 + 实体 + 相似度）防止重复触发告警。
- 对 Tier-0 事件启用高优先级队列，保证先处理、先提醒。

关键事件类型（建议优先）：
- 货币政策：利率决议、会议纪要、央行官员讲话
- 宏观数据：CPI、PPI、非农、GDP、失业率
- 地缘政治：战争、制裁、关税、能源中断
- 公司事件：财报、指引、并购、重大诉讼
- 商品供需：原油库存、金属库存、农产品报告

### 2.9 事件影响判定引擎-多资产多空

#### 资产覆盖范围（模板）

| 资产大类 | 典型标的 |
|---|---|
| 外汇 | EURUSD, USDJPY, GBPUSD, DXY |
| 全球股市指数 | SPX, NDX, SX5E, HSI, NKY |
| 个股 | AAPL, TSLA, 0700.HK 等 |
| 期货 | ES, NQ, CL, GC, SI, ZN |
| 国债/利率 | UST2Y, UST10Y, Bund, JGB |
| 贵金属 | XAUUSD, XAGUSD, 铂钯相关合约 |
| 金融衍生品 | ETF 期权、股指期权、利率互换、信用衍生品 |

#### 判定流程（模板）
1. 事件抽取：识别事件类型、主体、时间、数值、语义极性  
2. 资产召回：通过“事件类型-资产映射矩阵”召回候选品种  
3. 方向判定：输出 LONG / SHORT / NEUTRAL 与置信度  
4. 市场确认：结合 1m/5m 价格、波动率、成交量做一致性校验  
5. 告警分级：按影响分数与置信度触发 P0/P1/P2 提醒

#### 影响评分（模板）

```text
impact_score =
  w1 * event_severity +
  w2 * surprise_degree +
  w3 * asset_relevance +
  w4 * source_credibility +
  w5 * market_confirmation -
  w6 * time_decay

direction = argmax(P(long), P(short), P(neutral))
```

#### 输出字段（建议）
- `event_id`
- `asset_class`
- `instrument`
- `direction`（long/short/neutral）
- `confidence`（0-1）
- `impact_score`（0-100）
- `horizon`（intra-day / 1-3d / 1-2w）
- `explanation`（触发原因，便于人工复核）

---

## 3. 功能清单

- [ ] 重点金融网站分层采集（央行/监管/交易所/主流媒体/公告）
- [ ] 采集频率与容灾（Webhook + Polling + Retry）
- [ ] 文本清洗与标准化
- [ ] 实体识别（国家/央行/行业/公司/商品）
- [ ] 情绪分类（正向/中性/负向）
- [ ] 事件分类（宏观、政策、财报、地缘、供需、评级）
- [ ] 事件去重与合并（同事件多源融合）
- [ ] 事件影响映射（事件 -> 资产池）
- [ ] 多资产多空判断（外汇、股指、个股、期货、国债、贵金属、衍生品）
- [ ] 置信度与解释字段输出（可审计）
- [ ] P0/P1 实时提醒（IM/邮件/Webhook）
- [ ] 因子构建与特征存储
- [ ] 交易信号生成与阈值配置
- [ ] 历史回测与绩效分析
- [ ] 实时看板（行情 + 舆情 + 事件 + 信号）

---

## 4. 技术栈建议

> 可按团队现状替换，以下为推荐组合。

- **语言**：Python 3.11+
- **后端框架**：FastAPI
- **任务调度**：Airflow / Celery
- **消息队列**：Kafka / Redis Stream
- **数据存储**：
  - 事务与配置：PostgreSQL
  - 时序/分析：ClickHouse
  - 缓存：Redis
- **模型框架**：PyTorch / Transformers / scikit-learn
- **可观测性**：Prometheus + Grafana + Loki
- **部署**：Docker + Kubernetes（可选）
- **CI/CD**：GitHub Actions

---

## 5. 项目结构模板

```text
quant-sentiment-monitor/
├── README.md
├── LICENSE
├── .gitignore
├── pyproject.toml                  # 或 requirements.txt
├── docker-compose.yml
├── .env.example
├── configs/
│   ├── app.yaml
│   ├── model.yaml
│   └── strategy.yaml
├── data/
│   ├── raw/
│   ├── processed/
│   └── features/
├── src/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── nlp/
│   ├── factors/
│   ├── signals/
│   ├── backtest/
│   ├── api/
│   └── monitoring/
├── scripts/
│   ├── run_pipeline.py
│   ├── train_model.py
│   └── run_backtest.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── docs/
    ├── architecture.md
    ├── api.md
    └── runbook.md
```

---

## 6. 快速开始

### 6.1 环境要求

- Python >= 3.11
- Docker >= 24（可选）
- Git >= 2.40

### 6.2 克隆项目

```bash
git clone <your-repo-url>
cd quant-sentiment-monitor
```

### 6.3 安装依赖

```bash
# 方案 A：pip
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

# 方案 B：poetry（如使用）
poetry install
```

### 6.4 配置环境变量

```bash
cp .env.example .env
# 修改 .env 中的数据库、消息队列、API Key 等配置
```

### 6.5 启动本地依赖服务（可选）

```bash
docker compose up -d
```

### 6.6 启动应用

```bash
# 示例：启动 API
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

### 6.7 运行核心流程（示例）

```bash
python scripts/run_pipeline.py
python scripts/run_backtest.py
```

---

## 7. 配置说明

在 `.env.example` 中定义以下变量（示例）：

| 变量名 | 说明 | 示例 |
|---|---|---|
| APP_ENV | 运行环境 | dev / test / prod |
| APP_PORT | 服务端口 | 8000 |
| DB_URL | 数据库连接串 | postgresql://user:pass@host:5432/db |
| REDIS_URL | Redis 连接串 | redis://localhost:6379/0 |
| KAFKA_BROKERS | Kafka 地址 | localhost:9092 |
| NEWS_API_KEY | 新闻数据源密钥 | `<replace-me>` |
| SOCIAL_API_KEY | 社媒数据源密钥 | `<replace-me>` |
| SOURCE_TIER0_CONFIG | 一级源配置（央行/监管/交易所） | `configs/sources_tier0.yaml` |
| IMPACT_ASSET_UNIVERSE | 多资产池配置 | fx,global_equity,stock,futures,bond,metals,derivatives |
| MODEL_NAME | 默认模型名 | FinBERT |
| EVENT_P0_SLA_SEC | P0 告警时延阈值（秒） | 30 |
| IMPACT_MIN_CONFIDENCE | 影响判定最小置信度 | 0.65 |
| ALERT_WEBHOOK | 告警回调地址 | `<replace-me>` |

---

## 8. API 设计示例

### 8.1 健康检查

```http
GET /api/v1/health
```

响应示例：

```json
{
  "status": "ok",
  "timestamp": "2026-02-26T00:00:00Z"
}
```

### 8.2 获取标的最新情绪

```http
GET /api/v1/sentiment/{symbol}
```

响应示例：

```json
{
  "symbol": "AAPL",
  "sentiment_score": 0.72,
  "confidence": 0.89,
  "updated_at": "2026-02-26T00:00:00Z"
}
```

### 8.3 获取交易信号

```http
GET /api/v1/signals?symbol=AAPL&interval=1m
```

响应示例：

```json
{
  "symbol": "AAPL",
  "signal": "BUY",
  "strength": 0.67,
  "reason": ["positive_sentiment_spike", "event_earnings_positive"],
  "generated_at": "2026-02-26T00:00:00Z"
}
```

### 8.4 获取事件对多资产的影响判断

```http
GET /api/v1/events/{event_id}/impact
```

响应示例：

```json
{
  "event_id": "evt_20260226_001",
  "title": "FOMC 意外鹰派表态",
  "published_at": "2026-02-26T00:00:00Z",
  "impacts": [
    {
      "asset_class": "FX",
      "instrument": "USDJPY",
      "direction": "LONG",
      "confidence": 0.82,
      "impact_score": 88,
      "horizon": "intra-day"
    },
    {
      "asset_class": "PreciousMetal",
      "instrument": "XAUUSD",
      "direction": "SHORT",
      "confidence": 0.77,
      "impact_score": 74,
      "horizon": "1-3d"
    }
  ],
  "explanation": [
    "policy_surprise_high",
    "usd_real_yield_up"
  ]
}
```

---

## 9. 模型与策略说明

### 9.1 NLP 模型层
- 情绪模型：`<模型名称>`（如 FinBERT）
- 实体识别模型：`<模型名称>`
- 事件分类模型：`<模型名称>`

### 9.2 因子构建（示例）
- `sentiment_mean_5m`：5 分钟平均情绪分
- `negative_spike_1m`：1 分钟负向情绪突增
- `event_risk_score`：风险事件强度

### 9.3 信号策略（示例）
```text
if sentiment_mean_5m > 0.6 and event_risk_score < 0.2:
    BUY
elif sentiment_mean_5m < -0.6 or event_risk_score > 0.7:
    SELL
else:
    HOLD
```

### 9.4 多资产多空判断逻辑（模板）

#### 规则层（先验）
- 央行鹰派超预期：通常利多本币、利空长久期国债、利空黄金
- 增长数据显著走强：通常利多周期股/工业品，利空避险债
- 地缘冲突升级：通常利多黄金/原油，利空风险资产

#### 模型层（统计学习）
- 输入：事件类型、语义极性、惊奇度、资产暴露关系、历史反应窗口
- 输出：`P(long) / P(short) / P(neutral)` 与置信度

#### 校验层（市场确认）
- 若方向判定与短期市场反应严重背离，则降权或不触发强提醒
- 仅在 `confidence >= 阈值` 且 `impact_score >= 阈值` 时触发交易级信号

#### 事件到资产映射示例

| 事件 | 外汇 | 全球股市 | 国债 | 贵金属 |
|---|---|---|---|---|
| 美联储超预期鹰派 | 美元偏多 | 美股估值承压偏空 | 美债偏空（收益率上行） | 黄金偏空 |
| 美国通胀显著回落 | 美元偏空 | 风险资产偏多 | 美债偏多（收益率下行） | 黄金偏多 |
| 地缘冲突升级 | 避险货币偏多 | 风险资产偏空 | 核心国债偏多 | 黄金偏多 |

---

## 10. 回测与评估

### 10.1 核心评估指标
- 年化收益（Annual Return）
- 最大回撤（Max Drawdown）
- 夏普比率（Sharpe Ratio）
- 胜率（Win Rate）
- 换手率（Turnover）

### 10.2 回测命令（示例）

```bash
python scripts/run_backtest.py \
  --start 2024-01-01 \
  --end 2025-12-31 \
  --symbol AAPL \
  --strategy baseline_sentiment
```

### 10.3 输出产物
- 回测净值曲线
- 指标报表（CSV/HTML）
- 交易记录明细

---

## 11. 可观测性与告警

### 11.1 监控指标（建议）
- 数据采集吞吐（条/秒）
- 队列堆积长度
- 模型推理延迟（P50/P95/P99）
- 信号生成成功率
- API 错误率（5xx）

### 11.2 告警策略（示例）
- 延迟连续 5 分钟超过阈值触发 P1 告警
- 模型置信度异常下降触发漂移告警
- 关键服务不可用触发电话/IM 升级

### 11.3 事件提醒分级与 SLA（建议）

| 等级 | 触发条件（示例） | 通知渠道 | 时延目标 |
|---|---|---|---|
| P0 | 央行决议/战争升级/主权评级突变，且 `impact_score >= 85` | 电话 + IM + Webhook | P95 < 30s |
| P1 | CPI/NFP/财报爆雷等重大事件，且 `impact_score >= 70` | IM + 邮件 + Webhook | P95 < 120s |
| P2 | 一般风险事件或低置信提示 | IM/邮件汇总 | 5-10min |

执行建议：
- 同一事件在时间窗口内进行去重，避免告警风暴。
- 支持“首次触发 + 升级触发 + 解除通知”三段式提醒。
- 告警消息必须附带方向、置信度、影响品种与解释字段。

---

## 12. 测试与质量保障

### 12.1 测试类型
- 单元测试（unit）
- 集成测试（integration）
- 端到端测试（e2e）

### 12.2 执行命令（示例）

```bash
pytest -q
pytest tests/integration -q
```

### 12.3 质量门禁（建议）
- PR 必须通过单测与静态检查
- 覆盖率门槛：`>= 80%`
- 关键模块必须有回归用例

---

## 13. 部署方案

### 13.1 开发环境
- Docker Compose 一键启动依赖服务

### 13.2 生产环境（建议）
- Kubernetes 部署微服务
- HPA 自动扩缩容
- 蓝绿或金丝雀发布

### 13.3 CI/CD（示例）
1. push 触发 lint + test  
2. 构建镜像并推送镜像仓库  
3. 部署到 staging 自动验收  
4. 人工审批后发布 prod

---

## 14. 安全与合规

- 不在仓库中提交任何密钥或凭证
- 使用 `.env` + 密钥管理服务注入敏感信息
- 记录数据来源与授权范围，遵守平台协议
- 对日志进行脱敏处理（账户、手机号、Token）

---

## 15. 贡献指南

1. Fork / 新建功能分支
2. 提交前运行测试与格式化
3. 按约定提交 Commit Message（建议 Conventional Commits）
4. 发起 PR 并补充变更说明、测试结果与风险评估

示例：

```bash
git checkout -b feat/sentiment-factor
git commit -m "feat: add sentiment factor aggregation"
```

---

## 16. 常见问题

### Q1：没有实时数据源怎么办？
A：可先接入公开新闻 API 与历史社媒数据，优先打通离线链路。

### Q2：如何验证信号是否有效？
A：先做样本外回测，再做模拟盘（paper trading），最后小资金实盘验证。

### Q3：模型更新频率如何设置？
A：建议按周或按月重训，并在重大行情变化后触发临时重训。

### Q4：能否覆盖外汇、全球股市、个股、期货、国债、贵金属和衍生品？
A：可以。通过“事件影响判定引擎”先做事件到资产池映射，再输出各品种多空方向与置信度，并结合实时行情做二次确认后再告警/出信号。

---

## 17. 许可证

本项目采用 [MIT License](./LICENSE)（可按需替换）。

---

## 18. 联系方式

- 项目负责人：`<姓名>`
- 邮箱：`<邮箱>`
- 团队频道：`<Slack/飞书/钉钉链接>`

---

## 附：初始化检查清单

- [ ] 完成 `.env.example` 并补齐配置注释
- [ ] 创建 `src/` 基础模块骨架
- [ ] 配置 CI（lint + test）
- [ ] 配置监控面板与基础告警
- [ ] 打通“采集 -> NLP -> 因子 -> 信号 -> 回测”最小闭环
