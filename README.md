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
  - [2.10 主界面事件信息流规划](#210-主界面事件信息流规划)
  - [2.11 采集站点管理与权重体系](#211-采集站点管理与权重体系)
  - [2.12 手动消息录入与自动评估](#212-手动消息录入与自动评估)
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
| 性能 | 主界面事件流刷新延迟 | P95 < 2s |
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
| source-registry-service | 站点注册表与权重管理 | default/override 配置 + 管理指令 | 生效站点清单与权重快照 |
| manual-intake-service | 人工录入、校验与审核流转 | 人工消息表单/API 请求 | 标准化人工事件 |
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

1. 站点注册表服务加载默认配置与覆盖配置，生成当前生效采集清单  
2. 采集层按计划任务/流式方式获取重点金融网站数据  
3. 人工录入服务接收手动消息并进行字段校验、去重与审核流转  
4. 预处理层进行清洗、去重、时间对齐、标的映射  
5. NLP 层输出情绪分数、事件类型、实体列表  
6. 事件影响引擎完成“事件 -> 资产池 -> 多空方向/置信度”判定  
7. 因子层聚合形成分钟/小时级情绪与影响因子  
8. 信号层进行多因子融合并给出交易建议  
9. 回测层验证策略有效性并沉淀评估报告  
10. 监控层持续跟踪延迟、错误率、信号稳定性

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
- 默认加载 `configs/sources_registry.default.yaml` 作为内置站点库基线配置。

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
- `importance_level`（P0/P1/P2）
- `importance_score`（0-100）
- `impacted_markets`（FX/Equity/Bond/Commodity/Derivatives）
- `asset_class`
- `instrument`
- `direction`（long/short/neutral）
- `confidence`（0-1）
- `impact_score`（0-100）
- `long_score`（0-100）
- `short_score`（0-100）
- `net_bias_score`（-100~100，正值偏多、负值偏空）
- `horizon`（intra-day / 1-3d / 1-2w）
- `explanation`（触发原因，便于人工复核）

### 2.10 主界面事件信息流规划

> 主界面聚焦“事件流 + 影响量化 + 可操作信号”，支持交易员秒级研判。

#### 页面布局（建议）
- 顶部：系统状态、数据源健康、告警总览
- 左侧：实时事件信息流（时间倒序、自动去重）
- 中间：事件详情（摘要、原文链接、证据片段）
- 右侧：影响面板（影响市场、影响品种、多空量化、置信度）
- 底部：关联行情微图（1m/5m 变化、波动、成交量）

#### 事件流卡片字段（必须）
- `time`: 事件发布时间（UTC + 本地时区）
- `title`: 事件标题
- `source`: 来源站点与可信度等级
- `importance_level`: 重要性等级（P0/P1/P2）
- `importance_score`: 重要性分数（0-100）
- `impacted_markets`: 影响市场（如 FX、GlobalEquity、Bond）
- `top_impacted_instruments`: 影响最大的前 N 个品种
- `net_bias_score`: 多空净影响（-100~100）
- `direction_summary`: `Bullish / Bearish / Mixed`
- `latency_ms`: 从采集到展示的链路时延

#### 自动重要性评估（模板）
```text
importance_score =
  a1 * source_authority +
  a2 * event_severity +
  a3 * surprise_degree +
  a4 * cross_market_span +
  a5 * liquidity_relevance +
  a6 * recency -
  a7 * duplicate_penalty

importance_level:
  score >= 85 -> P0
  70 <= score < 85 -> P1
  else -> P2
```

#### 多空影响大小量化（模板）
```text
long_score  = 100 * P(long)
short_score = 100 * P(short)
net_bias_score = long_score - short_score

impact_magnitude:
  |net_bias_score| >= 60 -> strong
  30 <= |net_bias_score| < 60 -> medium
  else -> weak
```

### 2.11 采集站点管理与权重体系

> 系统必须支持“哪些网站要采、采集频率、站点权重”可配置可审计，并默认内置大量国内外重点站点。

#### 管理能力（必须）
- 站点管理：新增、编辑、停用、删除、分组、批量导入导出
- 权重管理：支持按站点、类别、国家/地区、事件类型配置权重
- 审计追踪：记录“谁在何时改了哪个站点和权重”
- 生效方式：支持即时热更新（reload）和定时发布（schedule）

#### 权重字段（建议）
- `source_weight`：站点基础权重（0-1）
- `credibility_weight`：可信度权重（0-1）
- `timeliness_weight`：时效性权重（0-1）
- `coverage_weight`：覆盖度权重（0-1）
- `noise_penalty`：噪声惩罚（0-1，越大惩罚越强）

#### 站点有效权重计算（模板）
```text
effective_source_weight =
  0.35 * source_weight +
  0.30 * credibility_weight +
  0.20 * timeliness_weight +
  0.15 * coverage_weight -
  0.20 * noise_penalty
```

#### 默认内置配置策略（建议）
- 内置配置文件：`configs/sources_registry.default.yaml`
- 覆盖规则文件：`configs/sources_registry.override.yaml`
- 启动顺序：先加载 default，再叠加 override
- 配置规模：默认内置国内外重点站点（央行/监管/交易所/统计机构/主流媒体）**60+**
- 变更策略：线上可通过 API 变更，定期落盘回写至 override 文件

#### 站点分层建议
- Tier-0（高优先级）：央行、监管、交易所、国家统计机构（高权重、低轮询间隔）
- Tier-1（中优先级）：全球主流财经媒体（中高权重）
- Tier-2（补充层）：行业媒体、券商观点、精选社媒（中低权重）
- Manual（人工输入源）：研究员/交易员手动录入消息（可按用户角色与历史准确率动态赋权）

### 2.12 手动消息录入与自动评估

> 人工录入消息与机器采集消息进入同一评估链路，统一输出重要性与多资产影响。

#### 录入方式
- Web 主界面录入：支持快捷模板（宏观/公司/地缘/商品）
- API 录入：供外部系统或值班机器人推送人工线索
- 批量导入：CSV/JSON（用于盘后复盘或补录）

#### 人工录入字段（建议）
- `title`：消息标题（必填）
- `content`：消息正文/摘要（必填）
- `source_hint`：线索来源说明（可选）
- `event_time`：事件发生时间（可选）
- `related_instruments`：相关品种（可选，可多值）
- `attachments`：截图/链接/文件（可选）
- `operator_id`：录入人
- `operator_role`：录入人角色（analyst/trader/admin）

#### 自动评估流程
1. 字段合法性校验与敏感信息检查  
2. 重复消息检测（标题相似度 + 实体 + 时间窗口）  
3. NLP 解析（事件类型、实体、情绪、惊奇度）  
4. 重要性评分（importance_score）与分级（P0/P1/P2）  
5. 影响市场识别 + 影响品种召回 + 多空量化（long/short/net）  
6. 输出可审计解释字段（证据片段 + 评分因子）  

#### 审核状态机（建议）
`draft -> submitted -> auto_assessed -> approved/rejected -> published`

#### 人工消息可信度加权（模板）
```text
manual_source_weight =
  base_manual_weight *
  operator_role_multiplier *
  operator_history_multiplier *
  evidence_quality_multiplier
```

---

## 3. 功能清单

- [x] 重点金融网站分层采集（央行/监管/交易所/主流媒体/公告）
- [x] 采集频率与容灾（Webhook + Polling + Retry）
- [x] 采集站点管理（增删改查、启停、分组、批量导入导出）
- [x] 站点权重管理（source/credibility/timeliness/coverage/noise）
- [x] 权重变更审计日志与版本回滚
- [x] 内置默认站点库（国内外重点网站 60+）
- [x] 手动消息录入（UI/API/批量导入）
- [x] 人工消息自动评估（重要性 + 影响市场 + 影响品种 + 多空量化）
- [x] 人工录入审核流（submitted/approved/rejected）与审计追踪
- [x] 文本清洗与标准化
- [x] 实体识别（国家/央行/行业/公司/商品）
- [x] 情绪分类（正向/中性/负向）
- [x] 事件分类（宏观、政策、财报、地缘、供需、评级）
- [x] 事件去重与合并（同事件多源融合）
- [x] 事件影响映射（事件 -> 资产池）
- [x] 多资产多空判断（外汇、股指、个股、期货、国债、贵金属、衍生品）
- [x] 主界面实时事件信息流（支持筛选、排序、搜索）
- [x] 自动重要性分级（P0/P1/P2）与重要性分数
- [x] 置信度与解释字段输出（可审计）
- [x] 多空影响量化（long_score / short_score / net_bias_score）
- [x] 影响市场与影响品种 TopN 自动归因
- [x] P0/P1 实时提醒（IM/邮件/Webhook）
- [x] 因子构建与特征存储
- [x] 交易信号生成与阈值配置
- [x] 历史回测与绩效分析
- [x] 实时看板（行情 + 舆情 + 事件 + 信号）

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
│   ├── strategy.yaml
│   ├── sources_registry.default.yaml
│   ├── sources_registry.override.yaml
│   ├── source_weight_rules.yaml
│   └── manual_input_rules.yaml
├── data/
│   ├── raw/
│   ├── processed/
│   └── features/
├── src/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── nlp/
│   ├── manual_intake/
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

云端/新机器可直接执行：
```bash
bash scripts/bootstrap_cloud_env.sh
```
该脚本会检查并安装运行与测试所需依赖（含 SQLAlchemy、psycopg[binary]、redis、ruff）。

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
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 6.7 运行核心流程（示例）

```bash
python scripts/run_pipeline.py
python scripts/run_backtest.py --start 2024-01-01 --end 2024-12-31 --symbol AAPL --strategy baseline_sentiment
```

### 6.8 API 快速验收（示例）

```bash
# 健康检查
curl -s http://127.0.0.1:8000/api/v1/health

# 登录获取访问令牌（内置测试账号：demo/demo123）
TOKEN=$(curl -s http://127.0.0.1:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"demo","password":"demo123"}' | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

# 查询个性化事件流
curl -s "http://127.0.0.1:8000/api/v1/users/me/feed?page=1&page_size=5" \
  -H "Authorization: Bearer ${TOKEN}"

# 外部事件接入（会自动触发告警判定）
curl -s -X POST "http://127.0.0.1:8000/api/v1/events/ingest" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"source_id":"federal_reserve","title":"Fed 声明偏鹰派","content":"通胀风险仍高，利率路径上调。","event_type":"central_bank_policy","related_instruments":["DXY","UST10Y"]}'

# 告警列表与确认
curl -s "http://127.0.0.1:8000/api/v1/alerts/feed?importance_min=P1" \
  -H "Authorization: Bearer ${TOKEN}"

# 批量接入（自动去重，返回 accepted/deduplicated/rejected 统计）
curl -s -X POST "http://127.0.0.1:8000/api/v1/events/batch-ingest" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"request_id":"batch-001","events":[{"source_id":"federal_reserve","title":"Fed 偏鹰派","content":"通胀风险仍高"},{"source_id":"opec","title":"OPEC 计划减产","content":"供给趋紧"}]}'
# 同 request_id 重放会命中幂等缓存（idempotent_hit=true）
```

### 6.9 日历/Webhook/RBAC 验收（示例）

```bash
# 管理员账号（具备 alerts.write / webhooks.manage / calendar.manage 权限）
ADMIN_TOKEN=$(curl -s http://127.0.0.1:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"sweer1234","password":"dev123"}' | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

# 事件日历与预期差
curl -s "http://127.0.0.1:8000/api/v1/calendar/events?country=US&importance_min=P1"
curl -s "http://127.0.0.1:8000/api/v1/calendar/events/cal_us_nfp_last/surprise"

# 创建 Webhook 订阅并触发一次测试分发
curl -s -X POST "http://127.0.0.1:8000/api/v1/webhooks/subscriptions" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"name":"local-test","url":"https://example.com/webhook","events":["event.created","alert.triggered"]}'
curl -s -X POST "http://127.0.0.1:8000/api/v1/webhooks/dispatch-test" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"

# 失败投递查询与重试
curl -s "http://127.0.0.1:8000/api/v1/webhooks/deliveries?status=failed" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
curl -s -X POST "http://127.0.0.1:8000/api/v1/webhooks/queue/process?limit=50" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
curl -s -X POST "http://127.0.0.1:8000/api/v1/webhooks/retry-failures?limit=20" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
curl -s "http://127.0.0.1:8000/api/v1/webhooks/stats" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
curl -s "http://127.0.0.1:8000/api/v1/webhooks/stats/subscriptions?top_n=10" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
curl -s "http://127.0.0.1:8000/api/v1/webhooks/dlq?status=pending_replay" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
curl -s -X POST "http://127.0.0.1:8000/api/v1/webhooks/dlq/replay?limit=20" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"

# 状态导出/重置（管理员）
SNAPSHOT=$(curl -s "http://127.0.0.1:8000/api/v1/admin/state/export" -H "Authorization: Bearer ${ADMIN_TOKEN}")
curl -s -X POST "http://127.0.0.1:8000/api/v1/admin/state/reset?reseed=false" -H "Authorization: Bearer ${ADMIN_TOKEN}"
curl -s -X POST "http://127.0.0.1:8000/api/v1/admin/state/import?merge=false" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "${SNAPSHOT}"

# 告警升级、日历实际值回填、审计日志
curl -s -X POST "http://127.0.0.1:8000/api/v1/alerts/escalate?force=true&limit=50" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
curl -s -X POST "http://127.0.0.1:8000/api/v1/calendar/events/cal_us_nfp_last/actual" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"actual":235,"consensus":180,"note":"official release"}'
curl -s "http://127.0.0.1:8000/api/v1/audit/logs?limit=20" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
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
| SOURCE_REGISTRY_DEFAULT | 默认站点注册表 | `configs/sources_registry.default.yaml` |
| SOURCE_REGISTRY_OVERRIDE | 自定义覆盖配置 | `configs/sources_registry.override.yaml` |
| SOURCE_WEIGHT_RULES | 权重规则配置 | `configs/source_weight_rules.yaml` |
| MANUAL_INPUT_RULES | 人工录入规则配置 | `configs/manual_input_rules.yaml` |
| MANUAL_INPUT_ENABLED | 是否开启人工录入 | true |
| MANUAL_REVIEW_REQUIRED | 人工录入是否需审核后发布 | true |
| IMPACT_ASSET_UNIVERSE | 多资产池配置 | fx,global_equity,stock,futures,bond,metals,derivatives |
| MODEL_NAME | 默认模型名 | FinBERT |
| EVENT_P0_SLA_SEC | P0 告警时延阈值（秒） | 30 |
| IMPACT_MIN_CONFIDENCE | 影响判定最小置信度 | 0.65 |
| EVENT_FEED_PAGE_SIZE | 主界面事件流分页大小 | 50 |
| EVENT_IMPORTANCE_RULESET | 重要性评分规则配置 | `configs/importance_rules.yaml` |
| PUBLIC_API_TOKEN | 对外 API 鉴权 Token（示例） | `<replace-me>` |
| ALERT_WEBHOOK | 告警回调地址 | `<replace-me>` |
| QSM_WEBHOOK_DELIVERY_RULES | Webhook 队列与退避策略 | `configs/webhook_delivery_rules.yaml` |
| QSM_STATE_PATH | 本地状态持久化文件路径 | `data/state.json` |

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
  "importance_level": "P0",
  "importance_score": 90,
  "impacted_markets": ["FX", "Bond", "PreciousMetal"],
  "published_at": "2026-02-26T00:00:00Z",
  "impacts": [
    {
      "asset_class": "FX",
      "instrument": "USDJPY",
      "direction": "LONG",
      "confidence": 0.82,
      "impact_score": 88,
      "long_score": 84,
      "short_score": 12,
      "net_bias_score": 72,
      "horizon": "intra-day"
    },
    {
      "asset_class": "PreciousMetal",
      "instrument": "XAUUSD",
      "direction": "SHORT",
      "confidence": 0.77,
      "impact_score": 74,
      "long_score": 18,
      "short_score": 79,
      "net_bias_score": -61,
      "horizon": "1-3d"
    }
  ],
  "explanation": [
    "policy_surprise_high",
    "usd_real_yield_up"
  ]
}
```

### 8.5 获取主界面事件信息流（分页）

```http
GET /api/v1/events/feed?from=2026-02-26T00:00:00Z&to=2026-02-26T23:59:59Z&importance_min=70&market=FX&page=1&page_size=50
```

响应示例：

```json
{
  "page": 1,
  "page_size": 50,
  "total": 231,
  "items": [
    {
      "event_id": "evt_20260226_001",
      "time": "2026-02-26T00:00:05Z",
      "title": "FOMC 意外鹰派表态",
      "source": "federalreserve.gov",
      "importance_level": "P0",
      "importance_score": 90,
      "impacted_markets": ["FX", "Bond", "PreciousMetal"],
      "top_impacted_instruments": ["USDJPY", "UST10Y", "XAUUSD"],
      "direction_summary": "Bullish USD / Bearish Gold",
      "net_bias_score": 68,
      "latency_ms": 824
    }
  ]
}
```

### 8.6 实时订阅事件流（WebSocket）

```http
GET /api/v1/stream/events
```

WebSocket 消息示例：

```json
{
  "type": "event_update",
  "event_id": "evt_20260226_001",
  "importance_level": "P0",
  "importance_score": 90,
  "impacted_markets": ["FX", "Bond", "PreciousMetal"],
  "top_impacted_instruments": ["USDJPY", "UST10Y", "XAUUSD"],
  "long_score": 81,
  "short_score": 16,
  "net_bias_score": 65,
  "updated_at": "2026-02-26T00:00:07Z"
}
```

### 8.7 批量查询品种受影响情况

```http
POST /api/v1/impact/batch
Content-Type: application/json
Authorization: Bearer <PUBLIC_API_TOKEN>
```

请求示例：

```json
{
  "instruments": ["EURUSD", "XAUUSD", "SPX", "UST10Y"],
  "window": "4h",
  "importance_min": 60
}
```

响应示例：

```json
{
  "window": "4h",
  "results": [
    {
      "instrument": "EURUSD",
      "events_count": 12,
      "long_score": 44,
      "short_score": 57,
      "net_bias_score": -13,
      "dominant_direction": "SHORT"
    },
    {
      "instrument": "XAUUSD",
      "events_count": 9,
      "long_score": 66,
      "short_score": 24,
      "net_bias_score": 42,
      "dominant_direction": "LONG"
    }
  ]
}
```

### 8.8 对外接口规范（建议）
- 鉴权：`API Token` 或 `OAuth2`
- 限流：按租户进行 QPS 与并发限制
- 幂等：批量查询接口支持 `request_id`
- 版本：`/api/v1`，向后兼容字段只增不减
- 文档：同步提供 OpenAPI / Swagger
- 回调：支持事件触发式 `Webhook` 推送

### 8.9 采集站点管理接口（对外/后台）

#### 8.9.1 查询站点列表
```http
GET /api/v1/sources?enabled=true&tier=0&region=CN&category=regulator
Authorization: Bearer <PUBLIC_API_TOKEN>
```

#### 8.9.2 更新站点权重与启停状态
```http
PATCH /api/v1/sources/{source_id}
Content-Type: application/json
Authorization: Bearer <PUBLIC_API_TOKEN>
```

请求示例：
```json
{
  "enabled": true,
  "source_weight": 0.95,
  "credibility_weight": 0.98,
  "timeliness_weight": 0.92,
  "coverage_weight": 0.90,
  "noise_penalty": 0.05,
  "poll_interval_sec": 15
}
```

#### 8.9.3 批量管理站点
```http
POST /api/v1/sources/batch
Content-Type: application/json
Authorization: Bearer <PUBLIC_API_TOKEN>
```

请求示例：
```json
{
  "operations": [
    {
      "op": "upsert",
      "source_id": "fed_press_releases",
      "enabled": true,
      "source_weight": 0.97
    },
    {
      "op": "disable",
      "source_id": "some_low_quality_site"
    }
  ]
}
```

#### 8.9.4 热加载站点配置
```http
POST /api/v1/sources/reload
Authorization: Bearer <PUBLIC_API_TOKEN>
```

响应示例：
```json
{
  "status": "ok",
  "default_sources_loaded": 69,
  "override_sources_loaded": 4,
  "reloaded_at": "2026-02-26T00:00:00Z"
}
```

#### 8.9.5 导出当前生效配置
```http
GET /api/v1/sources/export?format=yaml
Authorization: Bearer <PUBLIC_API_TOKEN>
```

### 8.10 人工消息录入与评估接口

#### 8.10.1 提交人工消息
```http
POST /api/v1/manual/messages
Content-Type: application/json
Authorization: Bearer <PUBLIC_API_TOKEN>
```

请求示例：
```json
{
  "title": "传某产油国将临时减产，油价盘前异动",
  "content": "盘前市场流传 OPEC+ 某成员国可能追加减产，需持续跟踪官方确认。",
  "event_time": "2026-02-26T08:20:00Z",
  "source_hint": "desk_chat",
  "related_instruments": ["CL", "XOM", "USDCAD"],
  "operator_id": "u_analyst_001",
  "operator_role": "analyst"
}
```

响应示例：
```json
{
  "manual_message_id": "mm_20260226_001",
  "status": "auto_assessed",
  "importance_level": "P1",
  "importance_score": 76,
  "impacted_markets": ["Commodity", "FX", "Equity"],
  "top_impacted_instruments": ["CL", "USDCAD", "XOM"],
  "long_score": 73,
  "short_score": 21,
  "net_bias_score": 52,
  "assessment_explanation": [
    "event_type_supply_shock",
    "cross_market_link_detected",
    "operator_history_multiplier_1.08"
  ]
}
```

#### 8.10.2 查询人工消息评估结果
```http
GET /api/v1/manual/messages/{manual_message_id}
Authorization: Bearer <PUBLIC_API_TOKEN>
```

#### 8.10.3 人工复核操作
```http
POST /api/v1/manual/messages/{manual_message_id}/review
Content-Type: application/json
Authorization: Bearer <PUBLIC_API_TOKEN>
```

请求示例：
```json
{
  "action": "approve",
  "review_comment": "已与官方来源交叉确认，允许发布至事件流"
}
```

#### 8.10.4 触发重新评估
```http
POST /api/v1/manual/messages/{manual_message_id}/re-evaluate
Authorization: Bearer <PUBLIC_API_TOKEN>
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
- 站点抓取成功率（按 source_id）
- 失效站点数与降级站点数
- 权重配置变更次数与回滚次数
- 人工录入消息量（按角色/班次）
- 人工录入自动评估耗时与通过率
- 人工录入复核通过率与误报率
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
python3 -m pytest
bash scripts/run_tests.sh
bash scripts/run_tests_cov.sh
```

说明：`bash scripts/run_tests.sh` 会先自动执行环境依赖检查与补齐（`scripts/bootstrap_cloud_env.sh`），确保在云端新环境可直接跑通。

CI 已内置：`.github/workflows/tests.yml`（push 到 `main` / PR 自动执行 pytest）。

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

### Q5：人工录入的消息会不会影响模型质量？
A：人工录入会进入自动评估与审核流，并引入录入角色、历史准确率、证据质量等因子动态赋权；只有通过阈值与审核规则的消息才会进入主事件流和交易信号链路。

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

- [x] 准备并校验 `configs/sources_registry.default.yaml`（内置站点）
- [x] 准备 `configs/sources_registry.override.yaml`（环境自定义）
- [x] 准备 `configs/manual_input_rules.yaml`（人工录入规则）
- [x] 完成 `.env.example` 并补齐配置注释
- [x] 创建 `src/` 基础模块骨架
- [x] 配置 CI（lint + test）
- [x] 配置监控面板与基础告警
- [x] 打通“采集 -> NLP -> 因子 -> 信号 -> 回测”最小闭环
