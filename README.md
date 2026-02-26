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
- 数据源：新闻 + 社媒（至少 2 类）
- 模型：情绪分类 + 实体识别 + 事件分类（基础版）
- 输出：分钟级情绪指标、事件得分、基础交易信号
- 展示：Web 看板 + 告警消息（邮件/IM）

#### 非 MVP（后续迭代）
- 多语言舆情
- 图神经网络关系传播
- 强化学习信号融合
- 自动化策略参数搜索

### 2.2 核心能力规划

1. **数据采集能力**  
   支持 RSS/API/网页抓取/流式消息接入，具备去重、清洗、重试能力。

2. **NLP 理解能力**  
   完成分词、实体识别（公司/行业/人物）、情绪打分、事件标签识别。

3. **信号生成能力**  
   将情绪因子、事件冲击因子、市场微观结构因子融合，生成可执行信号。

4. **策略评估能力**  
   通过回测引擎评估收益、风险、换手、滑点敏感性。

5. **可观测与运维能力**  
   全链路日志、指标、告警、模型漂移监测与灰度发布。

### 2.3 非功能性指标

| 类别 | 指标 | 目标值（模板） |
|---|---|---|
| 性能 | 舆情入库延迟 | P95 < 10s |
| 性能 | 舆情到信号延迟 | P95 < 60s |
| 可用性 | 核心服务可用率 | > 99.9% |
| 可靠性 | 数据丢失率 | < 0.01% |
| 安全性 | 密钥管理 | 使用密钥管理服务/环境变量注入 |
| 可维护性 | 代码覆盖率 | > 80% |

### 2.4 系统架构设计

```mermaid
flowchart LR
    A[外部数据源\n新闻/API/社媒] --> B[采集层\nCrawler/Connector]
    B --> C[消息队列\nKafka/Redis Stream]
    C --> D[预处理层\n清洗 去重 标准化]
    D --> E[NLP引擎\n情绪 实体 事件]
    E --> F[特征与因子服务\nFeature Store]
    F --> G[信号引擎\nRule/ML]
    G --> H[执行与回测\nBroker/Backtest]
    G --> I[可视化看板\nDashboard]
    E --> I
    F --> I
    I --> J[告警系统\nEmail/IM/Webhook]
```

### 2.5 模块拆分

| 模块 | 职责 | 输入 | 输出 |
|---|---|---|---|
| ingestion-service | 数据采集与接入 | 新闻/社媒/API | 原始文本流 |
| preprocessing-service | 清洗、去重、标准化 | 原始文本 | 标准化文本 |
| nlp-service | 情绪、实体、事件识别 | 标准化文本 | 结构化 NLP 结果 |
| factor-service | 特征生成与存储 | NLP 结果 + 市场数据 | 因子矩阵 |
| signal-service | 信号计算与融合 | 因子矩阵 | 交易信号 |
| backtest-service | 回测评估 | 信号 + 历史行情 | 回测报告 |
| api-gateway | 对外 API 聚合 | 内部服务结果 | REST/WebSocket 输出 |
| monitor-service | 监控与告警 | 系统指标/日志 | 告警消息 |

### 2.6 数据流与时序

1. 采集层按计划任务/流式方式获取文本数据  
2. 预处理层进行清洗、去重、时间对齐、标的映射  
3. NLP 层输出情绪分数、事件类型、实体列表  
4. 因子层聚合形成分钟/小时级情绪因子  
5. 信号层进行多因子融合并给出交易建议  
6. 回测层验证策略有效性并沉淀评估报告  
7. 监控层持续跟踪延迟、错误率、信号稳定性

### 2.7 迭代路线图

| 阶段 | 时间（示例） | 目标 | 交付物 |
|---|---|---|---|
| Phase 0 | Week 1-2 | 项目初始化 | 仓库结构、CI、基础文档 |
| Phase 1 | Week 3-5 | MVP 打通 | 采集+NLP+信号最小闭环 |
| Phase 2 | Week 6-8 | 回测与监控 | 回测报告、告警与看板 |
| Phase 3 | Week 9-12 | 策略优化 | 因子增强、多模型融合 |
| Phase 4 | Week 13+ | 生产化 | 高可用部署、灰度发布 |

---

## 3. 功能清单

- [ ] 多源舆情采集（新闻、社媒、公告）
- [ ] 文本清洗与标准化
- [ ] 实体识别（股票、公司、行业）
- [ ] 情绪分类（正向/中性/负向）
- [ ] 事件分类（财报、政策、风险事件等）
- [ ] 因子构建与特征存储
- [ ] 交易信号生成与阈值配置
- [ ] 历史回测与绩效分析
- [ ] 实时看板（行情 + 舆情 + 信号）
- [ ] 告警系统（邮件/钉钉/Slack/Webhook）

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
| MODEL_NAME | 默认模型名 | FinBERT |
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
