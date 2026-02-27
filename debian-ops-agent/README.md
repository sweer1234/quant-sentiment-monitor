# Debian Ops Agent（安全白名单版）

一个可直接运行的 Debian 运维智能体模板，支持：

- 安装/卸载软件包（`apt-get install/remove`）
- 服务管理（`systemctl status/start/stop/restart`）
- 日志与系统诊断（`journalctl`、`ss`、`df`、`free`、`ping`、`curl`）
- 审计日志记录（每次调用写入 JSON 行日志）
- 写操作审批令牌（防止误操作）

> 设计目标：不允许模型直接执行任意 shell，只允许策略白名单中的命令。

---

## 1. 目录结构

```text
debian-ops-agent/
├── app/
│   ├── __init__.py
│   ├── config.py
│   ├── executor.py
│   ├── main.py
│   ├── models.py
│   └── policy.py
├── deploy/
│   └── ops-agent.service
├── policy/
│   └── policy.yaml
├── scripts/
│   └── bootstrap_debian.sh
├── tests/
│   └── test_policy.py
├── .env.example
└── requirements.txt
```

---

## 2. 快速启动

```bash
cd debian-ops-agent
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
cp .env.example .env
```

设置审批令牌（写操作会校验）：

```bash
export OPS_AGENT_APPROVAL_TOKEN="请改成强随机字符串"
```

启动：

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8088
```

---

## 3. API 示例

### 3.1 健康检查

```bash
curl -s http://127.0.0.1:8088/health
```

### 3.2 查看命令白名单

```bash
curl -s http://127.0.0.1:8088/commands | jq
```

### 3.3 自然语言建议（不执行）

```bash
curl -s http://127.0.0.1:8088/suggest \
  -H 'Content-Type: application/json' \
  -d '{"task":"重启 nginx"}'
```

### 3.4 执行只读命令（无需审批令牌）

```bash
curl -s http://127.0.0.1:8088/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Actor: admin-demo' \
  -d '{"command_key":"service_status","args":["nginx"]}'
```

### 3.5 执行写命令（需要审批令牌）

```bash
curl -s http://127.0.0.1:8088/execute \
  -H 'Content-Type: application/json' \
  -H 'X-Actor: admin-demo' \
  -d '{
    "command_key":"apt_install",
    "args":["htop"],
    "approval_token":"请替换成你的令牌",
    "reason":"安装基础诊断工具"
  }'
```

---

## 4. 安全边界说明

1. 不支持任意命令执行，只能使用 `policy/policy.yaml` 中的 `command_key`。  
2. 参数有正则校验，防止拼接注入。  
3. 写操作默认要求 `approval_token`。  
4. 每次执行都会写入 `OPS_AGENT_AUDIT_LOG`。  

如果你要新增能力，建议流程是：

1. 在 `policy/policy.yaml` 新增命令定义  
2. 设定 `arg_pattern`、`min_args`、`max_args`  
3. 写操作打开 `require_approval: true`  
4. 先 `dry_run` 验证，再执行真实操作  

---

## 5. 生产部署建议

- 使用专用低权限用户（如 `opsbot`）运行服务  
- 用 `sudoers` 最小授权，而不是直接 root 常驻  
- 通过反向代理加鉴权（例如 Nginx + mTLS / token）  
- 审计日志落盘到 `/var/log/ops-agent/audit.log` 并接入集中日志系统  

`deploy/ops-agent.service` 已提供 systemd 模板，可按你的路径替换后启用。
