from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Tuple

from fastapi import FastAPI, Header, HTTPException

from .config import Settings
from .executor import ApprovalError, CommandExecutor
from .models import ExecuteRequest, ExecuteResponse, SuggestRequest, SuggestResponse
from .policy import PolicyError, load_policy


settings = Settings()
policy = load_policy(settings.policy_path_obj)
executor = CommandExecutor(
    policy=policy,
    audit_log=settings.audit_log_obj,
    default_timeout=settings.default_timeout,
)

app = FastAPI(
    title="Debian Ops Agent",
    version="0.1.0",
    description="安全白名单版 Debian 运维执行代理",
)


def _extract_service(task: str) -> str | None:
    patterns = [
        r"(?:重启|restart)\s*([a-zA-Z0-9_.@-]+)",
        r"(?:启动|start)\s*([a-zA-Z0-9_.@-]+)",
        r"(?:停止|stop)\s*([a-zA-Z0-9_.@-]+)",
        r"(?:查看|看|check)\s*([a-zA-Z0-9_.@-]+)\s*(?:状态|status)",
        r"(?:日志|journal)\s*([a-zA-Z0-9_.@-]+)",
    ]
    for pattern in patterns:
        matched = re.search(pattern, task, flags=re.IGNORECASE)
        if matched:
            return matched.group(1)
    return None


def _extract_package(task: str) -> str | None:
    patterns = [
        r"(?:安装|install)\s*([a-zA-Z0-9+._-]+)",
        r"(?:卸载|remove)\s*([a-zA-Z0-9+._-]+)",
    ]
    for pattern in patterns:
        matched = re.search(pattern, task, flags=re.IGNORECASE)
        if matched:
            return matched.group(1)
    return None


def route_task(task: str) -> Tuple[str | None, list[str], str]:
    normalized = task.strip().lower()
    if not normalized:
        return None, [], "任务为空"

    if any(keyword in normalized for keyword in ["更新软件源", "apt update", "更新源"]):
        return "apt_update", [], "建议执行 apt_update"

    if any(keyword in normalized for keyword in ["安装", "install"]):
        package = _extract_package(task)
        if package:
            return "apt_install", [package], "建议安装软件包"
        return None, [], "识别到安装意图，但没找到软件包名"

    if any(keyword in normalized for keyword in ["卸载", "remove"]):
        package = _extract_package(task)
        if package:
            return "apt_remove", [package], "建议卸载软件包"
        return None, [], "识别到卸载意图，但没找到软件包名"

    if any(keyword in normalized for keyword in ["重启", "restart"]):
        service = _extract_service(task)
        if service:
            return "service_restart", [service], "建议重启服务"
        return None, [], "识别到重启意图，但没找到服务名"

    if any(keyword in normalized for keyword in ["启动", "start"]):
        service = _extract_service(task)
        if service:
            return "service_start", [service], "建议启动服务"
        return None, [], "识别到启动意图，但没找到服务名"

    if any(keyword in normalized for keyword in ["停止", "stop"]):
        service = _extract_service(task)
        if service:
            return "service_stop", [service], "建议停止服务"
        return None, [], "识别到停止意图，但没找到服务名"

    if any(keyword in normalized for keyword in ["状态", "status"]):
        service = _extract_service(task)
        if service:
            return "service_status", [service], "建议查看服务状态"
        return None, [], "识别到状态查询意图，但没找到服务名"

    if any(keyword in normalized for keyword in ["日志", "journal"]):
        service = _extract_service(task)
        if service:
            return "journal_service", [service], "建议查看服务日志"
        return None, [], "识别到日志查询意图，但没找到服务名"

    if any(keyword in normalized for keyword in ["端口", "监听", "ss"]):
        return "list_listening_ports", [], "建议查看监听端口"

    if any(keyword in normalized for keyword in ["磁盘", "df"]):
        return "disk_usage", [], "建议查看磁盘使用"

    if any(keyword in normalized for keyword in ["内存", "free"]):
        return "memory_usage", [], "建议查看内存使用"

    if any(keyword in normalized for keyword in ["系统版本", "os", "发行版"]):
        return "os_release", [], "建议查看系统版本"

    matched_ping = re.search(r"(?:ping|连通性)\s*([a-zA-Z0-9.-]+)", task, flags=re.IGNORECASE)
    if matched_ping:
        return "ping_host", [matched_ping.group(1)], "建议执行 ping 检测"

    matched_url = re.search(
        r"(https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+)",
        task,
        flags=re.IGNORECASE,
    )
    if matched_url:
        return "curl_head", [matched_url.group(1)], "建议执行 HTTP 连通性检查"

    return None, [], "未匹配到内置意图，请直接使用 command_key 调用 /execute"


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "policy_version": policy.version,
        "commands": len(policy.commands),
    }


@app.get("/commands")
def list_commands() -> list[dict]:
    return [item.model_dump() for item in executor.list_commands()]


@app.post("/suggest", response_model=SuggestResponse)
def suggest(request: SuggestRequest) -> SuggestResponse:
    command_key, args, hint = route_task(request.task)
    if command_key is None:
        return SuggestResponse(matched=False, command_key=None, args=[], hint=hint)
    return SuggestResponse(matched=True, command_key=command_key, args=args, hint=hint)


@app.post("/execute", response_model=ExecuteResponse)
def execute(
    request: ExecuteRequest,
    x_actor: str = Header(default="unknown", alias="X-Actor"),
) -> ExecuteResponse:
    try:
        return executor.execute(request, actor=x_actor)
    except PolicyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ApprovalError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Unexpected error: {exc}") from exc
