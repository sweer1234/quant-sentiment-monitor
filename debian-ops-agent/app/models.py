from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class ExecuteRequest(BaseModel):
    command_key: str = Field(..., description="命令白名单 key")
    args: List[str] = Field(default_factory=list, description="命令参数")
    timeout_sec: Optional[int] = Field(default=None, ge=1, le=1200)
    dry_run: bool = Field(default=False, description="仅返回计划，不执行")
    approval_token: Optional[str] = Field(default=None, description="写操作审批令牌")
    reason: Optional[str] = Field(default=None, description="执行原因，写入审计")


class ExecuteResponse(BaseModel):
    status: str
    command_key: str
    command: List[str]
    exit_code: Optional[int] = None
    duration_ms: int = 0
    stdout: str = ""
    stderr: str = ""


class CommandMeta(BaseModel):
    command_key: str
    description: str
    write: bool
    require_approval: bool
    allow_extra_args: bool
    min_args: int
    max_args: int
    arg_pattern: Optional[str]


class SuggestRequest(BaseModel):
    task: str = Field(..., min_length=2, description="自然语言任务描述")


class SuggestResponse(BaseModel):
    matched: bool
    command_key: Optional[str] = None
    args: List[str] = Field(default_factory=list)
    hint: str
