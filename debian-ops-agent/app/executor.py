from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time
from typing import List

from .models import CommandMeta, ExecuteRequest, ExecuteResponse
from .policy import Policy, PolicyError


class ApprovalError(PermissionError):
    """Raised when approval token is missing or invalid."""


def _clip(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    clipped = text[:max_chars]
    return f"{clipped}\n...[truncated {len(text) - max_chars} chars]"


class CommandExecutor:
    def __init__(self, policy: Policy, audit_log: Path, default_timeout: int) -> None:
        self.policy = policy
        self.audit_log = audit_log
        self.default_timeout = default_timeout
        self.audit_log.parent.mkdir(parents=True, exist_ok=True)

    def list_commands(self) -> List[CommandMeta]:
        return [
            CommandMeta(
                command_key=key,
                description=spec.description,
                write=spec.write,
                require_approval=spec.require_approval,
                allow_extra_args=spec.allow_extra_args,
                min_args=spec.min_args,
                max_args=spec.max_args,
                arg_pattern=spec.arg_pattern,
            )
            for key, spec in sorted(self.policy.commands.items())
        ]

    def execute(self, request: ExecuteRequest, actor: str = "unknown") -> ExecuteResponse:
        spec = self.policy.get_command(request.command_key)
        spec.validate_args(request.command_key, request.args)
        self._check_approval(spec.write, spec.require_approval, request.approval_token)

        timeout_sec = request.timeout_sec or self.default_timeout
        if timeout_sec > self.policy.defaults.max_timeout_sec:
            raise PolicyError(
                f"timeout_sec {timeout_sec} exceeds policy limit {self.policy.defaults.max_timeout_sec}"
            )

        command = spec.command + request.args
        if request.dry_run:
            response = ExecuteResponse(
                status="dry_run",
                command_key=request.command_key,
                command=command,
                exit_code=None,
                duration_ms=0,
                stdout="",
                stderr="",
            )
            self._audit(actor=actor, request=request, response=response, write=spec.write)
            return response

        started = time.monotonic()
        try:
            completed = subprocess.run(  # noqa: S603
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
            duration_ms = int((time.monotonic() - started) * 1000)
            response = ExecuteResponse(
                status="ok" if completed.returncode == 0 else "error",
                command_key=request.command_key,
                command=command,
                exit_code=completed.returncode,
                duration_ms=duration_ms,
                stdout=_clip(completed.stdout, self.policy.defaults.max_output_chars),
                stderr=_clip(completed.stderr, self.policy.defaults.max_output_chars),
            )
            self._audit(actor=actor, request=request, response=response, write=spec.write)
            return response
        except subprocess.TimeoutExpired as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            response = ExecuteResponse(
                status="timeout",
                command_key=request.command_key,
                command=command,
                exit_code=None,
                duration_ms=duration_ms,
                stdout=_clip(exc.stdout or "", self.policy.defaults.max_output_chars),
                stderr=_clip(exc.stderr or "", self.policy.defaults.max_output_chars),
            )
            self._audit(actor=actor, request=request, response=response, write=spec.write)
            return response
        except FileNotFoundError as exc:
            raise PolicyError(f"Command binary not found: {exc}") from exc

    def _check_approval(
        self, is_write_command: bool, requires_approval: bool, approval_token: str | None
    ) -> None:
        if not is_write_command and not requires_approval:
            return
        if not requires_approval:
            return

        expected_token = os.getenv(self.policy.approval.token_env)
        if not expected_token:
            raise ApprovalError(
                f"Server missing approval token env '{self.policy.approval.token_env}'"
            )
        if approval_token != expected_token:
            raise ApprovalError("Invalid approval token")

    def _audit(
        self, actor: str, request: ExecuteRequest, response: ExecuteResponse, write: bool
    ) -> None:
        log_event = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "actor": actor,
            "command_key": request.command_key,
            "args": request.args,
            "reason": request.reason,
            "dry_run": request.dry_run,
            "write": write,
            "status": response.status,
            "exit_code": response.exit_code,
            "duration_ms": response.duration_ms,
            "command": response.command,
        }
        with self.audit_log.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(log_event, ensure_ascii=False) + "\n")
