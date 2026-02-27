from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Dict, Optional, Pattern

import yaml


class PolicyError(ValueError):
    """Raised when policy is invalid."""


@dataclass
class PolicyDefaults:
    max_timeout_sec: int = 120
    max_output_chars: int = 12000


@dataclass
class ApprovalConfig:
    token_env: str = "OPS_AGENT_APPROVAL_TOKEN"


@dataclass
class CommandSpec:
    description: str
    command: list[str]
    write: bool = False
    require_approval: bool = False
    allow_extra_args: bool = False
    min_args: int = 0
    max_args: int = 0
    arg_pattern: Optional[str] = None
    _compiled_pattern: Optional[Pattern[str]] = field(default=None, init=False, repr=False)

    def validate(self, command_key: str) -> None:
        if not self.description:
            raise PolicyError(f"Command '{command_key}' missing description")
        if not self.command or not all(self.command):
            raise PolicyError(f"Command '{command_key}' has invalid command list")
        if self.min_args < 0 or self.max_args < 0:
            raise PolicyError(f"Command '{command_key}' has negative arg bounds")
        if self.max_args and self.max_args < self.min_args:
            raise PolicyError(f"Command '{command_key}' max_args is less than min_args")
        if not self.allow_extra_args and (self.min_args != 0 or self.max_args != 0):
            raise PolicyError(
                f"Command '{command_key}' has arg limits but allow_extra_args is false"
            )
        if self.arg_pattern:
            try:
                self._compiled_pattern = re.compile(self.arg_pattern)
            except re.error as exc:
                raise PolicyError(f"Command '{command_key}' has invalid arg_pattern: {exc}") from exc

    def validate_args(self, command_key: str, args: list[str]) -> None:
        if not self.allow_extra_args and args:
            raise PolicyError(f"Command '{command_key}' does not allow args")
        if self.allow_extra_args:
            arg_count = len(args)
            if arg_count < self.min_args:
                raise PolicyError(
                    f"Command '{command_key}' requires at least {self.min_args} args"
                )
            if self.max_args and arg_count > self.max_args:
                raise PolicyError(
                    f"Command '{command_key}' allows at most {self.max_args} args"
                )
            if self._compiled_pattern:
                for arg in args:
                    if not self._compiled_pattern.fullmatch(arg):
                        raise PolicyError(
                            f"Command '{command_key}' arg '{arg}' violates arg_pattern"
                        )


@dataclass
class Policy:
    version: int
    defaults: PolicyDefaults
    approval: ApprovalConfig
    commands: Dict[str, CommandSpec]

    def get_command(self, command_key: str) -> CommandSpec:
        command = self.commands.get(command_key)
        if not command:
            raise PolicyError(f"Unknown command_key '{command_key}'")
        return command


def load_policy(path: Path) -> Policy:
    if not path.exists():
        raise PolicyError(f"Policy file not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise PolicyError("Policy file must be a mapping")

    version = raw.get("version")
    if version != 1:
        raise PolicyError("Only policy version 1 is supported")

    raw_defaults = raw.get("defaults", {}) or {}
    defaults = PolicyDefaults(
        max_timeout_sec=int(raw_defaults.get("max_timeout_sec", 120)),
        max_output_chars=int(raw_defaults.get("max_output_chars", 12000)),
    )
    if defaults.max_timeout_sec <= 0:
        raise PolicyError("defaults.max_timeout_sec must be > 0")
    if defaults.max_output_chars <= 0:
        raise PolicyError("defaults.max_output_chars must be > 0")

    raw_approval = raw.get("approval", {}) or {}
    approval = ApprovalConfig(token_env=str(raw_approval.get("token_env", "OPS_AGENT_APPROVAL_TOKEN")))

    raw_commands = raw.get("commands")
    if not isinstance(raw_commands, dict) or not raw_commands:
        raise PolicyError("commands must be a non-empty mapping")

    commands: Dict[str, CommandSpec] = {}
    for key, value in raw_commands.items():
        if not isinstance(value, dict):
            raise PolicyError(f"Command '{key}' must be a mapping")
        spec = CommandSpec(
            description=str(value.get("description", "")),
            command=[str(item) for item in value.get("command", [])],
            write=bool(value.get("write", False)),
            require_approval=bool(value.get("require_approval", False)),
            allow_extra_args=bool(value.get("allow_extra_args", False)),
            min_args=int(value.get("min_args", 0)),
            max_args=int(value.get("max_args", 0)),
            arg_pattern=value.get("arg_pattern"),
        )
        spec.validate(key)
        commands[key] = spec

    return Policy(version=version, defaults=defaults, approval=approval, commands=commands)
