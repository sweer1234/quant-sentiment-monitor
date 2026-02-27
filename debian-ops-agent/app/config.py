from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    policy_path: str = str(PROJECT_ROOT / "policy" / "policy.yaml")
    audit_log: str = str(PROJECT_ROOT / "ops-agent-audit.log")
    default_timeout: int = 60

    @property
    def policy_path_obj(self) -> Path:
        return Path(self.policy_path).expanduser().resolve()

    @property
    def audit_log_obj(self) -> Path:
        return Path(self.audit_log).expanduser().resolve()
