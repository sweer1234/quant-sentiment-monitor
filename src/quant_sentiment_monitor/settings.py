from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "quant-sentiment-monitor"
    env: str = "dev"
    public_api_token: str = "dev-public-token"
    source_registry_default: str = "configs/sources_registry.default.yaml"
    source_registry_override: str = "configs/sources_registry.override.yaml"
    source_weight_rules: str = "configs/source_weight_rules.yaml"
    manual_input_rules: str = "configs/manual_input_rules.yaml"

    model_config = SettingsConfigDict(env_prefix="QSM_", env_file=".env", extra="ignore")

    @property
    def source_registry_default_path(self) -> Path:
        return Path(self.source_registry_default)

    @property
    def source_registry_override_path(self) -> Path:
        return Path(self.source_registry_override)

    @property
    def source_weight_rules_path(self) -> Path:
        return Path(self.source_weight_rules)

    @property
    def manual_input_rules_path(self) -> Path:
        return Path(self.manual_input_rules)
