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
    multi_user_rules: str = "configs/multi_user_rules.yaml"
    topic_taxonomy: str = "configs/topic_taxonomy.yaml"
    investment_event_catalog: str = "configs/investment_event_catalog.yaml"
    alert_governance_rules: str = "configs/alert_governance_rules.yaml"
    source_compliance_registry: str = "configs/source_compliance_registry.yaml"
    portfolio_impact_rules: str = "configs/portfolio_impact_rules.yaml"
    feedback_learning_rules: str = "configs/feedback_learning_rules.yaml"
    billing_sla_rules: str = "configs/billing_sla_rules.yaml"

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

    @property
    def multi_user_rules_path(self) -> Path:
        return Path(self.multi_user_rules)

    @property
    def topic_taxonomy_path(self) -> Path:
        return Path(self.topic_taxonomy)

    @property
    def investment_event_catalog_path(self) -> Path:
        return Path(self.investment_event_catalog)

    @property
    def alert_governance_rules_path(self) -> Path:
        return Path(self.alert_governance_rules)

    @property
    def source_compliance_registry_path(self) -> Path:
        return Path(self.source_compliance_registry)

    @property
    def portfolio_impact_rules_path(self) -> Path:
        return Path(self.portfolio_impact_rules)

    @property
    def feedback_learning_rules_path(self) -> Path:
        return Path(self.feedback_learning_rules)

    @property
    def billing_sla_rules_path(self) -> Path:
        return Path(self.billing_sla_rules)
