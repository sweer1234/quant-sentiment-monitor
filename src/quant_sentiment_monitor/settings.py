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
    event_calendar_rules: str = "configs/event_calendar_rules.yaml"
    webhook_delivery_rules: str = "configs/webhook_delivery_rules.yaml"
    state_path: str = "data/state.json"
    state_backend: str = "file"
    database_url: str = "sqlite:///data/qsm.db"
    state_sql_table: str = "qsm_state"
    queue_backend: str = "local"
    queue_redis_url: str = "redis://localhost:6379/0"
    collector_task_queue_key: str = "qsm:collector:tasks"
    model_backend: str = "local"
    model_service_url: str = "http://127.0.0.1:9000/infer"
    model_service_timeout_sec: int = 3
    notification_backend: str = "mock"
    smtp_host: str = "127.0.0.1"
    smtp_port: int = 25
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_use_tls: bool = False
    notify_email_from: str = "qsm-alerts@example.com"
    notify_email_to: str = ""
    im_webhook_url: str = ""
    im_webhook_timeout_sec: int = 5

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

    @property
    def event_calendar_rules_path(self) -> Path:
        return Path(self.event_calendar_rules)

    @property
    def webhook_delivery_rules_path(self) -> Path:
        return Path(self.webhook_delivery_rules)

    @property
    def state_path_obj(self) -> Path:
        return Path(self.state_path)
