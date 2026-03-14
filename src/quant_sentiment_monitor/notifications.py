from __future__ import annotations

import json
import smtplib
from typing import Any, Protocol
from urllib import request as urlrequest

from .settings import Settings


class NotificationDispatcher(Protocol):
    def deliver(self, message: dict[str, Any]) -> tuple[bool, str]:
        ...


class MockNotificationDispatcher:
    def deliver(self, message: dict[str, Any]) -> tuple[bool, str]:
        _ = message
        return True, "mock_delivered"


class RealNotificationDispatcher:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _send_email(self, message: dict[str, Any]) -> tuple[bool, str]:
        recipients = [item.strip() for item in self.settings.notify_email_to.split(",") if item.strip()]
        if not recipients:
            return False, "missing_notify_email_to"
        subject = f"[QSM] {message.get('importance_level', 'P2')} {message.get('title', 'alert')}"
        body = (
            f"AlertID: {message.get('alert_id')}\n"
            f"EventID: {message.get('event_id')}\n"
            f"Title: {message.get('title')}\n"
            f"Summary: {message.get('summary')}\n"
        )
        mime = (
            f"From: {self.settings.notify_email_from}\r\n"
            f"To: {', '.join(recipients)}\r\n"
            f"Subject: {subject}\r\n"
            "\r\n"
            f"{body}"
        )
        try:
            with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port, timeout=10) as server:
                if self.settings.smtp_use_tls:
                    server.starttls()
                if self.settings.smtp_user:
                    server.login(self.settings.smtp_user, self.settings.smtp_password)
                server.sendmail(self.settings.notify_email_from, recipients, mime)
            return True, "email_sent"
        except Exception as exc:  # noqa: BLE001
            return False, f"smtp_error:{exc}"

    def _send_im(self, message: dict[str, Any]) -> tuple[bool, str]:
        if not self.settings.im_webhook_url:
            return False, "missing_im_webhook_url"
        payload = {
            "msg_type": "text",
            "content": {
                "text": (
                    f"[QSM]{message.get('importance_level')} "
                    f"{message.get('title')} "
                    f"(alert={message.get('alert_id')}, event={message.get('event_id')})"
                )
            },
        }
        req = urlrequest.Request(
            self.settings.im_webhook_url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json", "User-Agent": "qsm-notifier/0.1"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=self.settings.im_webhook_timeout_sec) as resp:
                code = getattr(resp, "status", 200)
            if int(code) >= 300:
                return False, f"im_http_{code}"
            return True, "im_sent"
        except Exception as exc:  # noqa: BLE001
            return False, f"im_error:{exc}"

    def deliver(self, message: dict[str, Any]) -> tuple[bool, str]:
        channel = str(message.get("channel", "app"))
        if channel == "app":
            return True, "app_inbox_delivered"
        if channel == "email":
            return self._send_email(message)
        if channel == "im":
            return self._send_im(message)
        return False, "unsupported_channel"


def build_notification_dispatcher(settings: Settings) -> NotificationDispatcher:
    backend = settings.notification_backend.lower().strip()
    if backend == "real":
        return RealNotificationDispatcher(settings=settings)
    return MockNotificationDispatcher()

