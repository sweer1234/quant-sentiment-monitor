from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any
from urllib import request as urlrequest
import xml.etree.ElementTree as ET

from .store import QuantStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_url(url: str, *, retries: int = 2, timeout_sec: int = 8) -> tuple[str | None, str | None]:
    last_error = None
    for _ in range(retries + 1):
        try:
            req = urlrequest.Request(url, headers={"User-Agent": "qsm-collector/0.1"})
            with urlrequest.urlopen(req, timeout=timeout_sec) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="ignore"), None
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
    return None, last_error or "unknown_error"


def _parse_feed_items(raw_text: str, *, max_items: int = 3) -> list[dict[str, str]]:
    text = raw_text.strip()
    if not text:
        return []
    items: list[dict[str, str]] = []
    if text.startswith("<"):
        try:
            root = ET.fromstring(text)
            rss_items = root.findall(".//item")
            if rss_items:
                for item in rss_items[:max_items]:
                    title = (item.findtext("title") or "").strip()
                    desc = (item.findtext("description") or "").strip()
                    if title:
                        items.append({"title": title[:180], "content": (desc or title)[:600]})
                return items
            atom_entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
            for entry in atom_entries[:max_items]:
                title = (entry.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
                summary = (entry.findtext("{http://www.w3.org/2005/Atom}summary") or "").strip()
                if title:
                    items.append({"title": title[:180], "content": (summary or title)[:600]})
            if items:
                return items
        except ET.ParseError:
            pass

    title_match = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    clean_title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else "site_update"
    snippet = re.sub(r"<[^>]+>", " ", text)
    snippet = re.sub(r"\s+", " ", snippet).strip()
    items.append({"title": clean_title[:180], "content": (snippet or clean_title)[:600]})
    return items


def run_collection_once(store: QuantStore, *, limit: int = 20, retries: int = 2) -> dict[str, Any]:
    sources = store.list_polling_sources(limit=limit)
    pulled = 0
    accepted = 0
    deduplicated = 0
    failed = 0
    errors: list[dict[str, Any]] = []

    for source in sources:
        source_id = str(source.get("source_id"))
        url = str(source.get("url", "")).strip()
        if not url:
            failed += 1
            store.mark_source_poll_result(source_id, ok=False, fetched=0, error="missing_url")
            errors.append({"source_id": source_id, "error": "missing_url"})
            continue
        raw_text, fetch_error = _fetch_url(url, retries=retries)
        if fetch_error or raw_text is None:
            failed += 1
            store.mark_source_poll_result(source_id, ok=False, fetched=0, error=fetch_error)
            errors.append({"source_id": source_id, "error": fetch_error})
            continue

        pulled += 1
        fetched_items = _parse_feed_items(raw_text, max_items=3)
        fetched_count = 0
        for item in fetched_items:
            fetched_count += 1
            payload = {
                "source_id": source_id,
                "title": item["title"],
                "content": item["content"],
                "published_at": _now_iso(),
                "event_type": "source_polling_update",
            }
            result = store.ingest_event(payload, actor="collector")
            if result.get("deduplicated"):
                deduplicated += 1
            else:
                accepted += 1
        store.mark_source_poll_result(source_id, ok=True, fetched=fetched_count)

    return {
        "status": "ok",
        "sources_total": len(sources),
        "sources_pulled": pulled,
        "accepted": accepted,
        "deduplicated": deduplicated,
        "failed": failed,
        "errors": errors,
        "ran_at": _now_iso(),
    }

