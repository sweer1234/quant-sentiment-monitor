from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from quant_sentiment_monitor.settings import Settings
from quant_sentiment_monitor.store import QuantStore


def main() -> None:
    settings = Settings()
    store = QuantStore(settings)
    events = store.list_events()
    print(f"loaded_sources={len(store.sources)} loaded_events={len(events)}")
    for event in events[:5]:
        top = [impact.instrument for impact in event.impacts[:2]]
        print(
            f"{event.event_id} | {event.importance_level}({event.importance_score}) "
            f"| markets={','.join(event.impacted_markets)} | top={','.join(top)}"
        )


if __name__ == "__main__":
    main()
