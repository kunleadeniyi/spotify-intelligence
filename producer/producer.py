import json
import logging
import os
import signal
import time
from datetime import datetime, timezone, timedelta

from confluent_kafka import Producer

from producer.spotify_client import SpotifyClient


# Structured JSON logging
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            **{k: v for k, v in record.__dict__.items()
               if k not in logging.LogRecord.__dict__ and not k.startswith("_")},
        })


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])


logger = logging.getLogger(__name__)

# Kafka producer wrapper
class EventProducer:
    def __init__(self) -> None:
        config = {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 3,
            "retry.backoff.ms": 100,
            "delivery.timeout.ms": 120000,
        }
        self._producer = Producer(config)
        self._failed_messages: list[dict] = []

    def _on_delivery(self, err, msg) -> None:
        if err:
            logger.error(
                "Delivery failed",
                extra={"topic": msg.topic(), "error": str(err)},
            )
            self._failed_messages.append({
                "topic": msg.topic(),
                "key": msg.key(),
                "value": msg.value(),
                "error": str(err),
            })
        else:
            logger.debug(
                "Message delivered",
                extra={"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()},
            )

    def send(self, topic: str, key: str, value: dict) -> None:
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(value).encode("utf-8"),
            callback=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> list[dict]:
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("Messages still in queue after flush", extra={"remaining": remaining})
        return self._failed_messages


# In-memory deduplication (sliding window)
class _SeenEventsWindow:
    def __init__(self, window_seconds: int = 3600) -> None:
        self._seen: dict[str, datetime] = {}
        self._window = timedelta(seconds=window_seconds)

    def is_new(self, event_id: str, event_time: datetime) -> bool:
        self._prune(event_time)
        return event_id not in self._seen

    def mark_seen(self, event_id: str, event_time: datetime) -> None:
        self._seen[event_id] = event_time

    def _prune(self, now: datetime) -> None:
        cutoff = now - self._window
        stale = [eid for eid, t in self._seen.items() if t < cutoff]
        for eid in stale:
            del self._seen[eid]


# Polling loop
POLL_INTERVAL_SECONDS = 30
_running = True


def _handle_shutdown(_signum, _frame) -> None:
    global _running
    logger.info("Shutdown signal received")
    _running = False


def run() -> None:
    _configure_logging()
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    spotify = SpotifyClient()
    kafka = EventProducer()
    seen = _SeenEventsWindow(window_seconds=3600)

    topic_play_events = os.environ["KAFKA_TOPIC_PLAY_EVENTS"]
    topic_audio_features = os.environ["KAFKA_TOPIC_AUDIO_FEATURES"]

    logger.info("Producer started", extra={"poll_interval": POLL_INTERVAL_SECONDS})

    while _running:
        try:
            new_track_ids: list[str] = []

            # recently_played is the primary source — has reliable played_at for deduplication
            recent = spotify.get_recently_played(limit=50)
            for item in recent:
                track = item["track"]
                played_at_str = item["played_at"]
                played_at = datetime.fromisoformat(played_at_str.replace("Z", "+00:00"))
                event_id = f"{track['id']}_{played_at_str}"

                if not seen.is_new(event_id, played_at):
                    continue

                kafka.send(
                    topic=topic_play_events,
                    key=track["id"],
                    value={
                        "event_type": "play_event",
                        "event_id": event_id,
                        "track_id": track["id"],
                        "played_at": played_at_str,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "raw_payload": item,
                    },
                )
                seen.mark_seen(event_id, played_at)
                new_track_ids.append(track["id"])
                logger.info(
                    "Published play event",
                    extra={"event_type": "play_event", "track_id": track["id"], "timestamp": played_at_str},
                )

            # Fetch and publish audio features only for newly seen tracks
            if new_track_ids:
                unique_ids = list(dict.fromkeys(new_track_ids))
                features = spotify.get_audio_features(unique_ids)
                for feature in features:
                    kafka.send(
                        topic=topic_audio_features,
                        key=feature["id"],
                        value={
                            "event_type": "audio_features",
                            "track_id": feature["id"],
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "features": feature,
                        },
                    )
                    logger.info(
                        "Published audio features",
                        extra={"event_type": "audio_features", "track_id": feature["id"], "timestamp": datetime.now(timezone.utc).isoformat()},
                    )

            kafka.flush()

        except Exception:
            logger.exception("Error during polling cycle")

        time.sleep(POLL_INTERVAL_SECONDS)

    logger.info("Producer stopped")


if __name__ == "__main__":
    run()
