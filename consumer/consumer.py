import json
import os
import logging
import psycopg2
import signal
import threading

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from datetime import datetime, timezone

# Structured JSON logging
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            **{k: v for k, v in record.__dict__.items()
               if k not in logging.LogRecord.__dict__ and not k.startswith("_")},
        }
        if record.exc_info:
            log["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log)


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])


logger = logging.getLogger(__name__)


class EventConsumer:
    def __init__(self):
        config = {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": os.environ["KAFKA_CONSUMER_GROUP_ID"],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self._consumer = Consumer(config)
        self._conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["PG_SPOTIFY_DB"],
            user=os.environ["PG_SPOTIFY_DB_USER"],
            password=os.environ["PG_SPOTIFY_DB_USER_PASSWORD"],
        )
        self._conn.autocommit = False

    def _write_play_event(self, value: dict) -> None:
        sql = """
            INSERT INTO raw.play_events (event_id, track_id, played_at, raw_payload)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE
                SET track_id    = EXCLUDED.track_id,
                    played_at   = EXCLUDED.played_at,
                    raw_payload = EXCLUDED.raw_payload
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (
                value["event_id"],
                value["track_id"],
                value["played_at"],
                json.dumps(value["raw_payload"]),
            ))
        self._conn.commit()

    def _write_audio_features(self, value: dict) -> None:
        sql = """
            INSERT INTO raw.track_audio_features (track_id, fetched_at, features)
            VALUES (%s, %s, %s)
            ON CONFLICT (track_id) DO UPDATE
                SET fetched_at = EXCLUDED.fetched_at,
                    features   = EXCLUDED.features
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (
                value["track_id"],
                value["timestamp"],
                json.dumps(value["features"]),
            ))
        self._conn.commit()

    def _write_artist_genres(self, value: dict) -> None:
        sql = """
            INSERT INTO raw.artist_genres( artist_id, genres, fetched_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (artist_id) DO UPDATE
                SET fetched_at =  EXCLUDED.fetched_at,
                    genres = EXCLUDED.genres
        
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (
                    value["artist_id"],
                    value["genres"],
                    value["timestamp"]
            ))
        self._conn.commit()

    def _write_dlq(self, msg: Message, error: Exception) -> None:
        sql = """
            INSERT INTO raw.dlq (topic, "partition", "offset", error, raw_payload)
            VALUES (%s, %s, %s, %s, %s)
        """
        try:
            raw = msg.value().decode("utf-8") if msg.value() else None
            payload = json.loads(raw) if raw else None
            with self._conn.cursor() as cur:
                cur.execute(sql, (
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    str(error),
                    json.dumps(payload) if payload else None,
                ))
            self._conn.commit()
        except Exception:
            logger.exception("Failed to write to DLQ", extra={"topic": msg.topic()})
            self._conn.rollback()

    def process_messages(self, msg: Message) -> bool:
        key = msg.key().decode("utf-8") if msg.key() else None
        try:
            value = json.loads(msg.value().decode("utf-8"))
            logger.debug("Received message", extra={"key": key, "event_type": value.get("event_type")})

            if value["event_type"] == "play_event":
                self._write_play_event(value)
            elif value["event_type"] == "audio_features":
                self._write_audio_features(value)
            elif value["event_type"] == "artist_genres":
                self._write_artist_genres(value)
            else:
                logger.warning("Unknown event_type", extra={"event_type": value.get("event_type")})

            logger.info("Message processed", extra={"event_type": value.get("event_type"), "track_id": value.get("track_id")})
            return True

        except Exception as e:
            self._conn.rollback()
            logger.exception("Failed to process message, routing to DLQ", extra={"key": key})
            self._write_dlq(msg, e)
            return False


    def consume_loop(self, topics: list, stop_event: threading.Event) -> None:
        self._consumer.subscribe(topics)

        try:
            while not stop_event.is_set():
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError.__PARTITION_EOF:
                        logger.error(f"Reached end of {msg.topic()} [{msg.partition()}]")
                    else:
                        raise KafkaException(msg.error())
                else:
                    success = self.process_messages(msg)
                    if success:
                        self._consumer.commit(msg)
        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            self._conn.close()
            self._consumer.close()


def run():
    _configure_logging()
    stop_event = threading.Event()

    def _handle_shutdown(_signum, _frame) -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    topics = [os.environ["KAFKA_TOPIC_PLAY_EVENTS"], os.environ["KAFKA_TOPIC_AUDIO_FEATURES"], os.environ["KAFKA_TOPIC_ARTIST_GENRES"]]

    logger.info("Consumer started", extra={"topics": topics})
    EventConsumer().consume_loop(topics=topics, stop_event=stop_event)
    logger.info("Consumer stopped")


if __name__ == '__main__':
    run()