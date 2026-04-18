import pytest
import json
import time
import uuid
from consumer.consumer import EventConsumer


# marker so it only runs with make test-integration, not make test-unit
@pytest.mark.integration
def test_consume_play_event(kafka_producer, kafka_consumer, pg_conn):
    event_id = f"test-event-{uuid.uuid4()}"
    MOCK_RECENTLY_PLAYED = {
        "track_id": "track_abc123",
        "event_id": event_id,
        "event_type": "play_event",
        "played_at": "2024-01-01T12:00:00Z",
        "raw_payload": {
            "track": {
                "id": "track_abc123",
                "name": "Blinding Lights",
                "artists": [{"id": "artist_1", "name": "The Weeknd"}],
            },
            "played_at": "2024-01-01T12:00:00Z",
        }
    }
    
    topic = "spotify.play.events"
    kafka_producer.produce(topic, value=json.dumps(MOCK_RECENTLY_PLAYED).encode('utf-8'))
    kafka_producer.flush()

    # no need for full loop
    kafka_consumer.subscribe([topic])
    deadline = time.time() + 10
    msg = None

    while time.time() < deadline:
        m = kafka_consumer.poll(timeout=1.0)
        # keep polling until the sent message (MOCK_AUDIO_FEATURES) is received 
        if m is not None and m.error() is None:
            parsed = json.loads(m.value().decode("utf-8"))
            if parsed.get("event_id") == event_id:
                msg = m
                break

    assert msg is not None
    assert msg.error() is None

    consumer = EventConsumer()
    consumer.process_messages(msg)
    consumer._conn.close()

    # check the database for entry using event_id
    sql = """
    SELECT * FROM raw.play_events
    WHERE event_id = %s
    """

    cur = pg_conn.cursor()
    cur.execute(sql, (event_id,))
    row = cur.fetchone()

    assert row is not None
    assert row["event_id"] == event_id
    



@pytest.mark.integration
def test_consume_audio_features(kafka_producer, kafka_consumer, pg_conn):
    track_id = f"test-track-{uuid.uuid4()}"
    MOCK_AUDIO_FEATURES = {
        "event_type": "audio_features",
        "track_id": track_id,
        "timestamp": "2024-01-01T12:00:00Z",
        "features": {
            "danceability": 0.514,
            "energy": 0.73,
            "tempo": 171.005,
            "valence": 0.334,
            "acousticness": 0.00146,
            "instrumentalness": 0.0000224,
            "speechiness": 0.0598,
            "loudness": -5.934,
        },
    }

    topic = "spotify.audio.features"
    kafka_producer.produce(topic, value=json.dumps(MOCK_AUDIO_FEATURES).encode("utf-8"))
    kafka_producer.flush()

    kafka_consumer.subscribe([topic])
    deadline = time.time() + 10
    msg = None
    while time.time() < deadline:
        m = kafka_consumer.poll(timeout=1.0)
        # keep polling until the sent message (MOCK_AUDIO_FEATURES) is received 
        if m is not None and m.error() is None:
            parsed = json.loads(m.value().decode("utf-8"))
            if parsed.get("track_id") == track_id:
                msg = m
                break

    assert msg is not None
    assert msg.error() is None

    consumer = EventConsumer()
    consumer.process_messages(msg)
    consumer._conn.close()

    sql = "SELECT * FROM raw.track_audio_features WHERE track_id = %s"
    cur = pg_conn.cursor()
    cur.execute(sql, (track_id,))
    row = cur.fetchone()

    assert row is not None
    assert row["track_id"] == track_id
