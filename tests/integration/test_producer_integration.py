import json
import time

import pytest

@pytest.mark.integration # marker so it only runs with make test-integration, not make test-unit
def test_push_to_topic(kafka_producer, kafka_consumer):
    topic = "spotify.play.events"

    MOCK_RECENTLY_PLAYED = {
    "items": [
        {
            "track": {
                "id": "track_abc123",
                "name": "Blinding Lights",
                "artists": [{"id": "artist_1", "name": "The Weeknd"}],
            },
            "played_at": "2024-01-01T12:00:00Z",
        },
        {
            "track": {
                "id": "track_def456",
                "name": "Save Your Tears",
                "artists": [{"id": "artist_1", "name": "The Weeknd"}],
            },
            "played_at": "2024-01-01T11:45:00Z",
        },
        ]
    }
    
    kafka_producer.produce(topic, value=json.dumps(MOCK_RECENTLY_PLAYED).encode('utf-8'))
    kafka_producer.flush()

    kafka_consumer.subscribe([topic])

    deadline = time.time() + 10
    msg = None
    while time.time() < deadline:
        m = kafka_consumer.poll(timeout=1.0)
        if m is not None:
            msg = m
            break

    assert msg is not None
    assert msg.error() is None
    assert json.loads(msg.value().decode('utf-8')) == MOCK_RECENTLY_PLAYED


@pytest.mark.integration
def test_poll_returns_none_on_empty_topic(kafka_consumer):
    """
    Polling messages from empty topic should always return None. 
    Topic used in this test: spotify.audio.features
        Should be empty as other test push to spotify.play.events topic instead
    """
    kafka_consumer.subscribe(["spotify.audio.features"])

    deadline = time.time() + 5
    msg = None
    while time.time() < deadline:
        m = kafka_consumer.poll(timeout=1.0)
        if m is not None:
            msg = m
            break

    assert msg is None
