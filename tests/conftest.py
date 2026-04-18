import os
import pytest
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer, Consumer
from dotenv import load_dotenv
load_dotenv()

@pytest.fixture(scope="session")
def kafka_bootstrap_servers():
    # Fallback to localhost:9092 if the variable isn't set
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

@pytest.fixture
def kafka_producer(kafka_bootstrap_servers):
    conf = {'bootstrap.servers': kafka_bootstrap_servers}
    return Producer(conf)

@pytest.fixture
def kafka_consumer(kafka_bootstrap_servers):
    conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': f'test-group-{uuid.uuid4()}', # groud id should be unique every run
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

@pytest.fixture
def pg_conn():
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["PG_SPOTIFY_DB"],
        user=os.environ["PG_SPOTIFY_DB_USER"],
        password=os.environ["PG_SPOTIFY_DB_USER_PASSWORD"],
        cursor_factory=RealDictCursor
        )
    conn.autocommit = False
    yield conn
    with conn.cursor() as cur:
        cur.execute("DELETE FROM raw.play_events WHERE event_id LIKE 'test-event-%'")
        cur.execute("DELETE FROM raw.track_audio_features WHERE track_id LIKE 'test-track-%'")
    conn.commit()
    conn.close()