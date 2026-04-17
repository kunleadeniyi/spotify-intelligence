import os
import pytest
import uuid
from confluent_kafka import Producer, Consumer

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
