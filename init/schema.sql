-- Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS feast;


-- raw.play_events
CREATE TABLE IF NOT EXISTS raw.play_events (
    event_id        TEXT        NOT NULL,
    track_id        TEXT        NOT NULL,
    played_at       TIMESTAMPTZ NOT NULL,
    raw_payload     JSONB       NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT play_events_pkey PRIMARY KEY (event_id)
);


-- raw.track_audio_features
CREATE TABLE IF NOT EXISTS raw.track_audio_features (
    track_id        TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    features        JSONB       NOT NULL,
    CONSTRAINT track_audio_features_pkey PRIMARY KEY (track_id)
);


-- raw.dlq  (dead letter queue)
CREATE TABLE IF NOT EXISTS raw.dlq (
    id              BIGSERIAL   NOT NULL,
    topic           TEXT        NOT NULL,
    "partition"     INT,
    "offset"        BIGINT,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error           TEXT        NOT NULL,
    raw_payload     JSONB,
    CONSTRAINT dlq_pkey PRIMARY KEY (id)
);
