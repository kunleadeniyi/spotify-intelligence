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

-- raw.artist_genres
CREATE TABLE IF NOT EXISTS raw.artist_genres (
    artist_id   TEXT NOT NULL,
    genres      TEXT[] NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT artist_genres_pkey PRIMARY KEY (artist_id)
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


-- Grants (must run after tables are created)
GRANT USAGE ON SCHEMA raw TO "spotify-sa";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO "spotify-sa";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw TO "spotify-sa";

GRANT USAGE ON SCHEMA staging TO "spotify-sa";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO "spotify-sa";
GRANT CREATE ON SCHEMA staging TO "spotify-sa";

GRANT USAGE ON SCHEMA marts TO "spotify-sa";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO "spotify-sa";
GRANT CREATE ON SCHEMA marts TO "spotify-sa";

-- feast-sa: read-only access to feature source schemas, full access to feast schema
GRANT USAGE ON SCHEMA raw TO "feast-sa";
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO "feast-sa";

GRANT USAGE ON SCHEMA staging TO "feast-sa";
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO "feast-sa";

GRANT USAGE ON SCHEMA marts TO "feast-sa";
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO "feast-sa";

GRANT ALL PRIVILEGES ON SCHEMA feast TO "feast-sa";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA feast TO "feast-sa";
GRANT CREATE ON SCHEMA feast TO "feast-sa";

ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO "feast-sa";
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO "feast-sa";
