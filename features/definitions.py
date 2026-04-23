from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import Bool, Float64, Int64

track = Entity(name="track", join_keys=["track_id"])
user = Entity(name="user", join_keys=["user_id"])

track_audio_features_source = PostgreSQLSource(
    name="track_audio_features_source",
    query="""
        SELECT track_id, fetched_at, danceability, energy, valence, tempo,
               acousticness, instrumentalness, speechiness, loudness, is_synthetic
        FROM staging.stg_track_audio_features
    """,
    timestamp_field="fetched_at",
)

user_track_stats_source = PostgreSQLSource(
    name="user_track_stats_source",
    query="""
        SELECT track_id, last_played_at, play_count, days_since_last_play, recency_score
        FROM marts.mart_track_stats
    """,
    timestamp_field="last_played_at",
)

user_taste_profile_source = PostgreSQLSource(
    name="user_taste_profile_source",
    query="""
        SELECT user_id, computed_at,
               avg_danceability, avg_energy, avg_valence, avg_tempo,
               avg_acousticness, avg_instrumentalness, avg_speechiness, avg_loudness,
               var_danceability, var_energy, var_valence, var_tempo,
               var_acousticness, var_instrumentalness, var_speechiness, var_loudness
        FROM marts.mart_user_taste_profile
    """,
    timestamp_field="computed_at",
)

track_audio_features_fv = FeatureView(
    name="track_audio_features_fv",
    entities=[track],
    ttl=timedelta(days=90), # how old a feature row can be before Feast considers it stale
    schema=[
        Field(name="danceability", dtype=Float64),
        Field(name="energy", dtype=Float64),
        Field(name="valence", dtype=Float64),
        Field(name="tempo", dtype=Float64),
        Field(name="acousticness", dtype=Float64),
        Field(name="instrumentalness", dtype=Float64),
        Field(name="speechiness", dtype=Float64),
        Field(name="loudness", dtype=Float64),
        Field(name="is_synthetic", dtype=Bool),
    ],
    source=track_audio_features_source,
)

user_track_stats_fv = FeatureView(
    name="user_track_stats_fv",
    entities=[track],
    ttl=timedelta(days=30),
    schema=[
        Field(name="play_count", dtype=Int64),
        Field(name="days_since_last_play", dtype=Float64),
        Field(name="recency_score", dtype=Float64),
    ],
    source=user_track_stats_source,
)

user_taste_profile_fv = FeatureView(
    name="user_taste_profile_fv",
    entities=[user],
    ttl=timedelta(days=7),
    schema=[
        Field(name="avg_danceability", dtype=Float64),
        Field(name="avg_energy", dtype=Float64),
        Field(name="avg_valence", dtype=Float64),
        Field(name="avg_tempo", dtype=Float64),
        Field(name="avg_acousticness", dtype=Float64),
        Field(name="avg_instrumentalness", dtype=Float64),
        Field(name="avg_speechiness", dtype=Float64),
        Field(name="avg_loudness", dtype=Float64),
        Field(name="var_danceability", dtype=Float64),
        Field(name="var_energy", dtype=Float64),
        Field(name="var_valence", dtype=Float64),
        Field(name="var_tempo", dtype=Float64),
        Field(name="var_acousticness", dtype=Float64),
        Field(name="var_instrumentalness", dtype=Float64),
        Field(name="var_speechiness", dtype=Float64),
        Field(name="var_loudness", dtype=Float64),
    ],
    source=user_taste_profile_source,
)
