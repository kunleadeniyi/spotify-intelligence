with plays as (
    select * from {{ ref('int_deduped_play_events') }}
),

features as (
    select * from {{ ref('stg_track_audio_features') }}
)

select
    p.event_id,
    p.track_id,
    p.track_name,
    p.album_name,
    p.album_release_date,
    p.duration_ms,
    p.popularity,
    p.is_explicit,
    p.primary_artist_id,
    p.primary_artist_name,
    p.played_at,
    p.ingested_at,
    f.danceability,
    f.energy,
    f.valence,
    f.tempo,
    f.acousticness,
    f.instrumentalness,
    f.speechiness,
    f.loudness,
    f.is_synthetic
from plays p
left join features f on p.track_id = f.track_id
