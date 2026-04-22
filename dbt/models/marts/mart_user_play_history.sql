select
    event_id,
    track_id,
    track_name,
    album_name,
    album_release_date,
    duration_ms,
    popularity,
    is_explicit,
    primary_artist_id,
    primary_artist_name,
    played_at,
    danceability,
    energy,
    valence,
    tempo,
    acousticness,
    instrumentalness,
    speechiness,
    loudness,
    is_synthetic
from {{ ref('int_track_enriched') }}
order by played_at desc
