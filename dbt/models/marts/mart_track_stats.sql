with play_history as (
    select
        track_id,
        track_name,
        primary_artist_id,
        primary_artist_name,
        played_at
    from {{ ref('mart_user_play_history') }}
),

stats as (
    select
        track_id,
        max(track_name)                                          as track_name,
        max(primary_artist_id)                                   as primary_artist_id,
        max(primary_artist_name)                                 as primary_artist_name,
        count(*)                                                 as play_count,
        max(played_at)                                           as last_played_at,
        extract(epoch from (now() - max(played_at))) / 86400.0  as days_since_last_play
    from play_history
    group by track_id
)

select
    track_id,
    track_name,
    primary_artist_id,
    primary_artist_name,
    play_count,
    last_played_at,
    days_since_last_play,
    /* recency_score is a simple linear decay — 1.0 if played today, 0.0 after 30 days.
    This gives Feast feature store a clean numeric feature to serve. */
    greatest(0.0, 1.0 - (days_since_last_play / 30.0)) as recency_score
from stats
