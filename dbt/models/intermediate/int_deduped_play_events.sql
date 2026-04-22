with ranked as (
    select
        *,
        row_number() over (
            partition by
                track_id,
                date_trunc('minute', played_at),
                (extract(second from played_at)::int / 30)
            order by played_at
        ) as rn
    from {{ ref('stg_play_events') }}
)
select
    event_id,
    track_id,
    played_at,
    ingested_at,
    track_name,
    album_name,
    album_release_date,
    duration_ms,
    popularity,
    is_explicit,
    primary_artist_id,
    primary_artist_name
from ranked
where rn = 1
