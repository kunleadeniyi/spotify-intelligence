select
    played_at::date                             as play_date,
    count(*)                                    as total_plays,
    count(distinct track_id)                    as unique_tracks,
    count(distinct primary_artist_id)           as unique_artists,
    round(sum(duration_ms) / 60000.0, 2)        as total_minutes
from {{ ref('int_deduped_play_events') }}
group by played_at::date
order by play_date desc
