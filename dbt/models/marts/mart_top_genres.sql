with plays as (
    select * from {{ ref('int_deduped_play_events') }}
),

genres as (
    select * from {{ ref('stg_artist_genres') }}
),

plays_with_genres as (
    select
        p.played_at,
        unnest(g.genres) as genre
    from plays p
    inner join genres g on p.primary_artist_id = g.artist_id
)

select
    genre,
    count(*) filter (where played_at >= now() - interval '7 days')        as plays_7d,
    count(*) filter (where played_at >= now() - interval '30 days')       as plays_30d,
    count(*) filter (where played_at >= now() - interval '365 days')      as plays_365d,
    count(*)                                                               as plays_all_time
from plays_with_genres
group by genre
order by plays_all_time desc
