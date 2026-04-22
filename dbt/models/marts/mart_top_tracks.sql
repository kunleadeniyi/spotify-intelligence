with plays as (
    select * from {{ ref('int_deduped_play_events') }}
),

windows as (
    select
        track_id,
        track_name,
        primary_artist_id,
        primary_artist_name,
        count(*) filter (where played_at >= now() - interval '7 days')    as plays_7d,
        count(*) filter (where played_at >= now() - interval '30 days')   as plays_30d,
        count(*) filter (where played_at >= now() - interval '365 days')  as plays_365d,
        count(*)                                                           as plays_all_time
    from plays
    group by track_id, track_name, primary_artist_id, primary_artist_name
)

select * from windows
order by plays_all_time desc
